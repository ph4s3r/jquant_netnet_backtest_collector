"""NETNET Backtest.

Given a list of selected dates,
- Fetch a list of all asset tickers traded on TSE
- Fetch detailed balance sheets for each ticker & date
- Fetch dividends data
- Calculate NCAV & TTM dividends
- Return a list of assets fulfilling a criteria (wip)
"""

# pypi
import aiofiles
import asyncio
import datetime
import better_exceptions

# built-in
import uuid
import time
from pathlib import Path
from asyncio import Lock, Semaphore
from collections import defaultdict

# local
import jquant_calc
import jquant_client
from perflogger import periodic_perf_logger
from structlogger import configure_logging, get_logger

# Limit concurrent API calls
SEMAPHORE_LIMIT = 5

# NVACPS LIMIT
NCAVPS_LIMIT = 0.8

# OHLC lookback limit
OHLC_LOOKBACK_LIMIT_DAYS = 14

# better_exceptions settings
better_exceptions.MAX_LENGTH = None
better_exceptions.encoding = 'utf-8'

# logger
# on glacius, log into the var/www folder, otherwise to local logfolder
LOCAL_LOGDIR = 'jquant_logs/'
GLACIUS_LOGDIR = r'/var/www/analytics/jquantv3/'
NETNET_HEADER = 'ticker,analysis_date,ncavps,share_price,mos_rate,fs_date,st_date,fs_st_skew_days\n'

GLACIUS_UUID = 94558092206834
ELEMENT_UUID = 91765249380

ON_ELEMENT = ELEMENT_UUID == uuid.getnode()
ON_GLACIUS = GLACIUS_UUID == uuid.getnode()

ULTIMATE_LOGDIR = GLACIUS_LOGDIR if ON_GLACIUS else LOCAL_LOGDIR

configure_logging(log_dir=ULTIMATE_LOGDIR)

log_main = get_logger('main')
log_main.info('-- Running NETNET Backtest --')

analysis_dates = [
    '2009-02-21',
    '2010-01-01',
    '2011-01-01',
    '2012-01-01',
    '2013-01-01',
    '2014-01-01',
    '2015-01-01',
    '2016-01-01',
    '2017-01-01',
    '2018-01-01',
    '2019-01-01',
    '2020-01-01',
    '2021-01-01',
    '2022-01-01',
    '2023-01-01',
    '2024-01-01',
    '2025-09-20',
]


async def process_ticker(  # noqa: ANN201, PLR0913
    ticker: str,
    analysis_date: str,
    data_calculated: defaultdict,
    fs_details: defaultdict,
    statements: defaultdict,
    ohlc_lock: Lock,
    netnet_lock: Lock,
    semaphore: Semaphore,
    jquant: jquant_client.JQuantAPIClient,
):
    """Process a single ticker for an analysis date."""
    async with semaphore:
        log_main.debug(f'Processing ticker: {ticker} for {analysis_date}')

        # NCAV data from https://jpx.gitbook.io/j-quants-en/api-reference/statements-1
        fs_details_params = {'code': ticker}
        if not fs_details[ticker]:
            fs_details[ticker] = await jquant.query_endpoint(endpoint='fs_details', params=fs_details_params)
        if fs_details[ticker]:
            ncav_data = jquant_calc.jquant_calculate_ncav(
                fs_details=fs_details[ticker],
                analysisdate=analysis_date,
            )
            if not ncav_data:
                return
            data_calculated[ticker][analysis_date].update(ncav_data)
        else:
            log_main.debug(f'No fs_details for {ticker}')
            return  # no fs_details, skip to next ticker

        # for NCAVPS: getting outstanding shares from https://jpx.gitbook.io/j-quants-en/api-reference/statements
        st_params = {'code': ticker}
        if not statements[ticker]['statements']:
            statements[ticker]['statements'] = await jquant.query_endpoint(endpoint='statements', params=st_params)
        if statements[ticker]['statements']:
            outstanding_shares_data = jquant_calc.jquant_extract_os(
                statements=statements[ticker]['statements'],
                analysisdate=analysis_date,
            )
            data_calculated[ticker][analysis_date].update(outstanding_shares_data)
        else:
            log_main.debug(f'No statements for {ticker}')
            return

        # Calculate NCAVPS
        try:
            data_calculated[ticker][analysis_date]['ncavps'] = data_calculated[ticker][analysis_date].get(
                'fs_ncav_total', 0.0
            ) / data_calculated[ticker][analysis_date].get(
                'st_NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock'
            )
            if not data_calculated[ticker][analysis_date].get('fs_ncav_total', 0.0):
                raise ZeroDivisionError  # noqa: TRY301
        except ZeroDivisionError:
            log_main.warning(f'no number of shares data for {ticker}')
            return  # skip to next ticker if ncav or outstanding shares is zero
        except TypeError:
            log_main.warning(f'No # of shares data found for {ticker}')
            return

        # Also take note of the skew between disclosure dates
        st_disclosure_date = data_calculated[ticker][analysis_date].get('st_disclosure_date')
        ncavdatadate = data_calculated[ticker][analysis_date].get('fs_disclosure_date')
        if st_disclosure_date and ncavdatadate:
            data_calculated[ticker][analysis_date]['fs_st_skew_days'] = (
                datetime.datetime.fromisoformat(st_disclosure_date).date() - \
                datetime.datetime.fromisoformat(ncavdatadate).date()
            ).days

        # get the share price for the day of the ncav data
        ohlc_data_for_ncav_date = await jquant.query_ohlc(params={'code': ticker, 'date': ncavdatadate})
        if not ohlc_data_for_ncav_date or not ohlc_data_for_ncav_date[0].get('Close', 0.0):
            fallback_date = ncavdatadate
            ohlc_attempt_limit = OHLC_LOOKBACK_LIMIT_DAYS
            while ohlc_attempt_limit > 0:
                ohlc_attempt_limit -= 1
                fallback_date = datetime.datetime.fromisoformat(str(fallback_date)).date() - datetime.timedelta(days=1)
                ohlc_data_for_ncav_date = await jquant.query_ohlc(params={'code': ticker, 'date': str(fallback_date)})
                if not ohlc_data_for_ncav_date or not ohlc_data_for_ncav_date[0].get('Close', 0.0):
                    continue
                data_calculated[ticker][analysis_date]['share_price_at_ncav_date'] = ohlc_data_for_ncav_date[0].get(
                    'Close', 0.0
                )
                break
            if ohlc_attempt_limit == 0:
                async with ohlc_lock:
                    async with aiofiles.open(f'{ULTIMATE_LOGDIR}/no_ohlc_found_{analysis_date}.txt', 'a', encoding='utf-8') as f:
                        await f.write(f'{ticker}\n')
                log_main.warning(f'No OHLC data found for {ticker}, even going {OHLC_LOOKBACK_LIMIT_DAYS} days back...')
                return

        # the asset is netnet if the share price is less than NVACPS_LIMIT * 100 % of the ncavps
        shareprice = data_calculated[ticker][analysis_date].get('share_price_at_ncav_date', 999999)
        MoS_rate = shareprice / data_calculated[ticker][analysis_date]['ncavps']
        data_calculated[ticker][analysis_date]['netnet'] = shareprice < (
            data_calculated[ticker][analysis_date]['ncavps'] * NCAVPS_LIMIT
        )
        if data_calculated[ticker][analysis_date]['netnet']:
            log_main.info('netnet stock found!')
            netnet_fname = f'{ULTIMATE_LOGDIR}/tse_netnets_{analysis_date}.csv'
            async with netnet_lock:
                netnet_file_exists = Path(netnet_fname).exists()
                async with aiofiles.open(netnet_fname, 'a', encoding='utf-8') as f:
                    if not netnet_file_exists:
                        await f.write(NETNET_HEADER)
                    await f.write(
                        f'{ticker},{analysis_date},{data_calculated[ticker][analysis_date]["ncavps"]:.2f},{shareprice},{MoS_rate:.2f},{ncavdatadate},{st_disclosure_date},{data_calculated[ticker][analysis_date]['fs_st_skew_days']}\n'
                    )
            log_main.debug(f'Wrote netnet data for {ticker}')


async def main() -> None:
    """Execute."""
    jquant = jquant_client.JQuantAPIClient()
    tickers: dict = jquant.get_tickers_for_dates(analysis_dates=analysis_dates)

    data_calculated = defaultdict(lambda: defaultdict(dict))
    fs_details = defaultdict(lambda: defaultdict(dict))
    statements = defaultdict(lambda: defaultdict(dict))
    ohlc_lock = Lock()
    netnet_lock = Lock()
    semaphore = Semaphore(SEMAPHORE_LIMIT)

    timestamp = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')
    perf_log_file = f'{ULTIMATE_LOGDIR}/performance_{timestamp}.csv'
    async with aiofiles.open(perf_log_file, 'w', encoding='utf-8') as f:
        await f.write('analysis_date,semaphore_limit,tickers_processed,duration_seconds,tickers_per_minute\n')

    for analysis_date in tickers:
        log_main.info(f'*** Running for analysis date: {analysis_date} ***')
        start_time = time.time()
        tickers_processed_counter = {'count': 0, 'start': start_time}
        stop_event = asyncio.Event()

        async def counted_process_ticker(
            ticker: str, *, analysis_date=analysis_date, tickers_processed_counter=tickers_processed_counter
        ) -> None:
            await process_ticker(
                ticker, analysis_date, data_calculated, fs_details,
                statements, ohlc_lock, netnet_lock, semaphore, jquant
            )
            tickers_processed_counter['count'] += 1

        tasks = [counted_process_ticker(t) for t in tickers[analysis_date]]
        periodic_logger_task = asyncio.create_task(
            periodic_perf_logger(
                60, perf_log_file, analysis_date, SEMAPHORE_LIMIT, tickers_processed_counter, stop_event
                )
        )

        await asyncio.gather(*tasks)
        stop_event.set()
        await periodic_logger_task  # let it exit gracefully

        duration = time.time() - start_time
        tpm = len(tasks) / (duration / 60) if duration > 0 else 0
        async with aiofiles.open(perf_log_file, 'a', encoding='utf-8') as f:
            await f.write(
                f'{analysis_date},{SEMAPHORE_LIMIT},{tickers_processed_counter["count"]},{duration:.2f},{tpm:.2f}\n'
            )
        log_main.info(f'*** Finished run for analysis date: {analysis_date} ***')

    log_main.info('-- Finished NETNET Backtest --')


if __name__ == '__main__':
    asyncio.run(main())
