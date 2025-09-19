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

# built-in
import uuid
from asyncio import Lock, Semaphore
from collections import defaultdict
import time

# local
import jquant_calc
import jquant_client
from perflogger import periodic_perf_logger
from structlogger import configure_logging, get_logger

# Limit concurrent API calls
SEMAPHORE_LIMIT = 5

# logger
# on glacius, log into the var/www folder, otherwise to local logfolder
LOCAL_LOGDIR = 'jquant_logs/'
GLACIUS_LOGDIR = r'/var/www/analytics/jquant/'

GLACIUS_UUID = 94558092206834
ELEMENT_UUID = 91765249380

ON_ELEMENT = ELEMENT_UUID == uuid.getnode()
ON_GLACIUS = GLACIUS_UUID == uuid.getnode()

ULTIMATE_LOGDIR = GLACIUS_LOGDIR if ON_GLACIUS else LOCAL_LOGDIR

configure_logging(log_dir=ULTIMATE_LOGDIR)

log_main = get_logger('main')
log_main.info('-- Running NETNET Backtest --')

# The free subscription covers the following dates: 2023-06-21 ~ 2025-06-21.
# If you want more data, please check other plans:  https://jpx-jquants.com/

analysis_dates = [
    '2007-12-21',
    '2006-12-21',
    '2005-12-21',
    '2004-12-21',
    '2003-12-21',
    '2002-12-21',
    '2001-12-21',
    '2000-12-21',
    '1999-12-21',
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
        st_params = {'code': ticker}

        if not fs_details[ticker]:
            fs_details[ticker] = await jquant.query_endpoint(endpoint='fs_details', params=st_params)
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
            log_main.debug(f'ZeroDivisionError for {ticker}: no ncav or shares')
            return  # skip to next ticker if ncav or outstanding shares is zero

        # Also take note of the skew between disclosure dates
        # fiscalyearenddate = data_full[ticker][analysis_date].get('st_disclosure_date')
        ncavdatadate = data_calculated[ticker][analysis_date].get('fs_disclosure_date')
        # if fiscalyearenddate and ncavdatadate:
        #     data_full[ticker][analysis_date]['fs_st_skew_days'] = (
        #         datetime.datetime.fromisoformat(fiscalyearenddate).date() - \
        #         datetime.datetime.fromisoformat(ncavdatadate).date()
        #     ).days
        # else:
        #     data_full[ticker][analysis_date]['fs_st_skew_days'] = -999999

        # get the share price for the day of the ncav data
        ohlc_params = {'code': ticker, 'date': ncavdatadate}
        if ohlc_data_for_ncav_date := await jquant.query_ohlc(params=ohlc_params):
            data_calculated[ticker][analysis_date]['share_price_at_ncav_date'] = ohlc_data_for_ncav_date[0].get('Close', 0.0)
            if not data_calculated[ticker][analysis_date]['share_price_at_ncav_date']:
                # TODO: get price from somewhere else because it can be None
                async with (
                    ohlc_lock,
                    aiofiles.open(f'{ULTIMATE_LOGDIR}/no_ohlc_found_{analysis_date}.txt', 'a', encoding='utf-8') as f,
                ):
                    await f.write(f'{ticker}\n')
                log_main.debug(f'No OHLC data for {ticker}')
                return

        # the asset is netnet if the share price is less than 67% of the ncavps
        data_calculated[ticker][analysis_date]['netnet'] = data_calculated[ticker][analysis_date].get(
            'share_price_at_ncav_date', 999999
        ) < (data_calculated[ticker][analysis_date]['ncavps'] * 0.67)
        if data_calculated[ticker][analysis_date]['netnet']:
            log_main.info('netnet stock found!')
            # write to file: ticker, date, ncavps
            async with (
                netnet_lock,
                aiofiles.open(f'{ULTIMATE_LOGDIR}/tse_netnets_{analysis_date}.txt', 'a', encoding='utf-8') as f,
            ):
                await f.write(f'{ticker},{analysis_date},{data_calculated[ticker][analysis_date]["ncavps"]}\n')
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
