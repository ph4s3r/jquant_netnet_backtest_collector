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

# built-in
import uuid
import datetime
from asyncio import Lock, Semaphore
from collections import defaultdict

# local
import jquant_calc
import jquant_client
from structlogger import configure_logging, get_logger

# logger
# on glacius, log into the var/www folder, otherwise to local logfolder
LOCAL_LOGDIR = 'jquant_logs/'
GLACIUS_LOGDIR = r'/var/www/analytics/jquant/'

GLACIUS_UUID = 94558092206834
ELEMENT_UUID = 91765249380

ON_ELEMENT = ELEMENT_UUID == uuid.getnode()
ON_GLACIUS = GLACIUS_UUID == uuid.getnode()

if ON_GLACIUS:
    configure_logging(log_dir=GLACIUS_LOGDIR)
else:
    configure_logging(log_dir=LOCAL_LOGDIR)

log_main = get_logger('main')
log_main.info('-- Running NETNET Backtest --')

# The free subscription covers the following dates: 2023-06-21 ~ 2025-06-21.
# If you want more data, please check other plans:  https://jpx-jquants.com/

analysis_dates = [
    '2024-12-21',
    '2023-12-21',
    '2022-12-21',
    '2021-12-21',
    '2020-12-21',
    '2019-12-21',
    '2018-12-21',
    '2016-12-21',
    '2014-12-21',
    '2008-12-21',
]


async def process_ticker(
    ticker: str, analysis_date: str, data_full: defaultdict, ohlc_lock: Lock, netnet_lock: Lock, semaphore: Semaphore
):
    """Process a single ticker for an analysis date."""
    async with semaphore:
        log_main.debug(f'Processing ticker: {ticker} for {analysis_date}')
        # NCAV data from https://jpx.gitbook.io/j-quants-en/api-reference/statements-1
        st_params = {'code': ticker}
        jquant = jquant_client.JQuantAPIClient()

        if fs_details := await jquant.query_endpoint(endpoint='fs_details', params=st_params):
            ncav_data = jquant_calc.jquant_calculate_ncav(
                fs_details=fs_details,
                analysisdate=analysis_date,
            )
            data_full[ticker][analysis_date].update(ncav_data)
        else:
            log_main.debug(f'No fs_details for {ticker}')
            return  # no fs_details, skip to next ticker

        # for NCAVPS: getting outstanding shares from https://jpx.gitbook.io/j-quants-en/api-reference/statements
        if statements := await jquant.query_endpoint(endpoint='statements', params=st_params):
            outstanding_shares_data = jquant_calc.jquant_extract_os(
                statements=statements,
                analysisdate=analysis_date,
            )
            data_full[ticker][analysis_date].update(outstanding_shares_data)
        else:
            log_main.debug(f'No statements for {ticker}')
            return

        # Calculate NCAVPS
        try:
            data_full[ticker][analysis_date]['ncavps'] = data_full[ticker][analysis_date].get(
                'fs_ncav_total', 0.0
            ) / data_full[ticker][analysis_date].get(
                'st_NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock'
            )
            if not data_full[ticker][analysis_date].get('fs_ncav_total', 0.0):
                raise ZeroDivisionError
        except ZeroDivisionError:
            log_main.debug(f'ZeroDivisionError for {ticker}: no ncav or shares')
            return  # skip to next ticker if ncav or outstanding shares is zero

        # Also take note of the skew between disclosure dates
        fiscalyearenddate = data_full[ticker][analysis_date].get('st_disclosure_date')
        ncavdatadate = data_full[ticker][analysis_date].get('fs_disclosure_date')
        if fiscalyearenddate and ncavdatadate:
            data_full[ticker][analysis_date]['fs_st_skew_days'] = (
                datetime.date.fromisoformat(fiscalyearenddate) - datetime.date.fromisoformat(ncavdatadate)
            ).days
        else:
            data_full[ticker][analysis_date]['fs_st_skew_days'] = -999999

        # get the share price for the day of the ncav data
        ohlc_params = {'code': ticker, 'date': ncavdatadate}
        if ohlc_data_for_ncav_date := await jquant.query_ohlc(params=ohlc_params):
            data_full[ticker][analysis_date]['share_price_at_ncav_date'] = ohlc_data_for_ncav_date[0].get('Close', 0.0)
            if not data_full[ticker][analysis_date]['share_price_at_ncav_date']:
                # TODO: get price from somewhere else because it can be None
                async with (
                    ohlc_lock,
                    aiofiles.open(f'jquant_logs/no_ohlc_found_{analysis_date}.txt', 'a', encoding='utf-8') as f,
                ):
                    await f.write(f'{ticker}\n')
                log_main.debug(f'No OHLC data for {ticker}')
                return

        # the asset is netnet if the share price is less than 67% of the ncavps
        data_full[ticker][analysis_date]['netnet'] = data_full[ticker][analysis_date].get(
            'share_price_at_ncav_date', 999999
        ) < (data_full[ticker][analysis_date]['ncavps'] * 0.67)
        if data_full[ticker][analysis_date]['netnet']:
            log_main.info('netnet stock found!')
            # write to file: ticker, date, ncavps
            async with (
                netnet_lock,
                aiofiles.open(f'jquant_netnet/tse_netnets_{analysis_date}.txt', 'a', encoding='utf-8') as f,
            ):
                await f.write(f'{ticker},{analysis_date},{data_full[ticker][analysis_date]["ncavps"]}\n')
            log_main.debug(f'Wrote netnet data for {ticker}')


async def main() -> None:
    """Execute."""
    jquant = jquant_client.JQuantAPIClient()
    tickers: dict = jquant.get_tickers_for_dates(analysis_dates=analysis_dates)

    # keys are created automatically
    data_full = defaultdict(lambda: defaultdict(dict))
    ohlc_lock = Lock()
    netnet_lock = Lock()
    semaphore = Semaphore(2)  # Limit to 10 concurrent API calls

    for analysis_date in tickers:
        log_main.info(f'*** Running for analysis date: {analysis_date} ***')
        tasks = [
            process_ticker(ticker, analysis_date, data_full, ohlc_lock, netnet_lock, semaphore)
            for ticker in tickers[analysis_date]
        ]
        await asyncio.gather(*tasks)
        log_main.info(f'*** Finished run for analysis date: {analysis_date} ***')

    log_main.info('-- Finished NETNET Backtest --')


if __name__ == '__main__':
    asyncio.run(main())
