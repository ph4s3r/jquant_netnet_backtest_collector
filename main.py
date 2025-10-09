"""JQUANT NETNET BACKTEST DATA COLLECTOR.

Given a list of selected dates,
- Fetch a list of all asset tickers traded on TSE
- Fetch detailed balance sheets for each ticker & date
- Calculate NCAVPS for each
- Return a list of tickers filtered by NCAVPS_LIMIT
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

# limits to how much earlier data we are willing to use if there is no recent given
OHLC_LOOKBACK_LIMIT_DAYS = 14  # latest share price vs ncav calculation date (fs_details disclosure)
FS_LOOKBACK_LIMIT_DAYS = 365  # fs_details vs analysis date
ST_LOOKBACK_LIMIT_DAYS = 365  # statements vs analysis date

# better_exceptions settings
better_exceptions.MAX_LENGTH = None
better_exceptions.encoding = 'utf-8'

# logger
# on glacius, log into the var/www folder, otherwise to local logfolder
LOCAL_LOGDIR = 'jquant_logs/'
GLACIUS_LOGDIR = r'/var/www/analytics/jquantv3/'
NETNET_HEADER = (
    'ticker,analysis_date,ncavps,share_price,mos_rate,market_cap,enterprise_value,ey_shares_out,ey_net_income,operating_profit,ey_gross_debt,ey_pe,ey_ev,roc_current_assets,roc_current_liabilities,roc_cash_and_equivalents,roc_property,roc_nwc_oper,roc_capital_base,roc,fs_gross_debt_fields,fs_date,st_date,fs_st_skew_days,st_report_type,fs_report_type\n'
)

GLACIUS_UUID = 94558092206834
ELEMENT_UUID = 91765249380

ON_ELEMENT = ELEMENT_UUID == uuid.getnode()
ON_GLACIUS = GLACIUS_UUID == uuid.getnode()

ULTIMATE_LOGDIR = GLACIUS_LOGDIR if ON_GLACIUS else LOCAL_LOGDIR

configure_logging(log_dir=ULTIMATE_LOGDIR)

log_main = get_logger('main')
log_main.info('-- Running NETNET Backtest --')

analysis_dates = [
    '2010-12-21',
    '2011-12-21',
    '2012-12-21',
    '2013-12-21',
    '2014-12-21',
    '2015-12-21',
    '2016-12-21',
    '2017-12-21',
    '2018-12-21',
    '2019-12-21',
    '2020-12-21',
    '2021-12-21',
    '2022-12-21',
    '2023-12-21',
    '2024-12-21',
    '2025-10-08',
]

global_fs_keys = set()

def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return float(default)


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
                max_lookbehind=FS_LOOKBACK_LIMIT_DAYS,
                # global_fs_keys=global_fs_keys
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
                max_lookbehind=ST_LOOKBACK_LIMIT_DAYS,
            )
            if outstanding_shares_data:
                data_calculated[ticker][analysis_date].update(outstanding_shares_data)
            else:
                log_main.debug(f'No quarterly statements for {ticker}')
                return
        else:
            log_main.debug(f'No statements for {ticker}')
            return

        # Calculate NCAVPS
        try:
            data_calculated[ticker][analysis_date]['ncavps'] = data_calculated[ticker][analysis_date].get('fs_ncav_total', 0.0) / data_calculated[ticker][analysis_date].get(
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
                datetime.datetime.fromisoformat(st_disclosure_date).date() - datetime.datetime.fromisoformat(ncavdatadate).date()
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
                data_calculated[ticker][analysis_date]['share_price_at_ncav_date'] = ohlc_data_for_ncav_date[0].get('Close', 0.0)
                break
            if ohlc_attempt_limit == 0:
                async with ohlc_lock:
                    async with aiofiles.open(f'{ULTIMATE_LOGDIR}/no_ohlc_found_{analysis_date}.txt', 'a', encoding='utf-8') as f:
                        await f.write(f'{ticker}\n')
                log_main.warning(f'No OHLC data found for {ticker}, even going {OHLC_LOOKBACK_LIMIT_DAYS} days back...')
                return
        else:
            data_calculated[ticker][analysis_date]['share_price_at_ncav_date'] = ohlc_data_for_ncav_date[0].get('Close', 0.0)

        # the asset is netnet if the share price is less than NVACPS_LIMIT * 100 % of the ncavps
        shareprice = data_calculated[ticker][analysis_date].get('share_price_at_ncav_date', 999999)
        MoS_rate = shareprice / data_calculated[ticker][analysis_date]['ncavps']
        data_calculated[ticker][analysis_date]['netnet'] = shareprice < (data_calculated[ticker][analysis_date]['ncavps'] * NCAVPS_LIMIT)

        if data_calculated[ticker][analysis_date]['netnet']:
            try:
                # earnings yield calculation
                share_price = data_calculated[ticker][analysis_date].get('share_price_at_ncav_date')
                shares_out = data_calculated[ticker][analysis_date].get('st_NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock')
                net_income = data_calculated[ticker][analysis_date].get('fs_profit_to_owners')
                operating_profit = _safe_float(data_calculated[ticker][analysis_date].get('fs_operating_profit'))
                gross_debt = data_calculated[ticker][analysis_date].get('fs_gross_debt')
                cash_eq = _safe_float(data_calculated[ticker][analysis_date].get('fs_cash_and_equivalents'), 0.0)

                # Compute market cap and EV defensively
                market_cap = None
                enterprise_value = None

                try:
                    if share_price and shares_out and float(shares_out) != 0:
                        market_cap = float(share_price) * float(shares_out)
                except (TypeError, ValueError):
                    market_cap = None

                try:
                    if market_cap:
                        enterprise_value = float(market_cap) - cash_eq + gross_debt
                except (TypeError, ValueError):
                    enterprise_value = None

                # Earnings yield (P/E-style): Net income / Market cap
                ey_pe = None
                try:
                    if market_cap and float(market_cap) != 0 and net_income is not None:
                        ey_pe = float(net_income) / float(market_cap)
                except (TypeError, ValueError, ZeroDivisionError):
                    ey_pe = None

                # Earnings yield (EV-based): EBIT / EV
                ey_ev = None
                try:
                    if enterprise_value and float(enterprise_value) != 0 and operating_profit is not None:
                        ey_ev = float(operating_profit) / float(enterprise_value)
                except (TypeError, ValueError, ZeroDivisionError):
                    ey_ev = None

                # Persist results
                data_calculated[ticker][analysis_date]['ey_pe'] = ey_pe
                data_calculated[ticker][analysis_date]['ey_ev'] = ey_ev
                data_calculated[ticker][analysis_date]['market_cap'] = market_cap
                data_calculated[ticker][analysis_date]['enterprise_value'] = enterprise_value
                # Persist EY inputs
                data_calculated[ticker][analysis_date]['ey_shares_out'] = shares_out
                data_calculated[ticker][analysis_date]['ey_net_income'] = net_income
                data_calculated[ticker][analysis_date]['operating_profit'] = operating_profit
                data_calculated[ticker][analysis_date]['ey_gross_debt'] = gross_debt
            except Exception as e:
                log_main.warning(f'error in ey calculation for {ticker=} at {analysis_date=}')

            try:
                # roc Greenblatt-style proxy
                # uses fileds:
                # - fs_current_assets
                # - fs_current_liabilities
                # - fs_cash_and_equivalents
                # - fs_property
                # - fs_operating_profit

                current_assets = data_calculated[ticker][analysis_date].get('fs_current_assets')
                current_liabilities_val = data_calculated[ticker][analysis_date].get('fs_current_liabilities', None)
                ppe = _safe_float(data_calculated[ticker][analysis_date].get('fs_property'), 0.0)

                roc = None
                try:
                    # Only proceed if we actually have current liabilities and operating profit
                    if current_liabilities_val is not None and operating_profit is not None:
                        current_liabilities = _safe_float(current_liabilities_val, 0.0)

                        if operating_profit is not None:
                            # Operating net working capital excludes cash
                            nwc_oper = current_assets - current_liabilities - cash_eq
                            capital_base = nwc_oper + ppe

                            denom = float(capital_base)
                            roc = operating_profit / denom if denom > 0.0 else None
                except Exception:
                    roc = None

                # Persist ROC result and inputs
                data_calculated[ticker][analysis_date]['roc'] = roc
                data_calculated[ticker][analysis_date]['roc_current_assets'] = current_assets
                data_calculated[ticker][analysis_date]['roc_current_liabilities'] = current_liabilities if 'current_liabilities' in locals() else None
                data_calculated[ticker][analysis_date]['roc_cash_and_equivalents'] = cash_eq
                data_calculated[ticker][analysis_date]['roc_property'] = ppe
                data_calculated[ticker][analysis_date]['operating_profit'] = operating_profit
                data_calculated[ticker][analysis_date]['roc_nwc_oper'] = nwc_oper if 'nwc_oper' in locals() else None
                data_calculated[ticker][analysis_date]['roc_capital_base'] = capital_base if 'capital_base' in locals() else None
            except Exception as e:
                log_main.warning(f'error in roc calculation for {ticker=} at {analysis_date=}. \r\n{e}')

            # write the netnet csv data
            log_main.info('netnet stock found!')
            netnet_fname = f'{ULTIMATE_LOGDIR}/tse_netnets_{analysis_date}.csv'
            async with netnet_lock:
                netnet_file_exists = Path(netnet_fname).exists()
                async with aiofiles.open(netnet_fname, 'a', encoding='utf-8') as f:
                    if not netnet_file_exists:
                        await f.write(NETNET_HEADER)
                    await f.write(
                        f'{ticker},'
                        f'{analysis_date},'
                        f'{data_calculated[ticker][analysis_date]["ncavps"]:.2f},'
                        f'{shareprice},'
                        f'{MoS_rate:.2f},'
                        f'{data_calculated[ticker][analysis_date]["market_cap"]},'
                        f'{data_calculated[ticker][analysis_date]["enterprise_value"]},'
                        f'{data_calculated[ticker][analysis_date].get("ey_shares_out", "")},'
                        f'{data_calculated[ticker][analysis_date].get("ey_net_income", "")},'
                        f'{data_calculated[ticker][analysis_date].get("operating_profit", "")},'
                        f'{data_calculated[ticker][analysis_date].get("ey_gross_debt", "")},'
                        f'{data_calculated[ticker][analysis_date]["ey_pe"]},'
                        f'{data_calculated[ticker][analysis_date]["ey_ev"]},'
                        f'{data_calculated[ticker][analysis_date].get("roc_current_assets", "")},'
                        f'{data_calculated[ticker][analysis_date].get("roc_current_liabilities", "")},'
                        f'{data_calculated[ticker][analysis_date].get("roc_cash_and_equivalents", "")},'
                        f'{data_calculated[ticker][analysis_date].get("roc_property", "")},'
                        f'{data_calculated[ticker][analysis_date].get("roc_nwc_oper", "")},'
                        f'{data_calculated[ticker][analysis_date].get("roc_capital_base", "")},'
                        f'{data_calculated[ticker][analysis_date]["roc"]},'
                        f'{data_calculated[ticker][analysis_date].get("fs_gross_debt_fields", "")},'
                        f'{ncavdatadate},'
                        f'{st_disclosure_date},'
                        f'{data_calculated[ticker][analysis_date].get("fs_st_skew_days", "")},'
                        f'{data_calculated[ticker][analysis_date]["st_report_type"]},'
                        f'{data_calculated[ticker][analysis_date]["fs_report_type"]}\n'
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

        async def counted_process_ticker(ticker: str, *, analysis_date=analysis_date, tickers_processed_counter=tickers_processed_counter) -> None:
            await process_ticker(ticker, analysis_date, data_calculated, fs_details, statements, ohlc_lock, netnet_lock, semaphore, jquant)
            tickers_processed_counter['count'] += 1

        tasks = [counted_process_ticker(t) for t in tickers[analysis_date]]
        periodic_logger_task = asyncio.create_task(periodic_perf_logger(60, perf_log_file, analysis_date, SEMAPHORE_LIMIT, tickers_processed_counter, stop_event))

        await asyncio.gather(*tasks)
        stop_event.set()
        await periodic_logger_task  # let it exit gracefully

        duration = time.time() - start_time
        tpm = len(tasks) / (duration / 60) if duration > 0 else 0
        async with aiofiles.open(perf_log_file, 'a', encoding='utf-8') as f:
            await f.write(f'{analysis_date},{SEMAPHORE_LIMIT},{tickers_processed_counter["count"]},{duration:.2f},{tpm:.2f}\n')
        log_main.info(f'*** Finished run for analysis date: {analysis_date} ***')

    log_main.info(f'Global FS keys: {global_fs_keys}')
    log_main.info('-- Finished NETNET Backtest --')


if __name__ == '__main__':
    asyncio.run(main())
