"""NETNET Backtest.

Given a list of selected dates,
- Fetch a list of all asset tickers traded on TSE
- Fetch deatiled balance sheets for each ticker & date
- Fetch dividends data
- Calculate NCAV & TTM dividends
- Return a list of assets fulfilling a criteria (wip)
"""

# built-in
from collections import defaultdict

# local
import jquant_calc
import jquant_client
import datetime

print('-- Running NETNET Backtest --')

# The free subscription covers the following dates: 2023-06-21 ~ 2025-06-21.
# If you want more data, please check other plans:  https://jpx-jquants.com/

analysis_dates = [
    '2024-12-21',
    # '2023-12-21',
    # '2024-02-21',
    # '2024-06-21',
    # '2024-10-21',
    # '2025-02-21',
    # '2025-06-21',
]

jquant = jquant_client.JQuantAPIClient()

tickers: dict = jquant.get_tickers_for_dates(analysis_dates=analysis_dates)

# ### get ncav data for each ticker ###
# https://jpx.gitbook.io/j-quants-pro/api-reference/statements

# keys are created automatically
data_full = defaultdict(lambda: defaultdict(dict))

for analysis_date in tickers:
    for ticker in tickers[analysis_date]:
        # NCAV data from https://jpx.gitbook.io/j-quants-en/api-reference/statements-1
        st_params = {'ticker': ticker, 'analysisdate': analysis_date}
        if fs_details := jquant.query_endpoint(endpoint='fs_details', params=st_params):
            data_full[ticker][analysis_date] = jquant_calc.jquant_calculate_ncav(
                fs_details=fs_details,
                analysisdate=analysis_date,
            )

        # for NCAVPS: getting outstanding shares from https://jpx.gitbook.io/j-quants-en/api-reference/statements
        if statements := jquant.query_endpoint(endpoint='statements', params=st_params):
            outstanding_shares_data = jquant_calc.jquant_extract_os(
                statements=statements,
                analysisdate=analysis_date,
            )
            data_full[ticker][analysis_date].update(outstanding_shares_data)

        # Calculate NCAVPS
        try:
            data_full[ticker][analysis_date]['ncavps'] = data_full[ticker][analysis_date].get(
                'fs_ncav_total', 0.0
            ) / data_full[ticker][analysis_date].get(
                'st_NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock'
            )  # noqa: E501
            if not data_full[ticker][analysis_date].get('fs_ncav_total', 0.0):
                raise ZeroDivisionError
        except ZeroDivisionError:
            continue  # skip to the next ticker for efficiency if ncav or outstanding shares is zero

        # Also take note of the skew between disclosure dates of the detailed balance sheet where NCAV is coming from
        # and the number of shares report / fiscal year end
        fiscalyearenddate = data_full[ticker][analysis_date]['st_CurrentFiscalYearEndDate']
        ncavdatadate = data_full[ticker][analysis_date]['fs_disclosure_date']
        data_full[ticker][analysis_date]['fs_st_skew_days'] = (
            datetime.date.fromisoformat(fiscalyearenddate) - datetime.date.fromisoformat(ncavdatadate)
        ).days

        # get the share price for the day of the ncav data
        ohlc_params = {'ticker': ticker, 'analysisdate': ncavdatadate}
        if ohlc_data_for_ncav_date := jquant.query_ohlc(params=ohlc_params):
            data_full[ticker][analysis_date]['share_price_at_ncav_date'] = ohlc_data_for_ncav_date.get('Close', 0.0)

        # the asset is netnet if the share price is less than 67% of the ncavps
        data_full[ticker][analysis_date]['netnet'] = data_full[ticker][analysis_date]['share_price_at_ncav_date'] > (
            data_full[ticker][analysis_date]['ncavps'] * 0.67
        )  # noqa: E501

        # get dividends from https://jpx.gitbook.io/j-quants-en/api-reference/dividend
        # for 2 years back from the analysis date
        isodate = datetime.date.fromisoformat(analysis_date)
        div_fromdate = isodate.replace(year=isodate.year - 2)
        dividend_params = {'ticker': ticker, 'from': div_fromdate, 'to': analysis_date}
        if dividend_data := jquant.query_endpoint(endpoint='dividend', params=dividend_params):
            dividend_fields = jquant_calc.jquant_extract_dividends(
                dividend_data=dividend_data,
                analysisdate=analysis_date,
            )
            data_full[ticker][analysis_date].update(dividend_fields)
