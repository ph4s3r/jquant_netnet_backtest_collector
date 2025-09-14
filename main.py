"""NETNET Backtest.

Given a list of selected dates,
- Fetch a list of all asset tickers traded on TSE
- Fetch financial statements for each ticker & date
- Calculate NCAV & other metrics (wip)
- Return a list of assets fulfilling the critera (wip)
"""

import jquant_get_tickers
import jquant_get_financial_data
import jquant_calc
from collections import defaultdict

print('-- Running NETNET Backtest --')

# The free subscription covers the following dates: 2023-06-21 ~ 2025-06-21.
# If you want more data, please check other plans:  https://jpx-jquants.com/

backtest_dates = [
    '2024-09-01',   # aim for exact quarterly data since fin. statements are only available for these specific dates
    # '2023-12-21',
    # '2024-02-21',
    # '2024-06-21',
    # '2024-10-21',
    # '2025-02-21',
    # '2025-06-21',
]

tickers = jquant_get_tickers.get_tickers_for_dates(backtest_dates)

# ### get ncav data for each ticker ###
# https://jpx.gitbook.io/j-quants-pro/api-reference/statements

jquant = jquant_get_financial_data.JQuantAPIClient()

data_full = defaultdict(lambda: defaultdict(dict))

for date in tickers:
    for ticker in tickers[date]:
        # This function is obsolete, because it uses the free endpoint, but the data here lacks the
        # Current assets (IFRS) data that is Graham's original NCAV calculation is using

        # if fs := jquant.query_endpoint(
        #     endpoint='statements', ticker=ticker, analysisdate=None,
        # ):
        #     data_full[ticker][date]['fs'] = fs
        #     # caclulate 1 ncav for the closest date to the analysis date
        #     data_full[ticker][date]['ncav_analysisdate'] = (
        #         jquant_calculate_ncav.jquant_find_latest_disclosed_statement_to_analysis_date(
        #             data_full[ticker][date]['fs'], date,
        #         )
        #     )

        # Proper NCAV calculation from the https://jpx.gitbook.io/j-quants-en/api-reference/statements-1 endpoint
        if fs_details := jquant.query_endpoint(
            endpoint='fs_details',
            ticker=ticker,
            analysisdate=date,
        ):
            data_full[ticker][date] = jquant_calc.jquant_calculate_ncav(
                fs_details=fs_details,
                analysisdate=date,
            )

        # get dividends (needs premium plan...) https://jpx.gitbook.io/j-quants-en/api-reference/dividend
        # for 2 years back
        isodate = date.fromisoformat(date)
        div_fromdate = isodate.replace(year=isodate.year - 2)
        if dividend_data := jquant.query_endpoint(
            endpoint='dividend',
            ticker=ticker,
            analysisdate=None,
            fromdate=div_fromdate,
            todate=date
        ):
            # write jquant_extract_dividends
            dividend_fields = jquant_calc.jquant_extract_dividends(
                dividend_data=dividend_data,
                analysisdate=date,
            )
            data_full[ticker][date].update(dividend_fields)

