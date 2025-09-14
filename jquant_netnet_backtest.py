"""NETNET Backtest.

Given a list of selected dates,
- Fetch a list of all asset tickers traded on TSE
- Fetch financial statements for each ticker & date
- Calculate NCAV & other metrics (wip)
- Return a list of assets fulfilling the critera (wip)
"""

import jquant_get_tickers
import jquant_get_financial_data
import jquant_calculate_ncav
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
        # get financial statements: https://jpx.gitbook.io/j-quants-en/api-reference/statements
        if fs := jquant.query_endpoint(
            endpoint='statements', ticker=ticker, analysisdate=None,
        ):
            data_full[ticker][date]['fs'] = fs
            # caclulate ncav
            data_full[ticker][date]['ncav_metrics'] = (
                jquant_calculate_ncav.jquant_calculate_ncav(
                    data_full[ticker][date]['fs'], date,
                )
            )
        # get balance sheets (needs premium plan...) https://jpx.gitbook.io/j-quants-en/api-reference/statements-1
        data_full[ticker][date]['bs'] = jquant.query_endpoint(
            endpoint='fs_details', ticker=ticker, analysisdate=None,
        )

        # get dividends (needs premium plan...) https://jpx.gitbook.io/j-quants-en/api-reference/dividend
        data_full[ticker][date]['dividends'] = jquant.query_endpoint(
            endpoint='dividend', ticker=ticker, analysisdate=None,
        )
