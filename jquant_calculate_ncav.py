from datetime import date
from operator import methodcaller

# This function is obsolete, because it uses the free endpoint, but the data here lacks the
# Current assets (IFRS) data that is Graham's original NCAV calculation is using
def jquant_find_latest_disclosed_statement_to_analysis_date(
        financial_data: list[dict], analysisdate: str | None
        ) -> dict:
    """Return the most relevant data for NCAV calculation from the free statements endpoint.

    inputs:
        - financial_data: output of get_sts (https://jpx.gitbook.io/j-quants-en/api-reference/statements)
        - analysisdate: the date we would like to run the backtest for: e.g. '2025-09-04'

    first we find the financial data disclosed timewise closest to our given analysis date
    from the financial data then we use:
        - TotalAssets
        - Equity
    """
    if not financial_data:
        return None

    # lexicographic search works as long as DisclosedDate is valid..
    financial_data.sort(key=methodcaller('get', 'DisclosedDate', ''))

    # convert analysis date to iso format to be able to compare them
    analysisdate = date.fromisoformat(analysisdate)

    # go from the earliest and stop at the closest date to our analysis
    for i, _ in enumerate(financial_data):
        if (date.fromisoformat(_['DisclosedDate']) - analysisdate).days > 0:
            st = financial_data[i - 1]
            del i, _, financial_data, analysisdate
            break

    def to_float(value) -> float:
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0


    # Extract balance sheet components (all values are strings)
    total_assets = to_float(st.get('TotalAssets'))
    equity = to_float(st.get('Equity'))
    shares_outstanding = to_float(
        st.get('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock')
                                  )

    # Compute derived metrics
    total_liabilities = total_assets - equity
    book_value_total = equity  # This is the same as total_assets - liabilities
    book_value_per_share = (
        book_value_total / shares_outstanding if shares_outstanding > 0 else 0
    )

    return {
        'disclosure_date': st.get('DisclosedDate'),
        'report_type': st.get('TypeOfDocument'),
        'period_type': st.get('TypeOfCurrentPeriod'),
        'total_assets': total_assets,
        'total_liabilities': total_liabilities,
        'equity': equity,
        'shares_outstanding': shares_outstanding,
        'book_value_total': book_value_total,
        'book_value_per_share': book_value_per_share,
    }

# This one should be fine
def jquant_calculate_ncav(fs_details: list[dict], analysisdate: str | None) -> dict:
    """Calculate NCAV (Net Current Asset Value) from J-Quants fs_details endpoint.

    inputs:
        - fs_details: output of /statements-1 endpoint
        - analysisdate: the date we would like to run the backtest for: e.g. '2025-09-04'

    Logic:
        1. Find the closest financial statement disclosed before or on `analysisdate`
        2. Use:
            - Current assets (IFRS)
            - Liabilities (IFRS)   (or sum of current + non-current liabilities if available)
        3. Return NCAV = Current Assets − Total Liabilities
    """
    if not fs_details:
        return None

    # sort by disclosure date
    fs_details.sort(key=methodcaller('get', 'DisclosedDate', ''))

    analysisdate = date.fromisoformat(analysisdate)

    # find latest statement before analysis date
    for i, record in enumerate(fs_details):
        if (date.fromisoformat(record['DisclosedDate']) - analysisdate).days > 0:
            st = fs_details[i - 1]
            del i, record, fs_details, analysisdate
            break
    else:
        # if no future record found, take the last one
        st = fs_details[-1]

    def to_float(value) -> float:
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    fs = st.get('FinancialStatement', {})

    # Extract required fields
    current_assets = to_float(fs.get('Current assets (IFRS)'))
    total_liabilities = to_float(fs.get('Liabilities (IFRS)'))

    # Fallback: if total liabilities missing, sum CL + NCL if available
    if not total_liabilities:
        current_liabilities = to_float(fs.get('Current liabilities (IFRS)'))
        noncurrent_liabilities = to_float(fs.get('Non-current liabilities (IFRS)'))
        total_liabilities = current_liabilities + noncurrent_liabilities

    # Calculate NCAV
    ncav_total = current_assets - total_liabilities

    return {
        'disclosure_date': st.get('DisclosedDate'),
        'report_type': st.get('TypeOfDocument'),
        'period_type': fs.get('Type of current period, DEI'),
        'current_assets': current_assets,
        'total_liabilities': total_liabilities,
        'ncav_total': ncav_total,
    }

def jquant_extract_dividends(dividend_data: dict, analysisdate: str | None) -> dict:
    """TODO: implement this."""
    return {}
