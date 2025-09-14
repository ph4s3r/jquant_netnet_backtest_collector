from datetime import date

# NCAV Calculation
def jquant_calculate_ncav(financial_data: list[dict], analysisdate: str | None) -> dict:
    """Calculate Net Current Asset Value from J-Quants data.

    inputs:
        - financial_data: output of get_statements (https://jpx.gitbook.io/j-quants-en/api-reference/statements)
        - analysisdate: the date we would like to run the backtest for: e.g. '2025-09-04'

    first we find the financial data disclosed timewise closest to our given analysis date
    from the financial data then we use:
        - TotalAssets
        - Equity
    """
    if not financial_data:
        return None

    # find the closest date
    target = date.fromisoformat(analysisdate)
    deltas = []
    min_delta_days = 2500
    for i, f in enumerate(financial_data):
        deltas.append(
            (
                target - date.fromisoformat(f['DisclosedDate']),
                date.fromisoformat(f['DisclosedDate']),
            ),
        )
        delta = abs((target - date.fromisoformat(f['DisclosedDate'])).days)
        if min_delta_days > delta:
            min_delta_days = delta
            min_delta_days_index = i

    closest_past = financial_data[min_delta_days_index]
    try:
        # Extract balance sheet components (all values are strings)
        total_assets = float(closest_past.get('TotalAssets', 0) or 0)
        equity = float(closest_past.get('Equity', 0) or 0)
        shares_outstanding = float(
            closest_past.get(
                'NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock',
                0,
            )
            or 0,
        )

        # Calculate total liabilities
        total_liabilities = total_assets - equity

        # NCAV calculation (ideally use current assets, but total assets as conservative estimate)
        book_value_total = total_assets - total_liabilities  # This equals equity
        book_value_per_share = (
            book_value_total / shares_outstanding if shares_outstanding > 0 else 0
        )

        return {
            'ticker': closest_past.get('LocalCode'),
            'disclosure_date': closest_past.get('DisclosedDate'),
            'total_assets': total_assets,
            'total_liabilities': total_liabilities,
            'equity': equity,
            'shares_outstanding': shares_outstanding,
            'book_value_total': book_value_total,
            'book_value_per_share': book_value_per_share,
            'report_type': closest_past.get('TypeOfDocument'),
            'period_type': closest_past.get('TypeOfCurrentPeriod'),
        }

    except (ValueError, KeyError, TypeError) as e:
        print(f'Error calculating NCAV: {e}')
        return None

# code written by AI, needs review & test
# Historical Data Collection
def get_historical_ncav_data(all_statements: list, years_back: int = 5) -> list:
    """Collect historical NCAV data for multiple years."""
    historical_data = []

    if not all_statements:
        return historical_data

    # Filter for annual reports only
    for statement in all_statements:
        if statement.get('TypeOfCurrentPeriod') == 'FY':  # Full year data
            ncav_data = jquant_calculate_ncav([statement])
            if ncav_data:
                historical_data.append(ncav_data)

    # Sort by disclosure date (most recent first)
    historical_data.sort(key=lambda x: x['disclosure_date'], reverse=True)

    return historical_data[:years_back]
