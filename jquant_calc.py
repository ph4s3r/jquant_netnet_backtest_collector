"""Functions to calculate the below from JQuant metrics.

- NCAV
- TTM (Trailing Twelve Months) dividends
"""


# built-in
from typing import Any
from operator import methodcaller
from datetime import date, timedelta

#local
from structlogger import get_logger
log_calc = get_logger('calc')


def to_float(v: Any) -> float:
    """Convert arbitrary values to float, return 0 if cannot."""
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def jquant_calculate_ncav(fs_details: list[dict], analysisdate: str | None = None) -> dict:
    """Calculate NCAV (Net Current Asset Value) from J-Quants fs_details endpoint.

    inputs:
        - fs_details: output of /statements-1 endpoint
        - analysisdate: the date we would like to run the backtest for: e.g. '2025-09-04'

    Logic:
        1. Find the closest financial statement disclosed before or on `analysisdate`
        2. Use:
            - Current assets (IFRS)
            - Liabilities (IFRS)   (or sum of current + non-current liabilities if available)
        3. Return NCAV = Current Assets - Total Liabilities
    """
    if not fs_details:
        return None
    st = None
    if analysisdate:

        analysisdate = date.fromisoformat(analysisdate)

        # sort by disclosure date
        fs_details.sort(key=methodcaller('get', 'DisclosedDate', ''))

        if date.fromisoformat(fs_details[0]['DisclosedDate']) > analysisdate:
            log_calc.info(
                f"no earlier fs_details found than analysis \
date for ticker {fs_details[0].get('LocalCode')}, skipping this."
                )
            return {}

        # find latest statement before analysis date
        for i, record in enumerate(fs_details):
            if (date.fromisoformat(record['DisclosedDate']) - analysisdate).days > 0:
                st = fs_details[0] if i == 0 else fs_details[i - 1]
                del i, record, fs_details
                break
    else:
        st = fs_details[0]  # no analysisdate given, working with the first element


    no_liabilities_data = False

    # Extract required fields
    current_assets = to_float(
        st['FinancialStatement'].get('Current assets (IFRS)') or st['FinancialStatement'].get('Current assets')
    )
    total_liabilities = to_float(
        st['FinancialStatement'].get('Liabilities (IFRS)') or st['FinancialStatement'].get('Liabilities')
    )

    if not current_assets:
        return {}

    # Fallback: if total liabilities missing, sum CL + NCL if available
    if not total_liabilities:
        current_liabilities = to_float(
            st['FinancialStatement'].get('Current liabilities (IFRS)')
            or st['FinancialStatement'].get('Current liabilities')
        )
        noncurrent_liabilities = to_float(
            st['FinancialStatement'].get('Non-current liabilities (IFRS)')
            or st['FinancialStatement'].get('Non-current liabilities')
        )
        if not current_liabilities or not noncurrent_liabilities:
            no_liabilities_data = True
        else:
            total_liabilities = current_liabilities + noncurrent_liabilities

    if no_liabilities_data:
        return {}
    # NCAV
    ncav_total = current_assets - total_liabilities

    return {
        'fs_disclosure_date': st.get('DisclosedDate'),
        'fs_period_type': st['FinancialStatement'].get('Type of current period, DEI'),
        'fs_current_fiscal_year_end': st['FinancialStatement'].get('Current fiscal year end date, DEI'),
        'fs_current_assets': current_assets,
        'fs_total_liabilities': total_liabilities,
        'fs_ncav_total': ncav_total,
    }


def jquant_extract_os(statements: list[dict], analysisdate: str | None = None) -> dict:
    """Get outstanding shares.

    inputs:
        - statements: output of /statements endpoint
        - analysisdate: the date we would like to run the backtest for: e.g. '2025-09-04'

    Logic:
        1. Find the closest financial statement disclosed before or on `analysisdate`
        2. Get NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock
        3. Return it as-is or 0.0 if missing
    """
    if not statements:
        return None
    st = None
    if analysisdate:
        # sort by disclosure date
        statements.sort(key=methodcaller('get', 'DisclosedDate', ''))

        analysisdate = date.fromisoformat(analysisdate)

        # find latest statement before analysis date
        for i, record in enumerate(statements):
            if (date.fromisoformat(record['DisclosedDate']) - analysisdate).days > 0:
                st = statements[i - 1]
                del i, record, statements, analysisdate
                break
        else:
            # if no future record found, take the last one
            st = statements[-1]
    else:
        st = statements[0]  # no analysisdate given, working with the first element

    shares_outstanding = to_float(
        st.get('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock')
    )
    total_assets = to_float(st.get('TotalAssets'))
    equity = to_float(st.get('Equity'))
    return {
        'st_disclosure_date': st.get('DisclosedDate'),
        'st_report_type': st.get('TypeOfDocument'),
        'st_period_type': st.get('TypeOfCurrentPeriod'),
        'st_total_assets': total_assets,
        'st_equity': equity,
        'st_NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock': shares_outstanding,
    }


def jquant_extract_dividends(dividend_data: dict, analysisdate: str | None = None) -> dict:
    """Calculate TTM (Trailing Twelve Months) dividends from J-Quants dividend endpoint.

    inputs:
        - dividend_data: output of /dividends endpoint (dict with "dividend" key containing list of dicts)
        - analysisdate: the date we would like to run the backtest for: e.g. '2025-09-04'

    Logic:
        1. Take all dividends with RecordDate within 12 months BEFORE analysisdate.
        2. Sum DistributionAmount to get TTM dividend.
    """
    if not dividend_data:
        return {'ttm_dividend': 0.0}

    # Convert analysisdate
    adate = date.fromisoformat(analysisdate)
    one_year_ago = adate - timedelta(days=365)

    ttm_dividend = 0.0
    dividend_records = []

    for d in dividend_data:
        record_date_str = d.get('RecordDate')
        if not record_date_str:
            continue
        try:
            record_date = date.fromisoformat(record_date_str)
        except ValueError:
            continue

        # Filter: record_date must be within last 12 months before analysisdate
        if one_year_ago <= record_date <= adate:
            amount = to_float(d.get('DistributionAmount'))
            ttm_dividend += amount
            dividend_records.append(
                {
                    'record_date': record_date_str,
                    'distribution_amount': amount,
                    'ex_date': d.get('ExDate'),
                    'announcement_date': d.get('AnnouncementDate'),
                }
            )

    return {
        'div_ttm_dividend': ttm_dividend,
        'div_dividend_count': len(dividend_records),
        'div_dividend_records': dividend_records,
    }
