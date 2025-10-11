"""Load data from pickle / dill and process."""

# built-in
from pathlib import Path
from collections import defaultdict
from datetime import date, timedelta

# local
from loader_with_filter import pickle_load
import jquant_get_st_fs


def nested_defaultdict_factory() -> defaultdict[dict]:
    """Pickle cannot load objects with lambda function, so we need this factory fun."""
    return defaultdict(dict)


st_fs_dv_data: defaultdict = pickle_load(data_file_path='data/fs_st_div.pkl')
ohlc_data: defaultdict = pickle_load(data_file_path='data/ohlc.pkl')

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
    '2025-09-20',
]

analysis_dates.reverse()

def get_wednesdays_in_range(date_frame) -> list:
    """Generate a list of date strings for every Wednesday between two dates.

    The function expects a list containing two date strings in "YYYY-MM-DD" format.
    It will automatically determine the earlier and later dates.

    Args:
        date_frame (list): A list of two date strings, e.g., ["2025-10-09", "2006-12-21"].

    Returns:
        list: A sorted list of date strings in "YYYY-MM-DD" format,
              representing every Wednesday within the specified range (inclusive).

    """
    try:
        # Convert string dates to date objects
        date_one = date.fromisoformat(date_frame[0])
        date_two = date.fromisoformat(date_frame[1])
    except (ValueError, TypeError) as e:
        print(f'Error parsing dates: {e}')
        return []

    # Establish the start and end of the date range
    start_date = min(date_one, date_two)
    end_date = max(date_one, date_two)

    wednesdays = []
    current_date = start_date

    # Find the first Wednesday on or after the start date
    # The weekday() method returns 0 for Monday and 6 for Sunday. Wednesday is 2.
    days_until_wednesday = (2 - current_date.weekday() + 7) % 7
    current_date += timedelta(days=days_until_wednesday)

    # Loop through the weeks and append each Wednesday
    while current_date <= end_date:
        wednesdays.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(weeks=1)

    return wednesdays


# Define the date frame as provided
DATEFRAME = ['2025-10-09', '2010-12-21']

ohlc_wednesdays = get_wednesdays_in_range(DATEFRAME)

TICKERS_FILE = 'inputs/tickers2025.txt'

tickers = Path(TICKERS_FILE).read_text(encoding='utf-8').replace('.T', '0').split('\n')

for ticker in tickers:
    if ticker:
        for analysis_date in analysis_dates:
            fsa = jquant_get_st_fs.jquant_get_latest_fs(fs_details=st_fs_dv_data[int(ticker)]['fs'], analysisdate=analysis_date)
            sta = jquant_get_st_fs.jquant_get_latest_st(statements=st_fs_dv_data[int(ticker)]['st'], analysisdate=analysis_date)
            # filtering dates for the analysis date, and before some time (2 weeks max) - depends on the analytics task
            ohlc = ohlc_data[int(ticker)]['ohlc']

            # calculate ncavps
            # calculate fcf
            # calculate icr
            # calculate roic

pass
