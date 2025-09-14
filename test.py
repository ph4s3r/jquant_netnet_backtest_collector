# ruff: noqa

"""NETNET Backtest Test with sample files

"""

import json
from pprint import pprint
from pathlib import Path

from jquant_calc import jquant_calculate_ncav, jquant_extract_dividends

# testfile names
fs_details_file = r"sample_data\\jquant_fs_details.json"
dividend_file   = r"sample_data\\jquant_dividend.json"

# load data as json
fs_details_json = json.loads(Path(fs_details_file).read_text(encoding='utf-8'))
dividend_json = json.loads(Path(dividend_file).read_text(encoding='utf-8'))

st = fs_details_json['fs_details']

ncav_data = jquant_calculate_ncav(fs_details=st, analysisdate='2023-01-30')
ttm_div = jquant_extract_dividends(dividend_data=dividend_json, analysisdate='2014-03-10')

print()
pprint(ncav_data)
print()
print(ttm_div)
pass