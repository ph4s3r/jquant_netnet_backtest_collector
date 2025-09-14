# ruff: noqa

"""NETNET Backtest Test with sample files

"""

import json
from pprint import pprint
from pathlib import Path

from jquant_calculate_ncav import jquant_calculate_ncav

testfile = 'sample_data\jquant_fs_details.json'
fs_details_json = json.loads(Path(testfile).read_text(encoding='utf-8'))
st = fs_details_json['fs_details']
data = jquant_calculate_ncav(fs_details=st)
print()
pprint(data)
pass