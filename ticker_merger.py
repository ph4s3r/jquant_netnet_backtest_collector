"""load all tickers from all ticker files then return a set."""
from pathlib import Path

tickers = []

DIR = 'jquant_tickers/'
OUT_FILE = 'all_tickers/all_tickers.txt'

tickerfiles = list(Path(DIR).glob('*.txt'))

for t in tickerfiles:
    t_text = Path(t).read_text(encoding='utf-8')
    tickers.extend(t_text.split('\n'))

tickers = set(tickers)

Path(OUT_FILE).write_text('\n'.join(tickers))

