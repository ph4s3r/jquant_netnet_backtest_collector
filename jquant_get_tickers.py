"""Get all actively traded asset tickers from TSE for given dates using Jquants API.

https://jpx-jquants.com/

uses the API endpoint: @title Listed Issue Information (/listed/info)

inputs:
    - EMAIL & PASS OR IDTOKEN in .env file (see .env template)
    - fill out the dates list for desired dates

output:
    - one txt file with 1 ticker per line per date e.g. /jquant_tickers/jquant_tickers_2023_06_21.txt
"""

import sys
import json
import requests
import pandas as pd
from pathlib import Path
from http import HTTPStatus
from dotenv import dotenv_values, set_key

config = dotenv_values('.env')

API_URL             = config.get('API_URL')
JQUANT_DATA_FOLDER  = config.get('JQUANT_DATA_FOLDER')

def get_idtoken(refresh: bool = False) -> str:
    """Read jquant id token from .env / get from API if not found."""
    if not refresh:
        if IDTOKEN := config.get('IDTOKEN'):
            print('idToken read successfully')
            return {'Authorization': f'Bearer {IDTOKEN}'}

    USER_DATA = {'mailaddress': config.get('EMAIL'), 'password': config.get('PASS')}

    # refresh token取得
    try:
        res = requests.post(
            f'{API_URL}/v1/token/auth_user', data=json.dumps(USER_DATA), timeout=30,
            )
        res.raise_for_status()
        refresh_token = res.json().get('refreshToken')
        if refresh_token is None:
            raise ValueError('Missing refreshToken in response.')
    except (requests.RequestException, ValueError) as e:
        print(f'Failed to get refresh token: {e}')
        sys.exit(1)

    # id token取得
    try:
        res = requests.post(
            f'{API_URL}/v1/token/auth_refresh?refreshtoken={refresh_token}', timeout=30,
        )
        res.raise_for_status()
        id_token = res.json().get('idToken')
        if id_token is None:
            raise ValueError('Missing idToken in response.')
    except (requests.RequestException, ValueError) as e:
        print(f'Failed to get idToken: {e}')
        sys.exit(1)
    print('idToken acquired successfully')
    set_key('.env', 'IDTOKEN', id_token)
    return id_token


def get_tickers_for_dates(dates: list[str]) -> dict:
    """Loop through the dates and get a ticker list for each.

    If a file already exists does not get it, otherwise get &
    write into a file. Returns a dict of list keyed with dates
    """
    # mkdir
    Path(JQUANT_DATA_FOLDER).mkdir(exist_ok=True, parents=True)

    id_token = get_idtoken()
    headers = {'Authorization': 'Bearer {}'.format(id_token)}

    all_tickers = {}

    i = 0
    token_error_flag = False

    while i < len(dates):
        date = dates[i]

        params = {'date': date}
        tickers_file = (
            f"{JQUANT_DATA_FOLDER}/jquant_tickers_{date.replace('-', '_')}.txt"
        )

        if Path(tickers_file).exists():
            print(f'{tickers_file} exists, skipping to next date..')
            all_tickers[date] = Path(tickers_file).read_text().splitlines()
            i+=1
            continue

        res = requests.get(f'{API_URL}/v1/listed/info', params=params, headers=headers, timeout=30)

        if res.status_code == HTTPStatus.OK:
            d = res.json()
            data = d['info']
            while 'pagination_key' in d:
                params['pagination_key'] = d['pagination_key']
                res = requests.get(f'{API_URL}/v1/listed/info', params=params, headers=headers, timeout=30)
                d = res.json()
                data += d['info']
            df = pd.DataFrame(data)
            if i == 0:
                print(f'sample data (for {date}): ')
                print(df)
            tickers_df = df[['Code']]
            tickers_df = tickers_df.drop_duplicates()
            all_tickers[date] = tickers_df['Code'].tolist()
            tickers_df.to_csv(tickers_file, index=False, header=False)
            print(f'tickers written to {tickers_file}')
            i += 1
        elif res.status_code == 401 and 'token is invalid or expired' in res.content.decode('utf-8'):
            print('idToken expired or invalid, refreshing.. (usually expires after 24 hours..)')
            if token_error_flag:
                print('cannot refresh token, check subscription, exiting..')
                sys.exit(1)
            id_token = get_idtoken(refresh=True)
            headers = {'Authorization': 'Bearer {}'.format(id_token)}
            token_error_flag = True
            i -= 1

        else:
            print(res.json())
            sys.exit(1)

    return all_tickers
