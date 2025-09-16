"""Jquant API Client."""

# pypi
import httpx
import pandas as pd
from dotenv import dotenv_values, set_key
from tenacity import retry, stop_after_attempt, wait_random_exponential

# built-in
import sys
import json
import requests
from typing import NoReturn
from pathlib import Path
from http import HTTPStatus

# local
from structlogger import get_logger

log_cli = get_logger('cli')

IDTOKEN_ERR_MSG = 'Missing idToken in response.'
REFRESHTOKEN_ERR_MSG = 'Missing refreshToken in response.'

def _raise_env_error(msg: str) -> NoReturn:
    log_cli.exception(msg)
    sys.exit(1)

def _raise_value_error(msg: str) -> NoReturn:
    raise ValueError(msg)

class JQuantAPIClient:

    """Manage JQuant API Calls."""

    HEADERS = ''
    IDTOKEN = ''
    API_URL = ''
    EMAIL = ''
    PASS = ''
    JQUANT_DATA_FOLDER = ''

    def __init__(self) -> None:
        self.classinit()

    @classmethod
    def classinit(cls) -> None:
        """Initialize Headers & API_URL only on first instance creation."""
        config = dotenv_values('.env')
        if cls.API_URL == '':
            cls.API_URL = config.get('API_URL', '')
            if not cls.API_URL.startswith(('http://', 'https://')):
                cls.API_URL = 'https://' + cls.API_URL
        if cls.IDTOKEN == '':
            cls.IDTOKEN = config.get('IDTOKEN')
        if cls.HEADERS == '':
            cls.HEADERS = {'Authorization': f'Bearer {cls.IDTOKEN}'}
        if cls.EMAIL == '':
            cls.EMAIL = config.get('EMAIL')
        if cls.PASS == '':
            cls.PASS = config.get('PASS')
        if cls.JQUANT_DATA_FOLDER == '':
            cls.JQUANT_DATA_FOLDER = config.get('JQUANT_DATA_FOLDER')

        # --- test dummy endpoint ---
        test_headers = {'Authorization': f'Bearer {cls.IDTOKEN}'}
        try:
            res = requests.get(
                f'{cls.API_URL}/v1/listed/info',
                params={'code': 'DUMMY', 'date': '1900-01-01'},
                headers=test_headers,
                timeout=10,
            )
            if res.status_code == HTTPStatus.UNAUTHORIZED:
                log_cli.info('Token expired or does not exist, refreshing...')
                cls.IDTOKEN = cls.get_idtoken(refresh=True)
                cls.HEADERS = {'Authorization': f'Bearer {cls.IDTOKEN}'}
        except requests.RequestException:
            log_cli.exception('Failed to test endpoint')
            sys.exit(1)

    @classmethod
    def get_idtoken(cls, refresh: bool = True) -> str:
        """Read jquant id token from .env / get from API if not found."""
        if not refresh:
            return {'Authorization': f'Bearer {cls.IDTOKEN}'}

        USER_DATA = {'mailaddress': cls.EMAIL, 'password': cls.PASS}

        # refresh token取得
        try:
            res = requests.post(
                f'{cls.API_URL}/v1/token/auth_user',
                data=json.dumps(USER_DATA),
                timeout=30,
            )
            res.raise_for_status()
            refresh_token = res.json().get('refreshToken')
            if refresh_token is None:
                _raise_value_error(REFRESHTOKEN_ERR_MSG)
        except (requests.RequestException, ValueError):
            _raise_env_error('Failed to get refresh token')
            sys.exit(1)

        # id token取得
        try:
            res = requests.post(
                f'{cls.API_URL}/v1/token/auth_refresh',
                params={'refreshtoken': refresh_token},
                timeout=30,
            )
            res.raise_for_status()
            id_token = res.json().get('idToken')
            if id_token is None:
                _raise_value_error(IDTOKEN_ERR_MSG)
        except (requests.RequestException, ValueError):
            _raise_env_error('Failed to get idToken')
            sys.exit(1)
        log_cli.info('idToken acquired successfully')
        set_key('.env', 'IDTOKEN', id_token)
        cls.HEADERS = {'Authorization': f'Bearer {id_token}'}
        return id_token

    @retry(stop=(stop_after_attempt(6)), wait=wait_random_exponential(min=5, max=60))
    def get_tickers_for_dates(self, analysis_dates: list[str]) -> dict:
        """Get all actively traded asset tickers from TSE for given dates using Jquants API."""
        Path(self.JQUANT_DATA_FOLDER).mkdir(exist_ok=True, parents=True)

        headers = {'Authorization': f'Bearer {self.IDTOKEN}'}
        all_tickers = {}
        i = 0
        token_error_flag = False

        while i < len(analysis_dates):
            date = analysis_dates[i]
            params = {'date': date}
            tickers_file = f'{self.JQUANT_DATA_FOLDER}/jquant_tickers_{date.replace("-", "_")}.txt'

            if Path(tickers_file).exists():
                log_cli.info(f'{tickers_file} exists, skipping to next date..')
                all_tickers[date] = Path(tickers_file).read_text().splitlines()
                i += 1
                continue

            res = requests.get(f'{self.API_URL}/v1/listed/info', params=params, headers=headers, timeout=30)
            if res.status_code == HTTPStatus.OK:
                d = res.json()
                data = d['info']
                while 'pagination_key' in d:
                    params['pagination_key'] = d['pagination_key']
                    res = requests.get(f'{self.API_URL}/v1/listed/info', params=params, headers=headers, timeout=30)
                    d = res.json()
                    data += d['info']
                df = pd.DataFrame(data)
                if i == 0:
                    log_cli.info(f'sample data (for {date}): ')
                    log_cli.info(df)
                tickers_df = df[['Code']]
                tickers_df = tickers_df.drop_duplicates()
                all_tickers[date] = tickers_df['Code'].tolist()
                tickers_df.to_csv(tickers_file, index=False, header=False)
                log_cli.info(f'tickers written to {tickers_file}')
                i += 1
            elif res.status_code == HTTPStatus.UNAUTHORIZED and 'token is invalid or expired' in res.content.decode(
                'utf-8'
            ):
                log_cli.info('idToken expired or invalid, refreshing.. (usually expires after 24 hours..)')
                if token_error_flag:
                    log_cli.exception('cannot refresh token, check subscription, exiting..')
                    sys.exit(1)
                id_token = self.get_idtoken(refresh=True)
                headers = {'Authorization': f'Bearer {id_token}'}
                token_error_flag = True
                i -= 1
            else:
                log_cli.info(res.json())
                sys.exit(1)

        return all_tickers

    @retry(stop=(stop_after_attempt(6)), wait=wait_random_exponential(min=5, max=60))
    async def query_endpoint(self, endpoint: str, params: dict) -> list[dict] | None:
        """General API query to Jquants fins endpoints."""
        endpoint_url = f'{self.API_URL}/v1/fins/{endpoint}'
        async with httpx.AsyncClient() as client:
            response = await client.get(
                endpoint_url,
                headers=self.HEADERS,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            if response.status_code == HTTPStatus.OK:
                data = []
                if response.json()[endpoint]:
                    data += response.json()[endpoint]
                    while 'pagination_key' in response.json():
                        params['pagination_key'] = response.json()['pagination_key']
                        response = await client.get(
                            endpoint_url,
                            headers=self.HEADERS,
                            params=params,
                            timeout=30,
                        )
                        data += response.json()[endpoint]
                    # log_cli.info(f'{len(data)} {endpoint} acquired for {params=}')
                    return data
                # log_cli.warning(f'empty {endpoint} data for {params=}')
                return None
            log_cli.exception(f'Error: {response.status_code} - {response.text}')
            return None

    @retry(stop=(stop_after_attempt(6)), wait=wait_random_exponential(min=5, max=60))
    async def query_ohlc(self, params: dict) -> list[dict] | None:
        """General API query to Jquants prices endpoints."""
        stub = 'daily_quotes'
        endpoint_url = f'{self.API_URL}/v1/prices/{stub}'
        async with httpx.AsyncClient() as client:
            response = await client.get(
                endpoint_url,
                headers=self.HEADERS,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            if response.status_code == HTTPStatus.OK:
                data = []
                if response.json()[stub]:
                    data += response.json()[stub]
                    while 'pagination_key' in response.json():
                        params['pagination_key'] = response.json()['pagination_key']
                        response = await client.get(
                            endpoint_url,
                            headers=self.HEADERS,
                            params=params,
                            timeout=30,
                        )
                        data += response.json()[stub]
                    # log_cli.info(f'{len(data)} {stub} acquired for {params=}')
                    return data
                # log_cli.warning(f'empty {stub} data for {params=}')
                return None
            log_cli.warning(f'Error: {response.status_code} - {response.text}')
            return None
