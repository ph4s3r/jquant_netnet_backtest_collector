import requests
from http import HTTPStatus
from dotenv import dotenv_values
from jquant_get_tickers import get_idtoken


class JQuantAPIClient:

    """Manage JQuant API Calls."""

    HEADERS = ''
    API_URL = ''

    def __init__(self) -> None:
        self.classinit()

    @classmethod
    def classinit(cls) -> None:
        """Initialize Headers & API_URL only on first instance creation."""
        if cls.HEADERS == '':
            cls.HEADERS = get_idtoken()
        if cls.API_URL == '':
            config = dotenv_values('.env')
            cls.API_URL = config.get('API_URL', cls.API_URL)

    def query_endpoint(self, endpoint: str, ticker: str, analysisdate: str | None = None)-> list[dict] | None:
        """General API query to Jquants fins endpoints."""
        endpoint_url = f'{self.API_URL}/v1/fins/{endpoint}'
        params = {'code': ticker, 'date': analysisdate} if analysisdate else {'code': ticker}

        response = requests.get(
            endpoint_url, headers=self.HEADERS, params=params, timeout=30,
        )
        response.raise_for_status()
        if response.status_code == HTTPStatus.OK:
            data = []
            if response.json()[endpoint]:
                data += response.json()[endpoint]
                while 'pagination_key' in response.json():
                    params['pagination_key'] = response.json()['pagination_key']
                    response = requests.get(
                        endpoint_url,
                        headers=self.HEADERS,
                        params=params,
                        timeout=30,
                    )
                    data += response.json()[endpoint]
                print(f'{len(data)} {endpoint} acquired for {ticker=} & {analysisdate=}')
                return data
            print(f'empty {endpoint} data for {ticker=}')
            return None
        print(f'Error: {response.status_code} - {response.text}')
        return None
