import requests
from dotenv import dotenv_values
from jquant_get_tickers import get_idtoken


class JQuantAPIClient:
    HEADERS = ""
    API_URL = ""

    def __init__(self):
        self.classinit()

    @classmethod
    def classinit(cls):
        if cls.HEADERS == "":
            cls.HEADERS = get_idtoken()
        if cls.API_URL == "":
            config = dotenv_values(".env")
            cls.API_URL = config.get("API_URL", cls.API_URL)

    def get_statements(self, ticker):
        """Get financial statements data for a specific ticker

        'You can obtain quarterly earnings summaries and disclosure information 
        (mainly numerical data) on revisions to earnings and dividend information'

        https://jpx.gitbook.io/j-quants-en/api-reference/statements"""

        endpoint_url = f"{self.API_URL}/v1/fins/statements"

        params = {
            "code": ticker,
        }

        response = requests.get(
            endpoint_url, headers=self.HEADERS, params=params
        )

        if response.status_code == 200:
            st = []
            if response.json()["statements"]:
                st += response.json()["statements"]
                while "pagination_key" in response.json():
                    params["pagination_key"] = response.json()["pagination_key"]
                    response = requests.get(
                        f"{self.API_URL}/v1/fins/statements", 
                        headers=self.HEADERS,
                        params=params,
                    )
                    st += response.json()["statements"]
                print(f"{len(st)} statements acquired for {ticker=}")
                return st
            print(f"empty statements data for {ticker=}")
            return None
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    def get_balance_sheet(self, ticker, date):
        """Get detailed balance sheet with current assets/liabilities breakdown

        https://jpx.gitbook.io/j-quants-en/api-reference/statements-1"""

        endpoint_url = f"{self.API_URL}/v1/fins/fs_details"

        response = requests.get(
            endpoint_url, headers=self.HEADERS, params={
                "code": ticker, 
                "date": date
                }
        )

        if response.status_code == 200:
            if fs := response.json()["fs_details"]:
                return fs
            print(f"empty balance sheet data for {ticker=}, {date=}")
            return None
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    def get_dividends(self, ticker, date):
        """Get information on dividends per share of listed companies.

        https://jpx.gitbook.io/j-quants-en/api-reference/dividend"""

        endpoint_url = f"{self.API_URL}/v1/fins/dividend"

        response = requests.get(
            endpoint_url, headers=self.HEADERS, params={"code": ticker, "date": date}
        )

        if response.status_code == 200:
            if fs := response.json()["fs_details"]:
                return fs
            print(f"empty balance sheet data for {ticker=}, {date=}")
            return None
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
