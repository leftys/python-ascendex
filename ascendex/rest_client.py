import aiosonic
import ujson
import time

from ascendex.util import *
from ascendex.exceptions import AscendexAPIException


class RestClient:
    API_URL = f"https://ascendex.com"
    POOL_SIZE = 10

    def __init__(self, group_id, api_key, api_secret):
        self.group_id = group_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.last_response_headers = {}
        self._timeouts = aiosonic.Timeouts(request_timeout=30)
        self.session = self._init_session()

    def _init_session(self):
        session = aiosonic.HTTPClient(
            connector=aiosonic.TCPConnector(
                pool_size=self.POOL_SIZE,
                timeouts=self._timeouts,
            ),
        )
        return session

    async def close(self):
        await self.session.shutdown()

    async def _request(self, method, path, uri_account = None, include_group = False, version = 'v1', **kwargs):
        '''
        Perform REST api request

        :param method: http method such as 'get'
        :param path: last portion of the api uri path without ROUTE_PREFIX, account and leading slash
        :param uri_account: 'cash' | 'margin' | ...
        :param include_group: should group id be included in uri?
        :param kwargs:
        :return:
        '''
        if method == "get":
            params = kwargs
            data = None
        elif method == "post":
            # Not tested!
            data = kwargs
            params = None
        else:
            raise ValueError(method)

        timestamp = int(time.time() * 1e3)
        headers = make_auth_headers(timestamp, path, self.api_key, self.api_secret)
        if uri_account:
            path = f'{uri_account}/{path}'
        if include_group:
            path = f'{self.group_id}/{ROUTE_PREFIX}/{version}/{path}'
        else:
            path = f'{ROUTE_PREFIX}/{version}/{path}'

        uri = f'{self.API_URL}/{path}'
        # print(uri)
        # print(params)
        # print(data)
        response = await self.session.request(
            uri, method, headers, params, data
        )
        return await self._handle_response(response)

    async def _handle_response(self, response: aiosonic.HttpResponse):
        """Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not str(response.status_code).startswith("2"):
            raise AscendexAPIException(response, await response.text())
        self.last_response_headers = response.headers
        try:
            return ujson.loads(await response.content())
        except ValueError:
            txt = await response.text()
            raise AscendexAPIException("Invalid Response", txt)

    # Exchange Endpoints

    async def get_assets(self):
        res = await self._request("get", "assets")
        return res["data"]

    async def get_products(self):
        res = await self._request("get", "products")
        return res["data"]

    async def get_ticker(self, symbol):
        res = await self._request("get", "ticker", symbol=symbol)
        return res["data"]

    async def get_balance(self):
        res = await self._request("get", "balance", include_group = True, uri_account = 'cash')
        return res["data"]

    async def get_info(self):
        res = await self._request("get", "info")
        return res["data"]

    async def get_fills(self, symbol):
        res = await self._request("get", "order/hist", include_group = True, symbol=symbol, account = 'cash', version = 'v2')
        return res["data"]
