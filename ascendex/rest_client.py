import asyncio
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
        :param include_group: should account type be included in uri?
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
            content = ujson.loads(await response.content())
        except ValueError:
            txt = await response.text()
            raise AscendexAPIException("Invalid Response", txt)
        if 'message' in content and 'reason' in content:
            raise AscendexAPIException(response, content['reason'] + ': ' + content['message'])
        return content

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

    async def get_candles(self, symbol, interval, start, end):
        '''
        :param interval: Candle interval such as 1 for minute candles and (probably) 60 for hourly.

        WARN: Only at most 500 candles are supported, so you have to employ some paging.
        '''
        # Work around the fact that from is reserved keyword
        kwargs = {'from': start}
        res = await self._request(
            "get",
            "barhist",
            symbol = symbol,
            interval = interval,
            to = end,
            n = 500,
            **kwargs,
        )
        return res["data"]

    async def get_fills_deprecated(self, symbol, limit):
        ''' this returns only fills of really recent orders (within limit) '''
        res = await self._request(
            "get",
            "order/hist/current",
            symbol = symbol,
            executedOnly = True,
            n = limit,
            include_group = True,
            uri_account = 'cash',
            account = 'cash'
        )
        return list(sorted(res["data"], key = lambda item: item['lastExecTime']))

    async def get_order_events(self, symbol, limit, seq_num = None, start_time_ms = None):
        kwargs = {}
        if seq_num is not None:
            kwargs['seqNum'] = seq_num
        if start_time_ms is not None:
            kwargs['startTime'] = start_time_ms
        res = await self._request(
            "get",
            "order/hist",
            include_group = True,
            symbol=symbol,
            account = 'cash',
            version = 'v2',
            limit = limit,
            **kwargs,
        )
        return list(sorted(res["data"], key = lambda item: item['lastExecTime']))

    async def get_fills(self, symbol, since_time_ms):
        fills = []
        order_events = await self.get_order_events(symbol, 1000, start_time_ms = since_time_ms)
        while order_events:
            await asyncio.sleep(1.5)
            fills.extend([event for event in order_events if float(event['fillQty']) > 0])
            seq_num = order_events[-1]['seqNum'] + 1
            order_events = await self.get_order_events(symbol, 1000, seq_num)
        return fills
