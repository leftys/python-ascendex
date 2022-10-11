import asyncio
import collections
import logging
import uuid
import time

import ujson
import websockets.exceptions

from ascendex.util import *
from ascendex.reconnecting_websocket import ReconnectingWebsocket
from ascendex.exceptions import AscendexAPIException


CHANNELS = ["order", "trades", "ref-px", "bar", "summary", "depth", "bbo"]
OrderInfo = collections.namedtuple(
    "OrderInfo", ["symbol", "px", "qty", "order_type", "order_side", "post_only"]
)


class WebSocketClient:
    WS_DOMAIN = "wss://ascendex.com"

    def __init__(self, group_id, api_key="", api_secret=""):
        self.loop = asyncio.get_event_loop()
        self._url = f"{self.WS_DOMAIN}/{group_id}/{ROUTE_PREFIX}/v1/stream"
        # TODO: the reconnection logic doesnt work properly, therefore we raise exception on reconnect
        self.ws = ReconnectingWebsocket(
            loop=self.loop,
            path=self._url,
            coro=self.on_message,
            reconnect_auth_coro = self._on_reconnect,
        )
        self.subscribers = {}
        self.responses = collections.defaultdict(dict)

        self.key = api_key
        self.secret = api_secret

    @staticmethod
    def utc_timestamp():
        tm = time.time()
        return int(tm * 1e3)

    async def authenticate(self):
        await asyncio.wait_for(self.ws.connected.wait(), timeout = 10)
        ts = self.utc_timestamp()
        sig = make_auth_headers(ts, "stream", self.key, self.secret)[
            "x-auth-signature"
        ]
        id_ = self.get_uuid()[:6]
        msg = ujson.dumps(
            dict(op="auth", id=id_, t=ts, key=self.key, sig=sig)
        )
        await self.ws.send(msg)
        await self._wait_response('auth', id_)

    async def _on_reconnect(self):
        await self.authenticate()
        # Resubscribe
        for channel, ids in self.subscribers.items():
            for id_ in ids:
                await self._send_subscribe(id_, channel)

    @staticmethod
    def _get_subscribe_message(channel, symbols, unsubscribe=False):
        msg_type = "unsub" if unsubscribe else "sub"
        msg = {"op": msg_type, "ch": "{}:{}".format(channel, symbols)}
        return ujson.dumps(msg)

    async def _send_subscribe(self, symbol, channel, unsubscribe=False):
        if self.ws.connected.is_set():
            msg = self._get_subscribe_message(channel, symbol, unsubscribe)
            await self.ws.send(msg)

    async def subscribe(self, channel, id_, coro):
        """
        Subscribe data. Only one subscriber is allowed per channel-id combination!
        :param coro: callback coroutine accepting channel, id and data parameters
        :param channel:  subscribe channel: order, trades, and so on
        :param id: symbol or account id, depending on channel
        :return:
        """
        channel_data = self.subscribers.setdefault(channel, dict())
        subscribers = channel_data.setdefault(id_, set())
        subscribers.add(coro)
        await self._send_subscribe(id_, channel)

    async def unsubscribe(self, channel, id_):
        """unsubscribe a symbol/account from channel"""
        await self._send_subscribe(id_, channel, unsubscribe = True)
        # Clear the defaultdict
        del self.subscribers[channel][id_]
        if not self.subscribers[channel]:
            del self.subscribers[channel]

    async def ping_pong(self):
        """ping pong to keep connection live"""
        if self.ws.connected.is_set():
            msg = ujson.dumps({"op": "ping"})
            await self.ws.send(msg)

    async def on_message(self, message):
        """
        callback fired when a complete WebSocket message was received.
        You usually need to override this method to consume the data.

        :param dict message: message dictionary.
        """
        topic = get_message_topic(message)

        if topic == "ping":
            await self.ping_pong()
            return
        if topic == 'pong':
            # Ignore pong replies
            return
        if topic == 'sub':
            # Ignore subscription replies
            return
        if topic == 'connected' and message['type'] == 'unauth':
            # Ignore connected message
            return

        try:
            if "m" in message:
                if "id" in message:
                    self.responses[message["m"]][message["id"]].set_result(message)
                    del self.responses[message["m"]][message["id"]]
                    return
                elif "symbol" in message and message["m"] == "depth-snapshot":
                    self.responses[message["m"]][message["symbol"]].set_result(message)
                    del self.responses[message["m"]][message["symbol"]]
                    return
                elif 'info' in message and 'id' in message['info'] and message['m'] != 'error':
                    self.responses[message["m"]][message["info"]['id']].set_result(message)
                    del self.responses[message["m"]][message["info"]['id']]
                    return
        except KeyError:
            if 'reason' in message and message['reason'] == 'AUTHORIZATION_NEEDED':
                # Authorization neede message probably comes as a second response to the message id.
                pass

        if "symbol" in message:
            id_ = message["symbol"]
        elif "s" in message:
            id_ = message["s"]
        elif topic == 'order' and 'ac' in message:
            id_ = message['ac'].lower()
        elif topic == 'balance' and 'ac' in message:
            id_ = message['ac'].lower()
        elif "accountId" in message:
            id_ = message["accountId"]
        elif "id" in message:
            id_ = message['id']
        else:
            logging.warning(f"unhandled message ${message}")
            return

        if "data" in message:
            data = message["data"]
        elif "info" in message:
            data = message["info"]
        elif topic == "bar" or topic == "summary":
            data = message
        else:
            logging.error(f"no data info: ${message}")
            return

        try:
            subscribers = self.subscribers[topic][id_]
        except KeyError:
            logging.info(f'no subscribers ${message}')
        else:
            for subscriber in subscribers:
                await subscriber(topic, id_, data)

    @staticmethod
    def get_channels(self):
        """get all supported sub/unsub channels"""
        return CHANNELS

    async def depth_snapshot(self, symbol):
        """take depth snapshot"""
        req = {
            "op": "req",
            "action": "depth-snapshot",
            "args": {"symbol": "{}".format(symbol)},
        }

        await self.ws.send(ujson.dumps(req))

    async def trade_snapshot(self, symbol, length = 500):
        """take trade snapshot"""
        req = {
            "op": "req",
            "action": "trade-snapshot",
            "args": {"symbol": "{}".format(symbol), "level": length},
        }

        await self.ws.send(ujson.dumps(req))

    async def place_new_order(
        self, symbol, px, qty, order_type, order_side, post_only=False, resp_inst="ACK", client_order_id = None,
    ):
        """
        Place a new order via websocket

        :param symbol: product symbol
        :param px: order price
        :param qty: order size
        :param order_type: limit or market
        :param order_side: buy or sell
        :param post_only: if postonly
        :param resp_inst: 'ACK', 'ACCEPT', or 'DONE'
        :return: user generated coid for this order
        """
        if not client_order_id:
            client_order_id = self.get_uuid()
        order_msg = {
            "op": "req",
            "action": "place-Order",
            "account": "cash",
            "args": {
                "time": self.utc_timestamp(),
                "id": client_order_id,
                "symbol": symbol,
                "orderPrice": str(px),
                "orderQty": str(qty),
                "orderType": order_type,
                "side": order_side,
                "postOnly": post_only,
                "respInst": resp_inst,
            },
        }
        await self.ws.send(ujson.dumps(order_msg))
        return await self._wait_response(action="order", id_=client_order_id)


    async def cancel_order(self, coid, symbol):
        """
        Cancel an existing order via websocket

        :param coid: server returned order id
        :param symbol: cancel target symbol
        :return:
        """
        id_ = self.get_uuid()
        cancel_msg = {
            "op": "req",
            "action": "cancel-Order",
            "account": "cash",
            "args": {
                "time": self.utc_timestamp(),
                "id": id_,
                "orderId": coid,
                "symbol": symbol,
                'respInst': 'DONE',
            },
        }
        await self.ws.send(ujson.dumps(cancel_msg))
        return await self._wait_response(action="order", id_=id_)

    async def cancel_all_orders(self, symbol=None):
        """
        Cancel all open orders.

        :param symbol: optional
        :return:
        """
        cancel_msg = {
            "op": "req",
            "action": "cancel-All",
            "ac": "CASH",
            "args": {"time": self.utc_timestamp(), "symbol": symbol},
        }

        await self.ws.send(ujson.dumps(cancel_msg))

    async def get_open_orders(self, symbol=None):
        """
        Get open orders.

        :param symbol: optional
        :return:
        """
        id_ = self.get_uuid()
        msg = {
            "op": "req",
            "action": "open-order",
            "id": id_,
            "account": "cash",
            "args": {"symbols": symbol},
        }

        await self.ws.send(ujson.dumps(msg))
        return await self._wait_response(action="open-order", id_=id_)

    async def recent_trades(self, symbol, amount=500):
        """
        Get recent trades.
        """
        id_ = self.get_uuid()
        msg = {
            "op": "req",
            "action": "market-trades",
            "id": id_,
            "args": {
                "symbol": symbol,
                "level": amount,
            },
        }

        await self.ws.send(ujson.dumps(msg))
        response = await self._wait_response(action="market-trades", id_=id_)
        response = list(sorted(response, key = lambda item: item['ts']))
        return response

    async def get_order_book(self, symbol):
        """
        Get recent trades.
        """
        id_ = self.get_uuid()
        msg = {
            "op": "req",
            "action": "depth-snapshot",
            "id": id_,
            "args": {
                "symbol": symbol,
            },
        }

        await self.ws.send(ujson.dumps(msg))
        return await self._wait_response(action="depth-snapshot", id_=symbol)

    @staticmethod
    def get_uuid():
        return uuid.uuid4().hex

    async def _wait_response(self, action, id_):
        '''
        Wait for reply on websocket, match the reply by action and id, return reply's 'data' or 'info' key.
        'data' is the usual response content key, but 'info' is sent for order actions.
        '''
        fut = asyncio.Future()
        self.responses[action][id_] = fut
        await fut
        result = fut.result()
        if 'reason' in result:
            raise AscendexAPIException(self._url, action, result['reason'], result)
        try:
            return result["data"]
        except KeyError:
            try:
                return result['info']
            except KeyError:
                return result

    async def start(self):
        await self.ws.start()
        await self.authenticate()

    async def close(self):
        # TODO more graceful cancel
        await self.ws.cancel()
