from abc import ABCMeta
import asyncio
import collections
import logging
import uuid
import time

import ujson
import numpy as np

from ascendex.util import *
from ascendex.reconnecting_websocket import ReconnectingWebsocket


CHANNELS = ["order", "trades", "ref-px", "bar", "summary", "depth", "bbo"]
OrderInfo = collections.namedtuple(
    "Order", ["symbol", "px", "qty", "order_type", "order_side", "post_only"]
)


class WebSocketClient:
    WS_DOMAIN = "wss://ascendex.com"

    def __init__(self, group_id, api_key="", api_secret=""):
        self.loop = asyncio.get_event_loop()
        url = f"{self.WS_DOMAIN}/{group_id}/{ROUTE_PREFIX}/stream"
        self.ws = ReconnectingWebsocket(loop=self.loop, path=url, coro=self.on_message)
        self.subscribers = {}
        self.responses = collections.defaultdict(dict)

        self.key = api_key
        self.secret = api_secret

    @staticmethod
    def utc_timestamp():
        tm = time.time()
        return int(tm * 1e3)

    async def authenticate(self):
        max_try = 0
        while not self.ws.connected.is_set():
            await asyncio.sleep(1)
            max_try += 1
            if max_try > 10:
                break

        if self.ws.connected.is_set():
            ts = self.utc_timestamp()
            sig = make_auth_headers(ts, "stream", self.key, self.secret)[
                "x-auth-signature"
            ]
            msg = ujson.dumps(
                dict(op="auth", id=self.get_uuid()[:6], t=ts, key=self.key, sig=sig)
            )
            await self.ws.send(msg)
        else:
            logging.error("websocket isn't active")

    @staticmethod
    def _get_subscribe_message(channel, symbols, unsubscribe=False):
        msg_type = "unsub" if unsubscribe else "sub"
        msg = {"op": msg_type, "ch": "{}:{}".format(channel, symbols)}
        return ujson.dumps(msg)

    async def _send_subscribe(self, symbol, channel, unsubscribe=False):
        if self.ws.connected.is_set():
            msg = self._get_subscribe_message(channel, symbol, unsubscribe)
            await self.ws.send(msg)

    async def subscribe(self, topic_id, channel):
        """subscribe a symbol/account to channel"""
        await self._send_subscribe(topic_id, channel)

    async def unsubscribe(self, topic_id, channel):
        """unsubscribe a symbol/account from channel"""
        await self._send_subscribe(topic_id, channel, True)

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
        # logging.debug(message)
        topic = get_message_topic(message)

        if topic == "ping":
            await self.ping_pong()
            return

        if "symbol" in message:
            topic_id = message["symbol"]
        elif "s" in message:
            topic_id = message["s"]
        elif "accountId" in message:
            topic_id = message["accountId"]
        else:
            logging.error(f"unhandled message ${message}")
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
        if "m" in message:
            if "id" in message:
                self.responses[message["m"]][message["id"]].set_result(message)
                del self.responses[message["m"]][message["id"]]
            elif "symbol" in message and message["m"] == "depth-snapshot":
                self.responses[message["m"]][message["symbol"]].set_result(message)
                del self.responses[message["m"]][message["symbol"]]

        await self.publish_data(topic, topic_id, data)

    async def publish_data(self, channel, id, data):
        subscribers = self.subscribers.setdefault(channel, dict()).setdefault(id, set())
        for subscriber in subscribers:
            await subscriber.update(channel, id, data)

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

    async def trade_snapshot(self, symbol):
        """take trade snapshot"""
        req = {
            "op": "req",
            "action": "trade-snapshot",
            "args": {"symbol": "{}".format(symbol), "level": 12},
        }

        await self.ws.send(ujson.dumps(req))

    async def place_new_order(
        self, symbol, px, qty, order_type, order_side, post_only=False, resp_inst="ACK"
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
        order_msg = {
            "op": "req",
            "action": "place-Order",
            "account": "cash",
            "args": {
                "time": self.utc_timestamp(),
                "coid": self.get_uuid(),
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
        return order_msg["args"]["coid"]

    async def cancel_order(self, coid, symbol):
        """
        Cancel an existing order via websocket

        :param coid: server returned order id
        :param symbol: cancel target symbol
        :return:
        """
        cancel_msg = {
            "op": "req",
            "action": "cancel-Order",
            "account": "cash",
            "args": {
                "time": self.utc_timestamp(),
                "coid": self.get_uuid(),
                "origCoid": coid,
                "symbol": symbol,
            },
        }
        await self.ws.send(ujson.dumps(cancel_msg))
        return cancel_msg["coid"]

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
        return await self._wait_response(action="open-order", id=id_)

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
        return await self._wait_response(action="market-trades", id=id_)

    async def order_book_snapshot(self, symbol):
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
        return await self._wait_response(action="depth-snapshot", id=symbol)

    def gen_order(self):
        """generate some random order"""
        symbol = np.random.choice(list(self.get_products()))
        px = round(100 * (1 + (np.random.uniform() - 0.5) * 0.02), 6)
        qty = round(max(1000.0 / px, 0.01), 6)
        order_type = np.random.choice(["market", "limit"])
        order_side = np.random.choice(["buy", "sell"])
        post_only = [True, False][np.random.choice([0, 1])]
        return OrderInfo(symbol, px, qty, order_type, order_side, post_only)

    def get_uuid(self):
        return uuid.uuid4().hex

    async def _wait_response(self, action, id):
        fut = asyncio.Future()
        self.responses[action][id] = fut
        await fut
        result = fut.result()
        return result["data"]

    async def subscribe_subs(self, subscriber, channel, id):
        """
        subscribe
        :param subscriber: subscriber object
        :param channel:  subscribe channel: order, trades, and so on
        :param id: symbol or account id, depending on channel
        :return:
        """
        channel_data = self.subscribers.setdefault(channel, dict())
        subscribers = channel_data.setdefault(id, set())
        subscribers.add(subscriber)
        await self.subscribe(id, channel)

    async def close(self):
        # TODO more graceful cancel
        await self.ws.cancel()


class Subscriber(metaclass=ABCMeta):
    """
    A simple data consumer
    """

    def __init__(self, client):
        # TODO circular reference
        self.client = client

    async def update(self, channel, id, data):
        pass

    async def subscribe(self, channel, id):
        pass
