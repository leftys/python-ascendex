import asyncio
import json
import logging
import contextlib
from random import random

import websockets as ws


class ReconnectingWebsocket:
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 10

    def __init__(self, loop, path, coro, reconnect_auth_coro = None):
        self._loop = loop
        self._log = logging.getLogger(__name__)
        self._path = path
        self._coro = coro
        self._reconnect_auth_coro = reconnect_auth_coro
        self._reconnects = 0
        self._conn = None
        self._socket = None
        self.connected = asyncio.Event()

        self._connect()

    def _connect(self):
        self._conn = asyncio.ensure_future(self._run(), loop=self._loop)
        self._conn.add_done_callback(self._handle_conn_done)

    async def start(self):
        await self.connected.wait()

    async def _run(self):
        keep_waiting = True
        self._log.debug("Connecting to %s", self._path)
        async with ws.connect(self._path) as socket:
            self._socket = socket
            self._reconnects = 0
            self.connected.set()
            self._log.info("Connected")

            try:
                while self.connected.is_set():
                    try:
                        evt = await asyncio.wait_for(
                            self._socket.recv(), timeout=self.TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        self._log.debug("no message in {} seconds".format(self.TIMEOUT))
                        await self.send_ping()
                    else:
                        try:
                            evt_obj = json.loads(evt)
                        except ValueError:
                            self._log.error('Unable to parse = %s', evt)
                        else:
                            try:
                                await self._coro(evt_obj)
                            except:
                                self._log.exception(
                                    "Handling message failed = %s", evt_obj
                                )
            except ws.ConnectionClosed as e:
                self._log.info("ws connection closed: %r", e)
                await self._reconnect()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.warning("ws exception: %r", e)
                await self._reconnect()
        self.connected.clear()

    def _handle_conn_done(self, task: asyncio.Task):
        self.connected.clear()
        try:
            task.result()
        except asyncio.CancelledError:
            self._log.debug("ws connection cancelled")
        except Exception:
            self._log.exception("connection finished with exception")

    def _get_reconnect_wait(self, attempts: int) -> int:
        expo = 2 ** attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def _reconnect(self):
        await self.cancel()
        self._reconnects += 1
        if self._reconnects < self.MAX_RECONNECTS:

            self._log.info(
                "websocket {} reconnecting {} reconnects left".format(
                    self._path, self.MAX_RECONNECTS - self._reconnects
                )
            )
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            await asyncio.sleep(reconnect_wait)
            self._connect()
            await self._reconnect_auth_coro()
        else:
            self._log.error("Max reconnections {} reached:".format(self.MAX_RECONNECTS))

    async def send(self, data):
        await self._socket.send(data)

    async def send_ping(self):
        if self._socket:
            await self._socket.ping()

    async def cancel(self):
        self._log.debug("canceling ws connection")
        if self._conn:
            self.connected.clear()
            self._conn.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._conn
        self._socket = None
