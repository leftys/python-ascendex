# python-ascendex

[![PyPi version](https://badgen.net/pypi/v/python-ascendex/)](https://pypi.python.org/pypi/python-ascendex/)
[![PyPI license](https://img.shields.io/pypi/l/python-ascendex.svg)](https://pypi.python.org/pypi/python-ascendex/)

Python API library for AscendEX designed with simplicity and performance in mind. Powered by asyncio and aiosonic. Requires python3.8+. Heavily experimental.

Features:

- almost complete websocket api
- few methods from rest api that are not available in the websocket one
- no order book modelling done
- only cash/spot trading, no margin or futures
- weakly tested error handling

## Usage

RestClient

```python
import ascendex.rest_client
client = ascendex.rest_client.RestClient(GROUP_ID, API_KEY, SECRET)
balance = await client.get_balance()
await client.close()
```

WebSocketClient

```python
import ascendex.web_socket_client

async def on_trade(self, _channel, id, data):
    ...

ws = ascendex.web_socket_client.WebSocketClient(GROUP_ID, API_KEY, SECRET)
await ws.start()
await ws.subscribe("trades", symbol, on_trade)
await ws.place_new_order(symbol, px, qty, order_type, order_side)
await ws.close()
```

If you miss any features, PRs are welcome!
