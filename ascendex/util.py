import hmac
import hashlib
import base64


ROUTE_PREFIX = "api/pro"


def sign(msg, secret):
    msg = bytearray(msg.encode("utf-8"))
    hmac_key = base64.b64decode(secret)
    signature = hmac.new(hmac_key, msg, hashlib.sha256)
    signature_b64 = base64.b64encode(signature.digest()).decode("utf-8")
    return signature_b64


def make_auth_headers(timestamp, path, apikey, secret):
    # convert timestamp to string
    if isinstance(timestamp, bytes):
        timestamp = timestamp.decode("utf-8")
    elif isinstance(timestamp, int):
        timestamp = str(timestamp)

    msg = f"{timestamp}+{path}"

    header = {
        "x-auth-key": apikey,
        "x-auth-signature": sign(msg, secret),
        "x-auth-timestamp": timestamp,
    }

    return header


# def gen_server_order_id(user_uid, cl_order_id, ts, order_src = 'a'):
# 	"""
# 	Server order generator based on user info and input.
# 	:param user_uid: user uid
# 	:param cl_order_id: user random digital and number id
# 	:param ts: order timestamp in milliseconds
# 	:param order_src: 'a' for rest api order, 's' for websocket order.
# 	:return: order id of length 32
# 	"""
#
# 	return (order_src + format(ts, 'x')[-11:] + user_uid[-11:] + cl_order_id[-9:])[:32]


def get_message_topic(message):
    if "m" in message:
        topic = message["m"]
    elif "message" in message:
        topic = message["message"]
    else:
        topic = None
    return topic
