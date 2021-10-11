import ujson


class AscendexAPIException(Exception):
    """Exception class to handle general API Exceptions

    `code` values

    `message` format

    """

    def __init__(self, uri, params, response, data):
        self.uri = uri
        self.params = params
        self.response = response
        self.data = data

    def __str__(self):  # pragma: no cover
        return "AscendexAPIException {}?{}: {}".format(self.uri, self.params, self.data)
