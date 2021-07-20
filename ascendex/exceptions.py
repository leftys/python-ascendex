import ujson


class AscendexAPIException(Exception):
    """Exception class to handle general API Exceptions

    `code` values

    `message` format

    """

    def __init__(self, response, data):
        self.response = response
        self.data = data

    def __str__(self):  # pragma: no cover
        return "AscendexAPIException {}: {}".format(self.response, self.data)
