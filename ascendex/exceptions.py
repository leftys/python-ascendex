import ujson


class AscendexAPIException(Exception):
    """Exception class to handle general API Exceptions

    `code` values

    `message` format

    """

    def __init__(self, response, text):
        self.code = ""
        self.message = "Unknown Error"
        try:
            json_res = ujson.loads(text)
        except ValueError:
            self.message = response.content
        else:
            if "error" in json_res:
                self.message = json_res["error"]
            if "msg" in json_res:
                self.message = json_res["msg"]
            if "message" in json_res and json_res["message"] != "No message available":
                self.message += " - {}".format(json_res["message"])
            if "code" in json_res:
                self.code = json_res["code"]
            if "data" in json_res:
                try:
                    self.message += " " + ujson.dumps(json_res["data"])
                except ValueError:
                    pass

        self.status_code = response.status
        self.response = response
        self.request = getattr(response, "request", None)

    def __str__(self):  # pragma: no cover
        return "AscendexAPIException {}: {}".format(self.code, self.message)
