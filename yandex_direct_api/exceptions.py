class YandexDirectClientError(Exception):
    def __init__(self, *args, **kwargs):
        self.response = kwargs.pop('response', None)
        self.request = kwargs.pop('request', None)


class YandexDirectApiError(Exception):
    def __init__(self, *args, **kwargs):
        self.response = kwargs.pop('response', None)
        self.request = kwargs.pop('request', None)


class YandexDirectConfigError(YandexDirectClientError):
    def __init__(self, *args, **kwargs):
        self.response = kwargs.pop('response', None)
        self.request = kwargs.pop('request', None)