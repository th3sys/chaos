import decimal
import time
import json


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class Connection(object):
    retries = 5

    def __init__(self):
        pass

    @staticmethod
    def ioreliable(func):
        async def _decorator(self, *args, **kwargs):
            tries = 0
            result = await func(self, *args, **kwargs)
            if result is None:
                while result is None and tries < Connection.retries:
                    tries += 1
                    time.sleep(2 ** tries)
                    result = await func(self, *args, **kwargs)
            return result

        return _decorator

    @staticmethod
    def reliable(func):
        def _decorator(self, *args, **kwargs):
            tries = 0
            result = func(self, *args, **kwargs)
            if result is None:
                while result is None and tries < Connection.retries:
                    tries += 1
                    time.sleep(2 ** tries)
                    result = func(self, *args, **kwargs)
            return result

        return _decorator
