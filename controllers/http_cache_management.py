from functools import wraps
from flask import Response


def http_cached(max_age: int = 86400, public: bool = True, immutable: bool = True):
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if isinstance(result, Response):
                result.cache_control.public = public
                result.cache_control.max_age = max_age
                result.cache_control.immutable = immutable
            return result

        return wrapper
    return decorate
