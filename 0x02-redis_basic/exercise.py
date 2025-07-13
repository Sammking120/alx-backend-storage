import redis
import uuid
from typing import Union, Callable, Optional
from functools import wraps

def count_calls(method: Callable) -> Callable:
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        key = method.__qualname__
        self._redis.incr(key)
        return method(self, *args, **kwargs)
    return wrapper

class Cache:
    def __init__(self):
        self._redis = redis.Redis()
        self._redis.flushdb()
    
    def store(self, data: Union[str, bytes, int, float]) -> str:
        key = str(uuid.uuid4())
        self._redis.set(key, data)
        return key
    
    def get(self, key: str, fn: Optional[Callable] = None) -> Union[str, bytes, int, float, None]:
        data = self._redis.get(key)
        if data is None:
            return None
        if fn is not None:
            return fn(data)
        # Ensure data is of the correct type (bytes, str, int, float, or None)
        if isinstance(data, (str, bytes, int, float)):
            return data
        return None
    
    def get_str(self, key: str) -> Optional[str]:
        result = self.get(key, lambda x: x.decode('utf-8'))
        return result if isinstance(result, str) or result is None else None
    
    def get_int(self, key: str) -> Optional[int]:
        result = self.get(key)
        if result is None:
            return None
        if isinstance(result, int):
            return result
        if isinstance(result, bytes):
            try:
                return int(result.decode('utf-8'))
            except (ValueError, UnicodeDecodeError):
                return None
        if isinstance(result, str):
            try:
                return int(result)
            except ValueError:
                return None
        if isinstance(result, float):
            return int(result)
        return None
    