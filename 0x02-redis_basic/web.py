import redis
import requests
from typing import Callable
from functools import wraps

def count_url_access(method: Callable) -> Callable:
    @wraps(method)
    def wrapper(url: str) -> str:
        redis_instance = redis.Redis()
        count_key = f"count:{url}"
        redis_instance.incr(count_key)
        return method(url)
    return wrapper

def cache_page(method: Callable) -> Callable:
    @wraps(method)
    def wrapper(url: str) -> str:
        redis_instance = redis.Redis()
        cache_key = f"cache:{url}"
        cached_content = redis_instance.get(cache_key)
        if cached_content is not None:
            return cached_content.decode('utf-8')  # Decode bytes to string
        content = method(url)  # Call the original method (returns string)
        redis_instance.setex(cache_key, 10, content.encode('utf-8'))  # Encode string to bytes for Redis
        return content
    return wrapper

@count_url_access
@cache_page
def get_page(url: str) -> str:
    response = requests.get(url)
    return response.text