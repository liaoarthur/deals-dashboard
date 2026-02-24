"""Generic in-memory cache helpers with TTL support."""

import hashlib
import json
import time

_query_cache = {}
_cache_ttl = 3600  # 1 hour cache


def get_cache_key(query_type, **kwargs):
    """Generate a cache key from query parameters"""
    key_string = f"{query_type}:" + json.dumps(kwargs, sort_keys=True)
    return hashlib.md5(key_string.encode()).hexdigest()


def get_cached_result(cache_key):
    """Get cached result if available and not expired"""
    if cache_key in _query_cache:
        result, timestamp = _query_cache[cache_key]
        if time.time() - timestamp < _cache_ttl:
            return result
    return None


def set_cached_result(cache_key, result):
    """Cache a result with timestamp"""
    _query_cache[cache_key] = (result, time.time())
