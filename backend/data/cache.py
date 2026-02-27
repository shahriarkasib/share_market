"""Simple in-memory cache with TTL."""

import time
from typing import Any, Optional


class TTLCache:
    """Thread-safe in-memory cache with per-key TTL."""

    def __init__(self):
        self._cache: dict[str, tuple[Any, float]] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get value if not expired."""
        if key in self._cache:
            value, expires_at = self._cache[key]
            if time.time() < expires_at:
                return value
            del self._cache[key]
        return None

    def set(self, key: str, value: Any, ttl: int):
        """Set value with TTL in seconds."""
        self._cache[key] = (value, time.time() + ttl)

    def delete(self, key: str):
        """Remove a key."""
        self._cache.pop(key, None)

    def clear(self):
        """Clear all cached data."""
        self._cache.clear()

    def cleanup(self):
        """Remove all expired entries."""
        now = time.time()
        expired = [k for k, (_, exp) in self._cache.items() if now >= exp]
        for k in expired:
            del self._cache[k]


# Global cache instance
cache = TTLCache()
