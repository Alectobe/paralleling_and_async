import asyncio
import time
from typing import Optional


class RateLimiter:
    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True) -> None:
        self.requests_per_second = requests_per_second
        self.per_domain = per_domain

        if requests_per_second <= 0:
            self.min_interval = 0.0
        else:
            self.min_interval = 1.0 / requests_per_second

        self._last_request_time = {}
        self._locks = {}
        self._global_lock = asyncio.Lock()

        self.total_acquires = 0
        self.total_wait_time = 0.0

    def _get_key(self, domain: Optional[str]) -> str:
        if self.per_domain:
            return domain or "__global__"
        return "__global__"

    def _get_lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def acquire(self, domain: Optional[str] = None) -> None:
        key = self._get_key(domain)
        lock = self._get_lock(key)

        async with lock:
            now = time.perf_counter()
            last_time = self._last_request_time.get(key)

            if last_time is not None and self.min_interval > 0:
                elapsed = now - last_time
                wait_time = self.min_interval - elapsed

                if wait_time > 0:
                    self.total_wait_time += wait_time
                    await asyncio.sleep(wait_time)

            self._last_request_time[key] = time.perf_counter()
            self.total_acquires += 1

    def get_stats(self) -> dict:
        average_wait = 0.0
        if self.total_acquires > 0:
            average_wait = self.total_wait_time / self.total_acquires

        return {
            "requests_per_second": self.requests_per_second,
            "per_domain": self.per_domain,
            "total_acquires": self.total_acquires,
            "average_wait_time": average_wait,
            "tracked_keys": len(self._last_request_time),
        }