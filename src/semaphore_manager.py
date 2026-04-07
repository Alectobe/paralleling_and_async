import asyncio
from urllib.parse import urlparse


class SemaphoreManager:
    def __init__(self, global_limit: int = 10, per_domain_limit: int = 3) -> None:
        self.global_semaphore = asyncio.Semaphore(global_limit)
        self.per_domain_limit = per_domain_limit
        self.domain_semaphores: dict[str, asyncio.Semaphore] = {}
        self.active_tasks = 0
        self._lock = asyncio.Lock()

    def _get_domain(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    def _get_domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        if domain not in self.domain_semaphores:
            self.domain_semaphores[domain] = asyncio.Semaphore(self.per_domain_limit)
        return self.domain_semaphores[domain]

    async def acquire(self, url: str) -> str:
        domain = self._get_domain(url)
        domain_semaphore = self._get_domain_semaphore(domain)

        await self.global_semaphore.acquire()
        await domain_semaphore.acquire()

        async with self._lock:
            self.active_tasks += 1

        return domain

    async def release(self, url: str) -> None:
        domain = self._get_domain(url)
        domain_semaphore = self._get_domain_semaphore(domain)

        domain_semaphore.release()
        self.global_semaphore.release()

        async with self._lock:
            self.active_tasks = max(0, self.active_tasks - 1)

    def get_stats(self) -> dict:
        return {
            "active_tasks": self.active_tasks,
            "tracked_domains": len(self.domain_semaphores),
        }