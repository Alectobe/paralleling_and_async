import asyncio
from itertools import count
from typing import Optional

from src.models import QueueItem
from src.utils import normalize_url


class CrawlerQueue:
    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._processed: set[str] = set()
        self._failed: dict[str, str] = {}
        self._queued: set[str] = set()
        self._counter = count()

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        """
        Добавляет URL в очередь.
        Возвращает True, если URL реально добавлен.
        """
        normalized_url = normalize_url(url)

        if normalized_url in self._processed:
            return False

        if normalized_url in self._failed:
            return False

        if normalized_url in self._queued:
            return False

        item = (
            priority,
            next(self._counter),
            QueueItem(priority=priority, depth=depth, url=normalized_url)
        )
        self._queue.put_nowait(item)
        self._queued.add(normalized_url)
        return True

    async def get_next(self) -> Optional[QueueItem]:
        if self._queue.empty():
            return None

        _, _, item = await self._queue.get()
        self._queued.discard(item.url)
        return item

    def mark_processed(self, url: str) -> None:
        self._processed.add(normalize_url(url))

    def mark_failed(self, url: str, error: str) -> None:
        self._failed[normalize_url(url)] = error

    def get_stats(self) -> dict:
        return {
            "queued": self._queue.qsize(),
            "processed": len(self._processed),
            "failed": len(self._failed),
        }

    @property
    def processed_urls(self) -> set[str]:
        return self._processed

    @property
    def failed_urls(self) -> dict[str, str]:
        return self._failed

    @property
    def queued_urls(self) -> set[str]:
        return self._queued