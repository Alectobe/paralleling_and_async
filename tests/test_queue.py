import pytest

from src.queue_manager import CrawlerQueue


@pytest.mark.asyncio
async def test_queue_priority():
    queue = CrawlerQueue()

    queue.add_url("https://example.com/low", priority=5, depth=0)
    queue.add_url("https://example.com/high", priority=1, depth=0)
    queue.add_url("https://example.com/medium", priority=3, depth=0)

    first = await queue.get_next()
    second = await queue.get_next()
    third = await queue.get_next()

    assert first.url == "https://example.com/high"
    assert second.url == "https://example.com/medium"
    assert third.url == "https://example.com/low"


@pytest.mark.asyncio
async def test_queue_no_duplicates():
    queue = CrawlerQueue()

    added_first = queue.add_url("https://example.com/page", priority=0, depth=0)
    added_second = queue.add_url("https://example.com/page/", priority=0, depth=0)

    assert added_first is True
    assert added_second is False


def test_mark_processed():
    queue = CrawlerQueue()
    queue.add_url("https://example.com/page", priority=0, depth=0)
    queue.mark_processed("https://example.com/page")

    stats = queue.get_stats()
    assert stats["processed"] == 1


def test_mark_failed():
    queue = CrawlerQueue()
    queue.add_url("https://example.com/page", priority=0, depth=0)
    queue.mark_failed("https://example.com/page", "Timeout")

    stats = queue.get_stats()
    assert stats["failed"] == 1
    assert queue.failed_urls["https://example.com/page"] == "Timeout"