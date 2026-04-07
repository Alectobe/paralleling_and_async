import pytest

from src.main import AsyncCrawler


@pytest.mark.asyncio
async def test_valid_url():
    crawler = AsyncCrawler(max_concurrent=2)

    try:
        result = await crawler.fetch_url("https://example.com")
        assert result.success is True
        assert result.content is not None
        assert len(result.content) > 0
    finally:
        await crawler.close()


@pytest.mark.asyncio
async def test_404_url():
    crawler = AsyncCrawler(max_concurrent=2)

    try:
        result = await crawler.fetch_url("https://httpbin.org/status/404")
        assert result.success is False
        assert result.content is None
        assert result.status_code == 404
    finally:
        await crawler.close()


@pytest.mark.asyncio
async def test_invalid_domain():
    crawler = AsyncCrawler(max_concurrent=2)

    try:
        result = await crawler.fetch_url("https://this-domain-does-not-exist-123456789.com")
        assert result.success is False
        assert result.content is None
        assert result.error is not None
    finally:
        await crawler.close()


@pytest.mark.asyncio
async def test_fetch_multiple_urls():
    crawler = AsyncCrawler(max_concurrent=3)

    urls = [
        "https://example.com",
        "https://httpbin.org/html"
    ]

    try:
        results = await crawler.fetch_urls(urls)
        assert len(results) == 2
        assert all(result.url in urls for result in results)
    finally:
        await crawler.close()