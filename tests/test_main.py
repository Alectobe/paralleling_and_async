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
async def test_fetch_and_parse():
    crawler = AsyncCrawler(max_concurrent=2)

    try:
        result = await crawler.fetch_and_parse("https://example.com")
        assert result["url"] == "https://example.com"
        assert "title" in result
        assert "text" in result
        assert "links" in result
        assert "metadata" in result
    finally:
        await crawler.close()