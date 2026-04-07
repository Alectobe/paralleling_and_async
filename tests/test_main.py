import pytest

from src.main import AsyncCrawler


@pytest.mark.asyncio
async def test_valid_url():
    crawler = AsyncCrawler(max_concurrent=2, respect_robots=False)

    try:
        result = await crawler.fetch_url("https://example.com")
        assert result.success is True
        assert result.content is not None
        assert len(result.content) > 0
    finally:
        await crawler.close()


@pytest.mark.asyncio
async def test_404_url():
    crawler = AsyncCrawler(max_concurrent=2, respect_robots=False, max_retries=2)

    try:
        result = await crawler.fetch_url("https://httpbin.org/status/404")
        assert result.success is False
        assert result.content is None
        assert "404" in result.error
    finally:
        await crawler.close()


@pytest.mark.asyncio
async def test_fetch_and_parse():
    crawler = AsyncCrawler(max_concurrent=2, respect_robots=False)

    try:
        result = await crawler.fetch_and_parse("https://example.com")
        assert result["url"] == "https://example.com"
        assert "title" in result
        assert "text" in result
        assert "links" in result
        assert "metadata" in result
    finally:
        await crawler.close()


def test_should_visit_url_same_domain():
    crawler = AsyncCrawler(max_concurrent=2, max_depth=2, respect_robots=False)

    result = crawler._should_visit_url(
        url="https://example.com/page",
        source_domain="example.com",
        same_domain_only=True,
        include_patterns=None,
        exclude_patterns=None
    )

    assert result is True


def test_should_visit_url_other_domain_blocked():
    crawler = AsyncCrawler(max_concurrent=2, max_depth=2, respect_robots=False)

    result = crawler._should_visit_url(
        url="https://other.com/page",
        source_domain="example.com",
        same_domain_only=True,
        include_patterns=None,
        exclude_patterns=None
    )

    assert result is False


def test_should_visit_url_include_pattern():
    crawler = AsyncCrawler(max_concurrent=2, max_depth=2, respect_robots=False)

    result = crawler._should_visit_url(
        url="https://example.com/blog/post-1",
        source_domain="example.com",
        same_domain_only=True,
        include_patterns=["/blog/"],
        exclude_patterns=None
    )

    assert result is True


def test_should_visit_url_exclude_pattern():
    crawler = AsyncCrawler(max_concurrent=2, max_depth=2, respect_robots=False)

    result = crawler._should_visit_url(
        url="https://example.com/logout",
        source_domain="example.com",
        same_domain_only=True,
        include_patterns=None,
        exclude_patterns=["logout"]
    )

    assert result is False


@pytest.mark.asyncio
async def test_robots_blocked_url(monkeypatch):
    crawler = AsyncCrawler(max_concurrent=2, respect_robots=True)

    async def fake_fetch_robots(url):
        return {}

    monkeypatch.setattr(crawler.robots_parser, "fetch_robots", fake_fetch_robots)
    monkeypatch.setattr(crawler.robots_parser, "can_fetch", lambda url, ua="*": False)
    monkeypatch.setattr(crawler.robots_parser, "get_crawl_delay", lambda url, ua="*": 0.0)

    try:
        result = await crawler.fetch_url("https://example.com/blocked")
        assert result.success is False
        assert result.error == "Blocked by robots.txt"
    finally:
        await crawler.close()