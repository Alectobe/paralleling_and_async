import pytest

from src.sitemap_parser import SitemapParser


@pytest.mark.asyncio
async def test_parse_simple_sitemap(monkeypatch):
    parser = SitemapParser()

    class FakeResponse:
        status = 200

        async def text(self):
            return """<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>https://example.com/</loc></url>
                <url><loc>https://example.com/about</loc></url>
            </urlset>
            """

        def raise_for_status(self):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

    class FakeSession:
        def get(self, url):
            return FakeResponse()

    parser.set_session(FakeSession())
    urls = await parser.fetch_sitemap("https://example.com/sitemap.xml")

    assert "https://example.com/" in urls
    assert "https://example.com/about" in urls