from src.robots_parser import RobotsParser


def test_parse_robots_and_can_fetch():
    parser = RobotsParser()

    robots_text = """
User-agent: *
Disallow: /admin
Allow: /
Crawl-delay: 2
"""

    parser.parse_robots_text("https://example.com", robots_text)

    assert parser.can_fetch("https://example.com/", "*") is True
    assert parser.can_fetch("https://example.com/admin", "*") is False
    assert parser.get_crawl_delay("https://example.com/page", "*") == 2.0


def test_missing_robots_defaults():
    parser = RobotsParser()

    assert parser.can_fetch("https://example.com/page", "*") is True
    assert parser.get_crawl_delay("https://example.com/page", "*") == 0.0