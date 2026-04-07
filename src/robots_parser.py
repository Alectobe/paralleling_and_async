from typing import Optional
from urllib.parse import urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import aiohttp

from src.utils import setup_logger


logger = setup_logger()


class RobotsParser:
    def __init__(self, session: Optional[aiohttp.ClientSession] = None) -> None:
        self.session = session
        self.parsers = {}
        self.robots_urls = {}
        self.fetch_errors = {}

    def set_session(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    def _get_base_domain_url(self, url: str) -> str:
        parsed = urlparse(url)
        return urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))

    def _get_robots_url(self, url: str) -> str:
        base = self._get_base_domain_url(url)
        return f"{base}/robots.txt"

    async def fetch_robots(self, base_url: str) -> dict:
        domain = urlparse(base_url).netloc.lower()

        if domain in self.parsers:
            parser = self.parsers[domain]
            return {
                "domain": domain,
                "robots_url": self.robots_urls.get(domain, ""),
                "fetched": True,
                "available": parser is not None,
                "error": self.fetch_errors.get(domain),
            }

        if self.session is None:
            self.fetch_errors[domain] = "ClientSession is not set"
            self.parsers[domain] = None
            self.robots_urls[domain] = self._get_robots_url(base_url)
            return {
                "domain": domain,
                "robots_url": self.robots_urls[domain],
                "fetched": False,
                "available": False,
                "error": self.fetch_errors[domain],
            }

        robots_url = self._get_robots_url(base_url)
        self.robots_urls[domain] = robots_url

        try:
            async with self.session.get(robots_url) as response:
                if response.status >= 400:
                    self.parsers[domain] = None
                    self.fetch_errors[domain] = f"HTTP {response.status}"
                    return {
                        "domain": domain,
                        "robots_url": robots_url,
                        "fetched": False,
                        "available": False,
                        "error": self.fetch_errors[domain],
                    }

                text = await response.text()
                self.parse_robots_text(base_url, text)

                return {
                    "domain": domain,
                    "robots_url": robots_url,
                    "fetched": True,
                    "available": True,
                    "error": None,
                }

        except Exception as error:
            self.parsers[domain] = None
            self.fetch_errors[domain] = f"{type(error).__name__}: {error}"
            return {
                "domain": domain,
                "robots_url": robots_url,
                "fetched": False,
                "available": False,
                "error": self.fetch_errors[domain],
            }

    def parse_robots_text(self, base_url: str, text: str) -> None:
        domain = urlparse(base_url).netloc.lower()
        robots_url = self._get_robots_url(base_url)

        parser = RobotFileParser()
        parser.set_url(robots_url)
        parser.parse(text.splitlines())

        self.parsers[domain] = parser
        self.robots_urls[domain] = robots_url
        self.fetch_errors.pop(domain, None)

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        domain = urlparse(url).netloc.lower()
        parser = self.parsers.get(domain)

        if parser is None:
            return True

        try:
            return parser.can_fetch(user_agent, url)
        except Exception as error:
            logger.warning(f"⚠️ Ошибка проверки robots.txt для {url}: {type(error).__name__}: {error}")
            return True

    def get_crawl_delay(self, url: str, user_agent: str = "*") -> float:
        domain = urlparse(url).netloc.lower()
        parser = self.parsers.get(domain)

        if parser is None:
            return 0.0

        try:
            delay = parser.crawl_delay(user_agent)
            if delay is None:
                delay = parser.crawl_delay("*")
            return float(delay) if delay is not None else 0.0
        except Exception:
            return 0.0

    def get_stats(self) -> dict:
        return {
            "cached_domains": len(self.parsers),
            "failed_fetches": len(self.fetch_errors),
        }