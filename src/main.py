import asyncio
import time
from typing import Optional
from urllib.parse import urlparse

import aiohttp

from src.models import FetchResult
from src.parser import HTMLParser
from src.queue_manager import CrawlerQueue
from src.semaphore_manager import SemaphoreManager
from src.utils import (
    calculate_speed,
    get_domain,
    normalize_url,
    print_crawl_progress,
    save_json_async,
    setup_logger,
)


logger = setup_logger()


class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        per_domain_limit: int = 3
    ) -> None:
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth

        self.session: Optional[aiohttp.ClientSession] = None
        self.parser = HTMLParser()
        self.queue = CrawlerQueue()
        self.semaphore_manager = SemaphoreManager(
            global_limit=max_concurrent,
            per_domain_limit=per_domain_limit
        )

        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=15,
                connect=5,
                sock_read=10
            )

            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=20,
                ssl=False
            )

            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": "AsyncCrawler/3.0"}
            )

        return self.session

    async def fetch_url(self, url: str) -> FetchResult:
        logger.info(f"▶️ Начало загрузки: {url}")

        try:
            session = await self._get_session()

            async with session.get(url) as response:
                response.raise_for_status()
                text = await response.text()

                logger.info(
                    f"✅ Успешно загружено: {url} | status={response.status} | bytes={len(text)}"
                )

                return FetchResult(
                    url=url,
                    content=text,
                    success=True,
                    status_code=response.status
                )

        except aiohttp.ClientResponseError as error:
            logger.error(
                f"🚫 HTTP ошибка для {url} | status={error.status} | message={error.message}"
            )
            return FetchResult(
                url=url,
                content=None,
                success=False,
                error=f"HTTP error {error.status}: {error.message}",
                status_code=error.status
            )

        except asyncio.TimeoutError:
            logger.error(f"⏰ Таймаут при загрузке: {url}")
            return FetchResult(
                url=url,
                content=None,
                success=False,
                error="Timeout error"
            )

        except aiohttp.ClientError as error:
            logger.error(f"❌ Сетевая ошибка для {url} | {type(error).__name__}: {error}")
            return FetchResult(
                url=url,
                content=None,
                success=False,
                error=f"Client error: {type(error).__name__}: {error}"
            )

        except Exception as error:
            logger.error(f"⚠️ Неожиданная ошибка для {url} | {type(error).__name__}: {error}")
            return FetchResult(
                url=url,
                content=None,
                success=False,
                error=f"Unexpected error: {type(error).__name__}: {error}"
            )

    async def fetch_and_parse(self, url: str) -> dict:
        fetch_result = await self.fetch_url(url)

        if not fetch_result.success or fetch_result.content is None:
            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
                "headings": {"h1": [], "h2": [], "h3": []},
                "tables": [],
                "lists": [],
                "error": fetch_result.error,
            }

        return await self.parser.parse_html(fetch_result.content, url)

    def _should_visit_url(
        self,
        url: str,
        source_domain: str,
        same_domain_only: bool,
        include_patterns: Optional[list[str]],
        exclude_patterns: Optional[list[str]]
    ) -> bool:
        normalized_url = normalize_url(url)

        if normalized_url in self.visited_urls:
            return False

        if normalized_url in self.processed_urls:
            return False

        if normalized_url in self.failed_urls:
            return False

        if same_domain_only and get_domain(normalized_url) != source_domain:
            return False

        if exclude_patterns and any(pattern in normalized_url for pattern in exclude_patterns):
            return False

        if include_patterns and not any(pattern in normalized_url for pattern in include_patterns):
            return False

        parsed = urlparse(normalized_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return False

        return True

    async def _process_queue_item(
        self,
        item,
        max_pages: int,
        source_domains: set[str],
        same_domain_only: bool,
        include_patterns: Optional[list[str]],
        exclude_patterns: Optional[list[str]]
    ) -> None:
        url = item.url
        depth = item.depth

        if len(self.processed_urls) >= max_pages:
            return

        if url in self.visited_urls:
            return

        self.visited_urls.add(url)

        await self.semaphore_manager.acquire(url)
        try:
            parsed_result = await self.fetch_and_parse(url)

            if parsed_result.get("error"):
                self.failed_urls[url] = parsed_result["error"]
                self.queue.mark_failed(url, parsed_result["error"])
                return

            self.processed_urls[url] = parsed_result
            self.queue.mark_processed(url)

            if depth >= self.max_depth:
                return

            for link in parsed_result.get("links", []):
                for source_domain in source_domains:
                    if self._should_visit_url(
                        url=link,
                        source_domain=source_domain,
                        same_domain_only=same_domain_only,
                        include_patterns=include_patterns,
                        exclude_patterns=exclude_patterns
                    ):
                        self.queue.add_url(link, priority=depth + 1, depth=depth + 1)
                        break

        finally:
            await self.semaphore_manager.release(url)

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = True,
        include_patterns: Optional[list[str]] = None,
        exclude_patterns: Optional[list[str]] = None
    ) -> list[dict]:
        start_time = time.perf_counter()

        source_domains = {get_domain(url) for url in start_urls}

        for url in start_urls:
            self.queue.add_url(url, priority=0, depth=0)

        while len(self.processed_urls) < max_pages:
            queue_stats = self.queue.get_stats()

            if queue_stats["queued"] == 0:
                break

            batch = []
            batch_size = min(
                self.max_concurrent,
                queue_stats["queued"],
                max_pages - len(self.processed_urls)
            )

            for _ in range(batch_size):
                item = await self.queue.get_next()
                if item is None:
                    break

                if item.depth > self.max_depth:
                    continue

                batch.append(
                    self._process_queue_item(
                        item=item,
                        max_pages=max_pages,
                        source_domains=source_domains,
                        same_domain_only=same_domain_only,
                        include_patterns=include_patterns,
                        exclude_patterns=exclude_patterns
                    )
                )

            if not batch:
                break

            await asyncio.gather(*batch)

            speed = calculate_speed(start_time, len(self.processed_urls))
            queue_stats = self.queue.get_stats()
            semaphore_stats = self.semaphore_manager.get_stats()

            print_crawl_progress(
                processed=len(self.processed_urls),
                queued=queue_stats["queued"],
                failed=len(self.failed_urls),
                active=semaphore_stats["active_tasks"],
                speed=speed
            )

        print()
        return list(self.processed_urls.values())

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
            logger.info("🔒 HTTP-сессия закрыта")


async def main() -> None:
    crawler = AsyncCrawler(max_concurrent=10, max_depth=2)

    try:
        results = await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=20,
            same_domain_only=True,
            include_patterns=None,
            exclude_patterns=["#"]
        )

        await save_json_async(results, "crawl_results_day3.json")
        print(f"Обработано: {len(results)} страниц")
        print("Результаты сохранены в crawl_results_day3.json")

    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())