import argparse
import asyncio
import logging
import random
import time
from logging.handlers import RotatingFileHandler
from typing import Optional
from urllib.parse import urlparse

import aiohttp

from src.config_loader import load_config
from src.errors import (
    CircuitBreakerOpenError,
    NetworkError,
    ParseError,
    PermanentError,
    RobotsBlockedError,
    TransientError,
)
from src.models import FetchResult
from src.parser import HTMLParser
from src.queue_manager import CrawlerQueue
from src.rate_limiter import RateLimiter
from src.retry_strategy import RetryStrategy
from src.robots_parser import RobotsParser
from src.semaphore_manager import SemaphoreManager
from src.sitemap_parser import SitemapParser
from src.stats import CrawlerStats
from src.storage import CSVStorage, JSONStorage, SQLiteStorage
from src.utils import (
    calculate_speed,
    get_domain,
    normalize_url,
    print_crawl_progress,
    save_dict_json_async,
    setup_logger,
)


logger = setup_logger()


def setup_file_logging(log_file: str = "crawler.log", level: str = "INFO") -> None:
    root_logger = logging.getLogger()
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root_logger.setLevel(numeric_level)

    has_rotating = any(isinstance(handler, RotatingFileHandler) for handler in root_logger.handlers)
    if has_rotating:
        return

    handler = RotatingFileHandler(
        log_file,
        maxBytes=1_000_000,
        backupCount=3,
        encoding="utf-8"
    )
    handler.setLevel(numeric_level)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    root_logger.addHandler(handler)


class AdvancedCrawler:
    def __init__(
        self,
        start_urls: Optional[list] = None,
        max_pages: int = 100,
        max_concurrent: int = 10,
        max_depth: int = 2,
        per_domain_limit: int = 3,
        requests_per_second: float = 1.0,
        respect_robots: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        user_agent: str = "AdvancedCrawler/7.0",
        user_agents: Optional[list] = None,
        max_retries: int = 3,
        backoff_base: float = 0.5,
        timeout_connect: float = 5.0,
        timeout_read: float = 15.0,
        timeout_total: float = 20.0,
        circuit_breaker_enabled: bool = True,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 30.0,
        same_domain_only: bool = True,
        include_patterns: Optional[list] = None,
        exclude_patterns: Optional[list] = None,
        storage=None,
        use_sitemap: bool = False,
        log_file: str = "crawler.log",
        log_level: str = "INFO"
    ) -> None:
        self.start_urls = start_urls or []
        self.max_pages = max_pages
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.requests_per_second = requests_per_second
        self.respect_robots = respect_robots
        self.min_delay = min_delay
        self.jitter = jitter
        self.user_agent = user_agent
        self.user_agents = user_agents or [user_agent]
        self.backoff_base = backoff_base
        self.timeout_connect = timeout_connect
        self.timeout_read = timeout_read
        self.timeout_total = timeout_total
        self.circuit_breaker_enabled = circuit_breaker_enabled
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.circuit_breaker_timeout = circuit_breaker_timeout
        self.same_domain_only = same_domain_only
        self.include_patterns = include_patterns
        self.exclude_patterns = exclude_patterns or []
        self.storage = storage
        self.use_sitemap = use_sitemap

        setup_file_logging(log_file=log_file, level=log_level)

        self.session: Optional[aiohttp.ClientSession] = None
        self.parser = HTMLParser()
        self.queue = CrawlerQueue()
        self.semaphore_manager = SemaphoreManager(
            global_limit=max_concurrent,
            per_domain_limit=per_domain_limit
        )
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=True
        )
        self.robots_parser = RobotsParser()
        self.sitemap_parser = SitemapParser()
        self.stats = CrawlerStats()

        self.retry_strategy = RetryStrategy(
            max_retries=max_retries,
            backoff_factor=2.0,
            retry_on=[TransientError, NetworkError, CircuitBreakerOpenError],
            base_delay=backoff_base,
            max_delay=30.0,
            type_limits={
                "TransientError": max_retries,
                "NetworkError": max_retries,
                "PermanentError": 0,
                "ParseError": 0,
                "RobotsBlockedError": 0,
                "CircuitBreakerOpenError": max_retries,
            },
            type_backoff={
                "TransientError": 2.0,
                "NetworkError": 2.0,
                "CircuitBreakerOpenError": 1.5,
            }
        )

        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.permanent_error_urls = {}
        self.storage_errors = []
        self.domain_error_counts = {}
        self.domain_open_until = {}

        self.request_timestamps = []
        self.delay_history = []
        self.robots_blocked_count = 0
        self.total_request_attempts = 0

    @classmethod
    def from_config(cls, config_path: str):
        config = load_config(config_path)

        storage_config = config.get("storage", {})
        storage_type = storage_config.get("type", "json")
        storage_path = storage_config.get("path", "results.json")

        storage = None
        if storage_type == "json":
            storage = JSONStorage(storage_path)
        elif storage_type == "csv":
            storage = CSVStorage(storage_path)
        elif storage_type == "sqlite":
            storage = SQLiteStorage(storage_path, batch_size=storage_config.get("batch_size", 10))

        return cls(
            start_urls=config.get("start_urls", []),
            max_pages=config.get("max_pages", 100),
            max_concurrent=config.get("max_concurrent", 10),
            max_depth=config.get("max_depth", 2),
            per_domain_limit=config.get("per_domain_limit", 3),
            requests_per_second=config.get("requests_per_second", 1.0),
            respect_robots=config.get("respect_robots", True),
            min_delay=config.get("min_delay", 0.0),
            jitter=config.get("jitter", 0.0),
            user_agent=config.get("user_agent", "AdvancedCrawler/7.0"),
            user_agents=config.get("user_agents"),
            max_retries=config.get("max_retries", 3),
            backoff_base=config.get("backoff_base", 0.5),
            timeout_connect=config.get("timeout_connect", 5.0),
            timeout_read=config.get("timeout_read", 15.0),
            timeout_total=config.get("timeout_total", 20.0),
            circuit_breaker_enabled=config.get("circuit_breaker_enabled", True),
            circuit_breaker_threshold=config.get("circuit_breaker_threshold", 5),
            circuit_breaker_timeout=config.get("circuit_breaker_timeout", 30.0),
            same_domain_only=config.get("same_domain_only", True),
            include_patterns=config.get("include_patterns"),
            exclude_patterns=config.get("exclude_patterns"),
            storage=storage,
            use_sitemap=config.get("use_sitemap", False),
            log_file=config.get("log_file", "crawler.log"),
            log_level=config.get("log_level", "INFO"),
        )

    def _get_random_user_agent(self) -> str:
        return random.choice(self.user_agents)

    def _get_timeout_for_attempt(self, attempt: int) -> aiohttp.ClientTimeout:
        multiplier = 1 + max(0, attempt - 1) * 0.5
        return aiohttp.ClientTimeout(
            total=self.timeout_total * multiplier,
            connect=self.timeout_connect * multiplier,
            sock_read=self.timeout_read * multiplier
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=self.timeout_total,
                connect=self.timeout_connect,
                sock_read=self.timeout_read
            )

            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=20,
                ssl=False
            )

            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": self.user_agent}
            )
            self.robots_parser.set_session(self.session)
            self.sitemap_parser.set_session(self.session)

        return self.session

    def _get_average_delay(self) -> float:
        if not self.delay_history:
            return 0.0
        return sum(self.delay_history) / len(self.delay_history)

    def _get_request_rate(self) -> float:
        elapsed = self.stats.get_elapsed_time()
        if elapsed <= 0:
            return 0.0
        return self.total_request_attempts / elapsed

    def _estimate_remaining_time(self) -> float:
        speed = self.stats.get_average_speed()
        if speed <= 0:
            return 0.0
        remaining = max(0, self.max_pages - len(self.processed_urls))
        return remaining / speed

    def _is_circuit_open(self, domain: str) -> bool:
        if not self.circuit_breaker_enabled:
            return False

        open_until = self.domain_open_until.get(domain)
        if open_until is None:
            return False

        if time.perf_counter() >= open_until:
            self.domain_open_until.pop(domain, None)
            self.domain_error_counts[domain] = 0
            return False

        return True

    def _register_domain_failure(self, domain: str) -> None:
        if not self.circuit_breaker_enabled:
            return

        count = self.domain_error_counts.get(domain, 0) + 1
        self.domain_error_counts[domain] = count

        if count >= self.circuit_breaker_threshold:
            self.domain_open_until[domain] = time.perf_counter() + self.circuit_breaker_timeout
            logger.warning(f"🚫 Circuit breaker открыт для домена {domain}")

    def _register_domain_success(self, domain: str) -> None:
        self.domain_error_counts[domain] = 0
        self.domain_open_until.pop(domain, None)

    def _classify_http_error(self, status: int, url: str, message: str) -> Exception:
        if status in (401, 403, 404):
            return PermanentError(f"HTTP {status}: {message}", url=url)

        if status in (429, 500, 502, 503, 504):
            return TransientError(f"HTTP {status}: {message}", url=url)

        if 400 <= status < 500:
            return PermanentError(f"HTTP {status}: {message}", url=url)

        if status >= 500:
            return TransientError(f"HTTP {status}: {message}", url=url)

        return TransientError(f"HTTP {status}: {message}", url=url)

    async def _apply_politeness_rules(self, url: str, current_user_agent: str) -> None:
        domain = get_domain(url)

        if self._is_circuit_open(domain):
            raise CircuitBreakerOpenError(f"Circuit breaker open for {domain}", url=url)

        if self.respect_robots:
            await self.robots_parser.fetch_robots(url)

            if not self.robots_parser.can_fetch(url, current_user_agent):
                self.robots_blocked_count += 1
                logger.warning(f"🚫 URL заблокирован robots.txt: {url}")
                raise RobotsBlockedError("Blocked by robots.txt", url=url)

            robots_delay = self.robots_parser.get_crawl_delay(url, current_user_agent)
        else:
            robots_delay = 0.0

        await self.rate_limiter.acquire(domain)

        extra_delay = max(self.min_delay, robots_delay)
        if self.jitter > 0:
            extra_delay += random.uniform(0.0, self.jitter)

        if extra_delay > 0:
            self.delay_history.append(extra_delay)
            await asyncio.sleep(extra_delay)

    async def _single_fetch_attempt(self, url: str, attempt: int = 1) -> FetchResult:
        current_user_agent = self._get_random_user_agent()
        domain = get_domain(url)

        await self._apply_politeness_rules(url, current_user_agent)
        await self.semaphore_manager.acquire(url)

        try:
            session = await self._get_session()
            timeout = self._get_timeout_for_attempt(attempt)

            self.total_request_attempts += 1
            self.stats.record_request_attempt()
            self.request_timestamps.append(time.perf_counter())

            logger.info(f"▶️ Начало загрузки: {url} | attempt={attempt}")

            try:
                async with session.get(
                    url,
                    headers={"User-Agent": current_user_agent},
                    timeout=timeout
                ) as response:
                    if response.status >= 400:
                        raise self._classify_http_error(
                            status=response.status,
                            url=url,
                            message=response.reason
                        )

                    text = await response.text()
                    self._register_domain_success(domain)

                    logger.info(
                        f"✅ Успешно загружено: {url} | status={response.status} | bytes={len(text)}"
                    )

                    return FetchResult(
                        url=url,
                        content=text,
                        success=True,
                        error=None,
                        status_code=response.status
                    )

            except asyncio.TimeoutError:
                self._register_domain_failure(domain)
                raise TransientError("Timeout error", url=url)

            except aiohttp.ClientConnectorError as error:
                self._register_domain_failure(domain)
                raise NetworkError(f"ClientConnectorError: {error}", url=url)

            except aiohttp.ClientOSError as error:
                self._register_domain_failure(domain)
                raise NetworkError(f"ClientOSError: {error}", url=url)

            except aiohttp.ClientError as error:
                self._register_domain_failure(domain)
                raise NetworkError(f"ClientError: {error}", url=url)

        finally:
            await self.semaphore_manager.release(url)

    async def fetch_url(self, url: str) -> FetchResult:
        attempt_holder = {"attempt": 0}

        async def wrapped_fetch():
            attempt_holder["attempt"] += 1
            return await self._single_fetch_attempt(url, attempt=attempt_holder["attempt"])

        try:
            result = await self.retry_strategy.execute_with_retry(wrapped_fetch)
            self.stats.record_page(url, True, result.status_code)
            return result

        except PermanentError as error:
            logger.error(f"❌ Постоянная ошибка | url={url} | error={error}")
            self.permanent_error_urls[url] = str(error)
            self.stats.record_page(url, False, None)
            return FetchResult(url=url, content=None, success=False, error=str(error))

        except RobotsBlockedError as error:
            logger.error(f"❌ robots blocked | url={url} | error={error}")
            self.permanent_error_urls[url] = str(error)
            self.stats.record_page(url, False, None)
            return FetchResult(url=url, content=None, success=False, error=str(error))

        except ParseError as error:
            logger.error(f"❌ Ошибка парсинга | url={url} | error={error}")
            self.stats.record_page(url, False, None)
            return FetchResult(url=url, content=None, success=False, error=str(error))

        except Exception as error:
            logger.error(f"❌ Временная/сетевая ошибка после всех повторов | url={url} | error={error}")
            self.stats.record_page(url, False, None)
            return FetchResult(url=url, content=None, success=False, error=str(error))

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
                "status_code": fetch_result.status_code,
                "content_type": "",
            }

        try:
            parsed = await self.parser.parse_html(fetch_result.content, url)
            parsed["status_code"] = fetch_result.status_code
            parsed["content_type"] = "text/html"
            return parsed
        except Exception as error:
            raise ParseError(f"{type(error).__name__}: {error}", url=url)

    def _prepare_storage_record(self, parsed_result: dict) -> dict:
        from datetime import datetime, timezone

        return {
            "url": parsed_result.get("url", ""),
            "title": parsed_result.get("title", ""),
            "text": parsed_result.get("text", ""),
            "links": parsed_result.get("links", []),
            "metadata": parsed_result.get("metadata", {}),
            "crawled_at": datetime.now(timezone.utc).isoformat(),
            "status_code": parsed_result.get("status_code", 200),
            "content_type": parsed_result.get("content_type", "text/html"),
        }

    async def _save_result(self, parsed_result: dict) -> None:
        if self.storage is None:
            return

        record = self._prepare_storage_record(parsed_result)

        try:
            await self.storage.save(record)
            self.stats.record_storage_saved()
        except Exception as error:
            message = f"{type(error).__name__}: {error}"
            self.storage_errors.append(
                {
                    "url": record.get("url", ""),
                    "error": message,
                }
            )
            self.stats.record_storage_error()
            logger.error(f"💾 Ошибка сохранения | url={record.get('url', '')} | error={message}")

    def _should_visit_url(
        self,
        url: str,
        source_domain: str,
        same_domain_only: Optional[bool] = None,
        include_patterns: Optional[list] = None,
        exclude_patterns: Optional[list] = None
    ) -> bool:
        normalized_url = normalize_url(url)

        if normalized_url in self.visited_urls:
            return False

        if normalized_url in self.processed_urls:
            return False

        if normalized_url in self.failed_urls:
            return False

        effective_same_domain_only = self.same_domain_only if same_domain_only is None else same_domain_only
        effective_include_patterns = self.include_patterns if include_patterns is None else include_patterns
        effective_exclude_patterns = self.exclude_patterns if exclude_patterns is None else exclude_patterns

        if effective_same_domain_only and get_domain(normalized_url) != source_domain:
            return False

        if effective_exclude_patterns and any(pattern in normalized_url for pattern in effective_exclude_patterns):
            return False

        if effective_include_patterns and not any(pattern in normalized_url for pattern in effective_include_patterns):
            return False

        parsed = urlparse(normalized_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return False

        return True

    async def _process_queue_item(self, item, source_domains: set) -> None:
        url = item.url
        depth = item.depth

        if len(self.processed_urls) >= self.max_pages:
            return

        if url in self.visited_urls:
            return

        self.visited_urls.add(url)
        parsed_result = await self.fetch_and_parse(url)

        if parsed_result.get("error"):
            self.failed_urls[url] = parsed_result["error"]
            self.queue.mark_failed(url, parsed_result["error"])
            return

        self.processed_urls[url] = parsed_result
        self.queue.mark_processed(url)

        await self._save_result(parsed_result)

        if depth >= self.max_depth:
            return

        for link in parsed_result.get("links", []):
            for source_domain in source_domains:
                if self._should_visit_url(link, source_domain):
                    self.queue.add_url(link, priority=depth + 1, depth=depth + 1)
                    break

    async def _enqueue_sitemap_urls(self) -> None:
        if not self.use_sitemap:
            return

        for url in self.start_urls:
            urls_from_sitemap = await self.sitemap_parser.discover_and_fetch(url)
            for sitemap_url in urls_from_sitemap:
                self.queue.add_url(sitemap_url, priority=0, depth=0)

    async def crawl(self) -> list:
        await self._get_session()
        self.stats.start()

        try:
            source_domains = {get_domain(url) for url in self.start_urls}

            for url in self.start_urls:
                self.queue.add_url(url, priority=0, depth=0)

            await self._enqueue_sitemap_urls()

            while len(self.processed_urls) < self.max_pages:
                queue_stats = self.queue.get_stats()

                if queue_stats["queued"] == 0:
                    break

                batch = []
                batch_size = min(
                    self.max_concurrent,
                    queue_stats["queued"],
                    self.max_pages - len(self.processed_urls)
                )

                for _ in range(batch_size):
                    item = await self.queue.get_next()
                    if item is None:
                        break

                    if item.depth > self.max_depth:
                        continue

                    batch.append(self._process_queue_item(item, source_domains))

                if not batch:
                    break

                await asyncio.gather(*batch)

                speed = calculate_speed(self.stats.started_at if self.stats.started_at else time.perf_counter(), len(self.processed_urls))
                queue_stats = self.queue.get_stats()
                semaphore_stats = self.semaphore_manager.get_stats()
                request_rate = self._get_request_rate()
                average_delay = self._get_average_delay()
                eta = self._estimate_remaining_time()

                print_crawl_progress(
                    processed=len(self.processed_urls),
                    queued=queue_stats["queued"],
                    failed=len(self.failed_urls),
                    active=semaphore_stats["active_tasks"],
                    speed=speed,
                    request_rate=request_rate,
                    avg_delay=average_delay,
                    robots_blocked=self.robots_blocked_count
                )
                print(f" | ETA: {eta:.2f} сек", end="", flush=True)

            print()
            return list(self.processed_urls.values())

        finally:
            self.stats.finish()

    def get_stats(self) -> dict:
        return self.stats.to_dict()

    async def export_to_json(self, filename: str) -> None:
        await self.stats.export_to_json(filename)

    async def export_to_html_report(self, filename: str) -> None:
        await self.stats.export_to_html_report(filename)

    async def close(self) -> None:
        if self.storage is not None:
            try:
                await self.storage.close()
            except Exception as error:
                logger.error(f"💾 Ошибка закрытия storage: {type(error).__name__}: {error}")

        if self.session is not None and not self.session.closed:
            await self.session.close()
            logger.info("🔒 HTTP-сессия закрыта")


def build_storage_from_output(output: Optional[str]):
    if output is None:
        return None

    output_lower = output.lower()

    if output_lower.endswith(".json"):
        return JSONStorage(output)

    if output_lower.endswith(".csv"):
        return CSVStorage(output)

    if output_lower.endswith(".db") or output_lower.endswith(".sqlite"):
        return SQLiteStorage(output)

    return JSONStorage(output)


def parse_args():
    parser = argparse.ArgumentParser(description="Advanced async web crawler")

    parser.add_argument("--urls", nargs="+", help="Стартовые URL")
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--output", type=str, default="results.json")
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--respect-robots", action="store_true")
    parser.add_argument("--rate-limit", type=float, default=1.0)

    return parser.parse_args()


async def cli_main():
    args = parse_args()

    if args.config:
        crawler = AdvancedCrawler.from_config(args.config)
    else:
        storage = build_storage_from_output(args.output)
        crawler = AdvancedCrawler(
            start_urls=args.urls or ["https://example.com"],
            max_pages=args.max_pages,
            max_depth=args.max_depth,
            requests_per_second=args.rate_limit,
            respect_robots=args.respect_robots,
            storage=storage
        )

    try:
        await crawler.crawl()

        stats = crawler.get_stats()
        print(f"Обработано: {stats['total_pages']} страниц")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")

        await crawler.export_to_json("stats.json")
        await crawler.export_to_html_report("report.html")
        await save_dict_json_async(stats, "stats_backup.json")

    finally:
        await crawler.close()

AsyncCrawler = AdvancedCrawler

if __name__ == "__main__":
    asyncio.run(cli_main())