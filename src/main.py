import asyncio
import random
import time
from typing import Optional
from urllib.parse import urlparse

import aiohttp

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
from src.utils import (
    calculate_speed,
    get_domain,
    normalize_url,
    print_crawl_progress,
    save_dict_json_async,
    save_json_async,
    setup_logger,
)


logger = setup_logger()


class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        per_domain_limit: int = 3,
        requests_per_second: float = 1.0,
        respect_robots: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        user_agent: str = "AsyncCrawler/5.0",
        user_agents: Optional[list] = None,
        max_retries: int = 3,
        backoff_base: float = 0.5,
        timeout_connect: float = 5.0,
        timeout_read: float = 15.0,
        timeout_total: float = 20.0,
        circuit_breaker_enabled: bool = True,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 30.0
    ) -> None:
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

        self.request_timestamps = []
        self.delay_history = []
        self.robots_blocked_count = 0
        self.total_request_attempts = 0
        self.permanent_error_urls = {}

        self.domain_error_counts = {}
        self.domain_open_until = {}

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

        return self.session

    def _get_average_delay(self) -> float:
        if not self.delay_history:
            return 0.0
        return sum(self.delay_history) / len(self.delay_history)

    def _get_request_rate(self, start_time: float) -> float:
        elapsed = time.perf_counter() - start_time
        if elapsed <= 0:
            return 0.0
        return self.total_request_attempts / elapsed

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
            return await self.retry_strategy.execute_with_retry(wrapped_fetch)

        except PermanentError as error:
            logger.error(f"❌ Постоянная ошибка | url={url} | error={error}")
            self.permanent_error_urls[url] = str(error)
            return FetchResult(
                url=url,
                content=None,
                success=False,
                error=str(error)
            )

        except RobotsBlockedError as error:
            logger.error(f"❌ robots blocked | url={url} | error={error}")
            self.permanent_error_urls[url] = str(error)
            return FetchResult(
                url=url,
                content=None,
                success=False,
                error=str(error)
            )

        except ParseError as error:
            logger.error(f"❌ Ошибка парсинга | url={url} | error={error}")
            return FetchResult(
                url=url,
                content=None,
                success=False,
                error=str(error)
            )

        except Exception as error:
            logger.error(f"❌ Временная/сетевая ошибка после всех повторов | url={url} | error={error}")
            return FetchResult(
                url=url,
                content=None,
                success=False,
                error=str(error)
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

        try:
            return await self.parser.parse_html(fetch_result.content, url)
        except Exception as error:
            raise ParseError(f"{type(error).__name__}: {error}", url=url)

    def _should_visit_url(
        self,
        url: str,
        source_domain: str,
        same_domain_only: bool,
        include_patterns: Optional[list],
        exclude_patterns: Optional[list]
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
        source_domains: set,
        same_domain_only: bool,
        include_patterns: Optional[list],
        exclude_patterns: Optional[list]
    ) -> None:
        url = item.url
        depth = item.depth

        if len(self.processed_urls) >= max_pages:
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

    async def crawl(
        self,
        start_urls: list,
        max_pages: int = 100,
        same_domain_only: bool = True,
        include_patterns: Optional[list] = None,
        exclude_patterns: Optional[list] = None
    ) -> list:
        start_time = time.perf_counter()
        await self._get_session()

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
            request_rate = self._get_request_rate(start_time)
            average_delay = self._get_average_delay()

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

        print()
        return list(self.processed_urls.values())

    def get_error_stats(self) -> dict:
        retry_stats = self.retry_strategy.get_stats()

        return {
            "total_request_attempts": self.total_request_attempts,
            "failed_urls_count": len(self.failed_urls),
            "permanent_error_urls_count": len(self.permanent_error_urls),
            "permanent_error_urls": dict(self.permanent_error_urls),
            "failed_urls": dict(self.failed_urls),
            "retry_stats": retry_stats,
            "domain_error_counts": dict(self.domain_error_counts),
            "open_circuits": {
                domain: until
                for domain, until in self.domain_open_until.items()
            },
        }

    def get_politeness_stats(self) -> dict:
        return {
            "requests_per_second_config": self.requests_per_second,
            "average_delay": self._get_average_delay(),
            "robots_blocked_count": self.robots_blocked_count,
            "total_request_attempts": self.total_request_attempts,
            "rate_limiter": self.rate_limiter.get_stats(),
            "robots_parser": self.robots_parser.get_stats(),
        }

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
            logger.info("🔒 HTTP-сессия закрыта")


async def main() -> None:
    crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=1,
        per_domain_limit=2,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.3,
        jitter=0.1,
        user_agent="MyBot/1.0",
        max_retries=3,
        backoff_base=0.5,
        timeout_connect=3.0,
        timeout_read=5.0,
        timeout_total=8.0,
        circuit_breaker_enabled=True,
        circuit_breaker_threshold=3,
        circuit_breaker_timeout=15.0
    )

    try:
        results = await crawler.crawl(
            start_urls=[
                "https://example.com",
                "https://httpbin.org/status/503",
                "https://httpbin.org/status/404",
                "https://httpbin.org/status/429",
            ],
            max_pages=10,
            same_domain_only=False,
            include_patterns=None,
            exclude_patterns=["#", "mailto:", "tel:"]
        )

        await save_json_async(results, "crawl_results_day5.json")
        await save_dict_json_async(crawler.get_error_stats(), "error_report_day5.json")

        print(f"Обработано: {len(results)} страниц")
        print("Результаты сохранены в crawl_results_day5.json")
        print("Отчёт об ошибках сохранён в error_report_day5.json")
        print("Статистика ошибок:")
        print(crawler.get_error_stats())

    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())