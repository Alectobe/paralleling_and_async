import asyncio
import time
from typing import Optional

import aiohttp

from src.models import FetchResult
from src.utils import print_results, setup_logger


logger = setup_logger()


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10) -> None:
        """
        Инициализация краулера.

        :param max_concurrent: максимальное количество одновременных запросов
        """
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """
        Создаёт HTTP-сессию при первом обращении.
        """
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
                headers={"User-Agent": "AsyncCrawler/1.0"}
            )

        return self.session

    async def fetch_url(self, url: str) -> FetchResult:
        """
        Загружает одну страницу.

        :param url: URL страницы
        :return: объект FetchResult
        """
        async with self.semaphore:
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

            except aiohttp.ClientResponseError as e:
                logger.error(
                    f"🚫 HTTP ошибка для {url} | status={e.status} | message={e.message}"
                )
                return FetchResult(
                    url=url,
                    content=None,
                    success=False,
                    error=f"HTTP error {e.status}: {e.message}",
                    status_code=e.status
                )

            except asyncio.TimeoutError:
                logger.error(f"⏰ Таймаут при загрузке: {url}")
                return FetchResult(
                    url=url,
                    content=None,
                    success=False,
                    error="Timeout error"
                )

            except aiohttp.ClientError as e:
                logger.error(f"❌ Сетевая ошибка для {url} | {type(e).__name__}: {e}")
                return FetchResult(
                    url=url,
                    content=None,
                    success=False,
                    error=f"Client error: {type(e).__name__}: {e}"
                )

            except Exception as e:
                logger.error(f"⚠️ Неожиданная ошибка для {url} | {type(e).__name__}: {e}")
                return FetchResult(
                    url=url,
                    content=None,
                    success=False,
                    error=f"Unexpected error: {type(e).__name__}: {e}"
                )

    async def fetch_urls(self, urls: list[str]) -> list[FetchResult]:
        """
        Параллельно загружает список URL.

        :param urls: список URL
        :return: список результатов
        """
        tasks = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results

    async def close(self) -> None:
        """
        Закрывает HTTP-сессию.
        """
        if self.session is not None and not self.session.closed:
            await self.session.close()
            logger.info("🔒 HTTP-сессия закрыта")


async def fetch_urls_sequential(urls: list[str]) -> list[FetchResult]:
    """
    Последовательная загрузка URL для сравнения.
    """
    crawler = AsyncCrawler(max_concurrent=1)
    results: list[FetchResult] = []

    try:
        for url in urls:
            result = await crawler.fetch_url(url)
            results.append(result)
    finally:
        await crawler.close()

    return results


async def fetch_urls_parallel(urls: list[str], max_concurrent: int = 5) -> list[FetchResult]:
    """
    Параллельная загрузка URL.
    """
    crawler = AsyncCrawler(max_concurrent=max_concurrent)

    try:
        results = await crawler.fetch_urls(urls)
        return results
    finally:
        await crawler.close()


async def main() -> None:
    urls = [
        "https://example.com",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/html",
        "https://httpbin.org/status/404",
        "https://this-domain-does-not-exist-123456789.com"
    ]

    print("=== Последовательная загрузка ===")
    start = time.perf_counter()
    sequential_results = await fetch_urls_sequential(urls)
    sequential_time = time.perf_counter() - start
    print_results(sequential_results)
    print(f"\nВремя последовательной загрузки: {sequential_time:.2f} сек")

    print("\n" + "=" * 60 + "\n")

    print("=== Параллельная загрузка ===")
    start = time.perf_counter()
    parallel_results = await fetch_urls_parallel(urls, max_concurrent=5)
    parallel_time = time.perf_counter() - start
    print_results(parallel_results)
    print(f"\nВремя параллельной загрузки: {parallel_time:.2f} сек")

    if parallel_time > 0:
        print(f"\nУскорение: {sequential_time / parallel_time:.2f}x")


if __name__ == "__main__":
    asyncio.run(main())