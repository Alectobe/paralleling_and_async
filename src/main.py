import asyncio
import time
from typing import Optional

import aiohttp

from src.models import FetchResult
from src.parser import HTMLParser
from src.utils import print_parsed_summary, setup_logger, save_json_async


logger = setup_logger()


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10) -> None:
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: Optional[aiohttp.ClientSession] = None
        self.parser = HTMLParser()

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
                headers={"User-Agent": "AsyncCrawler/2.0"}
            )

        return self.session

    async def fetch_url(self, url: str) -> FetchResult:
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

    async def fetch_urls(self, urls: list[str]) -> list[FetchResult]:
        tasks = [self.fetch_url(url) for url in urls]
        return await asyncio.gather(*tasks)

    async def fetch_and_parse(self, url: str) -> dict:
        """
        Загружает страницу и сразу парсит HTML.
        """
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

    async def fetch_and_parse_many(self, urls: list[str]) -> list[dict]:
        tasks = [self.fetch_and_parse(url) for url in urls]
        return await asyncio.gather(*tasks)

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
            logger.info("🔒 HTTP-сессия закрыта")


async def main() -> None:
    urls = [
        "https://example.com",
        "https://httpbin.org/html",
        "https://www.python.org",
    ]

    crawler = AsyncCrawler(max_concurrent=5)

    try:
        start = time.perf_counter()
        parsed_pages = await crawler.fetch_and_parse_many(urls)
        total_time = time.perf_counter() - start

        print_parsed_summary(parsed_pages)
        await save_json_async(parsed_pages, "parsed_results.json")

        print(f"\n⏱️ Общее время: {total_time:.2f} сек")
        print("💾 Результаты сохранены в parsed_results.json")

    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())