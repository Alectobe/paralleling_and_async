import json
import logging
import time
from typing import Iterable
from urllib.parse import urlparse

import aiofiles

from src.models import FetchResult


def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    return logging.getLogger("async_crawler")


def print_results(results: Iterable[FetchResult]) -> None:
    print("\nРезультаты загрузки:")
    for result in results:
        status = "OK" if result.success else "ERROR"
        size = len(result.content) if result.content is not None else 0
        print(f"{status:>5} | {size:>6} bytes | {result.url}")

        if result.error:
            print(f"      Ошибка: {result.error}")


def print_parsed_summary(parsed_pages: list) -> None:
    print("\nРезультаты парсинга:")
    for page in parsed_pages:
        print(
            f"URL: {page['url']}\n"
            f"  title: {page['title']}\n"
            f"  text_length: {len(page['text'])}\n"
            f"  links_count: {len(page['links'])}\n"
            f"  images_count: {len(page['images'])}\n"
            f"  h1_count: {len(page['headings']['h1'])}\n"
        )


async def save_json_async(data: list, file_path: str) -> None:
    async with aiofiles.open(file_path, "w", encoding="utf-8") as file:
        json_text = json.dumps(data, ensure_ascii=False, indent=4)
        await file.write(json_text)


async def save_dict_json_async(data: dict, file_path: str) -> None:
    async with aiofiles.open(file_path, "w", encoding="utf-8") as file:
        json_text = json.dumps(data, ensure_ascii=False, indent=4)
        await file.write(json_text)


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    normalized = f"{parsed.scheme}://{parsed.netloc}{path}"

    if parsed.query:
        normalized += f"?{parsed.query}"

    return normalized


def get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def calculate_speed(start_time: float, processed_count: int) -> float:
    elapsed = time.perf_counter() - start_time
    if elapsed <= 0:
        return 0.0
    return processed_count / elapsed


def print_crawl_progress(
    processed: int,
    queued: int,
    failed: int,
    active: int,
    speed: float,
    request_rate: float,
    avg_delay: float,
    robots_blocked: int
) -> None:
    print(
        f"\rОбработано: {processed} | "
        f"В очереди: {queued} | "
        f"Ошибок: {failed} | "
        f"Активных: {active} | "
        f"Скорость страниц: {speed:.2f} стр/сек | "
        f"Скорость запросов: {request_rate:.2f} req/сек | "
        f"Средняя задержка: {avg_delay:.2f} сек | "
        f"robots blocked: {robots_blocked}",
        end="",
        flush=True
    )