import json
import logging
from typing import Iterable

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


def print_parsed_summary(parsed_pages: list[dict]) -> None:
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


async def save_json_async(data: list[dict], file_path: str) -> None:
    async with aiofiles.open(file_path, "w", encoding="utf-8") as file:
        json_text = json.dumps(data, ensure_ascii=False, indent=4)
        await file.write(json_text)