import logging
from typing import Iterable

from src.models import FetchResult


def setup_logger() -> logging.Logger:
    """
    Настраивает и возвращает логгер.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    return logging.getLogger("async_crawler")


def print_results(results: Iterable[FetchResult]) -> None:
    """
    Печатает результаты загрузки в удобном виде.
    """
    print("\nРезультаты:")
    for result in results:
        status = "OK" if result.success else "ERROR"
        size = len(result.content) if result.content is not None else 0
        print(f"{status:>5} | {size:>6} bytes | {result.url}")

        if result.error:
            print(f"      Ошибка: {result.error}")