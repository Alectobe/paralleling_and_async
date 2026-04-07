import json
import time
from collections import Counter
from typing import Optional
from urllib.parse import urlparse

import aiofiles


class CrawlerStats:
    def __init__(self) -> None:
        self.started_at = None
        self.finished_at = None

        self.total_pages = 0
        self.successful = 0
        self.failed = 0

        self.status_codes = Counter()
        self.domains = Counter()

        self.request_attempts = 0
        self.storage_saved = 0
        self.storage_errors = 0

    def start(self) -> None:
        self.started_at = time.perf_counter()

    def finish(self) -> None:
        self.finished_at = time.perf_counter()

    def record_page(self, url: str, success: bool, status_code: Optional[int] = None) -> None:
        self.total_pages += 1

        if success:
            self.successful += 1
        else:
            self.failed += 1

        domain = urlparse(url).netloc.lower()
        if domain:
            self.domains[domain] += 1

        if status_code is not None:
            self.status_codes[str(status_code)] += 1

    def record_request_attempt(self) -> None:
        self.request_attempts += 1

    def record_storage_saved(self) -> None:
        self.storage_saved += 1

    def record_storage_error(self) -> None:
        self.storage_errors += 1

    def get_elapsed_time(self) -> float:
        if self.started_at is None:
            return 0.0

        end_time = self.finished_at if self.finished_at is not None else time.perf_counter()
        return max(0.0, end_time - self.started_at)

    def get_average_speed(self) -> float:
        elapsed = self.get_elapsed_time()
        if elapsed <= 0:
            return 0.0
        return self.total_pages / elapsed

    def get_top_domains(self, top_n: int = 10) -> list:
        return [
            {"domain": domain, "count": count}
            for domain, count in self.domains.most_common(top_n)
        ]

    def to_dict(self) -> dict:
        return {
            "total_pages": self.total_pages,
            "successful": self.successful,
            "failed": self.failed,
            "average_speed_pages_per_sec": self.get_average_speed(),
            "request_attempts": self.request_attempts,
            "storage_saved": self.storage_saved,
            "storage_errors": self.storage_errors,
            "status_codes": dict(self.status_codes),
            "top_domains": self.get_top_domains(),
            "elapsed_time_sec": self.get_elapsed_time(),
        }

    async def export_to_json(self, filename: str) -> None:
        async with aiofiles.open(filename, "w", encoding="utf-8") as file:
            await file.write(json.dumps(self.to_dict(), ensure_ascii=False, indent=4))

    async def export_to_html_report(self, filename: str) -> None:
        data = self.to_dict()

        status_rows = "".join(
            f"<tr><td>{code}</td><td>{count}</td></tr>"
            for code, count in data["status_codes"].items()
        )

        domain_rows = "".join(
            f"<tr><td>{item['domain']}</td><td>{item['count']}</td></tr>"
            for item in data["top_domains"]
        )

        html = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Crawler Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 24px;
            background: #f8f9fb;
            color: #222;
        }}
        h1, h2 {{
            color: #1f3b5b;
        }}
        .card {{
            background: white;
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background: white;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
        }}
        th {{
            background: #eef3f8;
        }}
    </style>
</head>
<body>
    <h1>Отчёт по работе краулера</h1>

    <div class="card">
        <p><strong>Всего страниц:</strong> {data["total_pages"]}</p>
        <p><strong>Успешно:</strong> {data["successful"]}</p>
        <p><strong>Ошибок:</strong> {data["failed"]}</p>
        <p><strong>Средняя скорость:</strong> {data["average_speed_pages_per_sec"]:.2f} стр/сек</p>
        <p><strong>Попыток запросов:</strong> {data["request_attempts"]}</p>
        <p><strong>Успешных сохранений:</strong> {data["storage_saved"]}</p>
        <p><strong>Ошибок сохранения:</strong> {data["storage_errors"]}</p>
        <p><strong>Время работы:</strong> {data["elapsed_time_sec"]:.2f} сек</p>
    </div>

    <div class="card">
        <h2>Распределение по статус-кодам</h2>
        <table>
            <tr><th>Статус</th><th>Количество</th></tr>
            {status_rows}
        </table>
    </div>

    <div class="card">
        <h2>Топ доменов</h2>
        <table>
            <tr><th>Домен</th><th>Количество страниц</th></tr>
            {domain_rows}
        </table>
    </div>
</body>
</html>
"""
        async with aiofiles.open(filename, "w", encoding="utf-8") as file:
            await file.write(html)