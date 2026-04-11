import abc
import asyncio
import csv
import io
import json
from typing import Optional

import aiofiles
import aiosqlite

from src.utils import setup_logger


logger = setup_logger()


class DataStorage(abc.ABC):
    @abc.abstractmethod
    async def save(self, data: dict) -> None:
        pass

    @abc.abstractmethod
    async def close(self) -> None:
        pass


class JSONStorage(DataStorage):
    def __init__(self, file_path: str, encoding: str = "utf-8") -> None:
        self.file_path = file_path
        self.encoding = encoding
        self._initialized = False
        self._first_item = True
        self._lock = asyncio.Lock()

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return

        async with aiofiles.open(self.file_path, "w", encoding=self.encoding) as file:
            await file.write("[\n")

        self._initialized = True
        self._first_item = True

    async def save(self, data: dict) -> None:
        async with self._lock:
            await self._ensure_initialized()

            json_text = json.dumps(data, ensure_ascii=False, indent=4)

            async with aiofiles.open(self.file_path, "a", encoding=self.encoding) as file:
                if not self._first_item:
                    await file.write(",\n")
                await file.write(json_text)
                self._first_item = False

    async def close(self) -> None:
        async with self._lock:
            if not self._initialized:
                return

            async with aiofiles.open(self.file_path, "a", encoding=self.encoding) as file:
                await file.write("\n]\n")


class CSVStorage(DataStorage):
    def __init__(self, file_path: str, encoding: str = "utf-8") -> None:
        self.file_path = file_path
        self.encoding = encoding
        self._initialized = False
        self._headers = None
        self._lock = asyncio.Lock()

    def _flatten_data(self, data: dict) -> dict:
        flattened = {}

        for key, value in data.items():
            if isinstance(value, (list, dict)):
                flattened[key] = json.dumps(value, ensure_ascii=False)
            else:
                flattened[key] = value

        return flattened

    async def _ensure_initialized(self, data: dict) -> None:
        if self._initialized:
            return

        flattened = self._flatten_data(data)
        self._headers = list(flattened.keys())

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=self._headers)
        writer.writeheader()

        async with aiofiles.open(self.file_path, "w", encoding=self.encoding, newline="") as file:
            await file.write(buffer.getvalue())

        self._initialized = True

    async def save(self, data: dict) -> None:
        async with self._lock:
            await self._ensure_initialized(data)

            flattened = self._flatten_data(data)

            for header in self._headers:
                if header not in flattened:
                    flattened[header] = ""

            row_buffer = io.StringIO()
            writer = csv.DictWriter(row_buffer, fieldnames=self._headers)
            writer.writerow(flattened)

            async with aiofiles.open(self.file_path, "a", encoding=self.encoding, newline="") as file:
                await file.write(row_buffer.getvalue())

    async def close(self) -> None:
        return


class SQLiteStorage(DataStorage):
    def __init__(self, db_path: str, batch_size: int = 10) -> None:
        self.db_path = db_path
        self.batch_size = batch_size
        self.connection: Optional[aiosqlite.Connection] = None
        self.buffer = []
        self._initialized = False
        self._lock = asyncio.Lock()

    async def init_db(self) -> None:
        if self._initialized:
            return

        self.connection = await aiosqlite.connect(self.db_path)
        await self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS crawled_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                title TEXT,
                text TEXT,
                links TEXT,
                metadata TEXT,
                crawled_at TEXT,
                status_code INTEGER,
                content_type TEXT
            )
            """
        )

        await self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_crawled_pages_url ON crawled_pages(url)"
        )
        await self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_crawled_pages_status_code ON crawled_pages(status_code)"
        )

        await self.connection.commit()
        self._initialized = True

    def _serialize_row(self, data: dict) -> tuple:
        return (
            data.get("url", ""),
            data.get("title", ""),
            data.get("text", ""),
            json.dumps(data.get("links", []), ensure_ascii=False),
            json.dumps(data.get("metadata", {}), ensure_ascii=False),
            data.get("crawled_at", ""),
            data.get("status_code"),
            data.get("content_type", ""),
        )

    async def _flush(self) -> None:
        if not self.buffer:
            return

        await self.init_db()

        await self.connection.executemany(
            """
            INSERT OR REPLACE INTO crawled_pages (
                url, title, text, links, metadata, crawled_at, status_code, content_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self.buffer
        )

        await self.connection.commit()
        self.buffer.clear()

    async def save(self, data: dict) -> None:
        async with self._lock:
            await self.init_db()

            row = self._serialize_row(data)
            self.buffer.append(row)

            if len(self.buffer) >= self.batch_size:
                await self._flush()

    async def read_all(self) -> list:
        await self.init_db()
        await self._flush()

        cursor = await self.connection.execute(
            """
            SELECT url, title, text, links, metadata, crawled_at, status_code, content_type
            FROM crawled_pages
            ORDER BY id
            """
        )
        rows = await cursor.fetchall()

        result = []
        for row in rows:
            result.append(
                {
                    "url": row[0],
                    "title": row[1],
                    "text": row[2],
                    "links": json.loads(row[3]) if row[3] else [],
                    "metadata": json.loads(row[4]) if row[4] else {},
                    "crawled_at": row[5],
                    "status_code": row[6],
                    "content_type": row[7],
                }
            )

        return result

    async def close(self) -> None:
        async with self._lock:
            if self.connection is None:
                return

            await self._flush()
            await self.connection.close()
            self.connection = None