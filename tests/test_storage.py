import json
from pathlib import Path

import pytest

from src.storage import CSVStorage, JSONStorage, SQLiteStorage


@pytest.mark.asyncio
async def test_json_storage(tmp_path: Path):
    file_path = tmp_path / "results.json"
    storage = JSONStorage(str(file_path))

    try:
        await storage.save(
            {
                "url": "https://example.com",
                "title": "Example",
                "text": "Hello",
                "links": ["https://example.com/about"],
                "metadata": {"description": "test"},
                "crawled_at": "2026-01-01T00:00:00+00:00",
                "status_code": 200,
                "content_type": "text/html",
            }
        )
    finally:
        await storage.close()

    text = file_path.read_text(encoding="utf-8")
    data = json.loads(text)

    assert len(data) == 1
    assert data[0]["url"] == "https://example.com"


@pytest.mark.asyncio
async def test_csv_storage(tmp_path: Path):
    file_path = tmp_path / "results.csv"
    storage = CSVStorage(str(file_path))

    try:
        await storage.save(
            {
                "url": "https://example.com",
                "title": "Example",
                "text": "Hello",
                "links": ["https://example.com/about"],
                "metadata": {"description": "test"},
                "crawled_at": "2026-01-01T00:00:00+00:00",
                "status_code": 200,
                "content_type": "text/html",
            }
        )
    finally:
        await storage.close()

    text = file_path.read_text(encoding="utf-8")
    assert "url" in text
    assert "https://example.com" in text
    assert "Example" in text


@pytest.mark.asyncio
async def test_sqlite_storage(tmp_path: Path):
    db_path = tmp_path / "crawler.db"
    storage = SQLiteStorage(str(db_path), batch_size=2)

    try:
        await storage.save(
            {
                "url": "https://example.com",
                "title": "Example",
                "text": "Hello",
                "links": ["https://example.com/about"],
                "metadata": {"description": "test"},
                "crawled_at": "2026-01-01T00:00:00+00:00",
                "status_code": 200,
                "content_type": "text/html",
            }
        )
        await storage.save(
            {
                "url": "https://example.com/about",
                "title": "About",
                "text": "About page",
                "links": [],
                "metadata": {},
                "crawled_at": "2026-01-01T00:00:00+00:00",
                "status_code": 200,
                "content_type": "text/html",
            }
        )

        rows = await storage.read_all()
        assert len(rows) == 2
        assert rows[0]["url"] == "https://example.com"
    finally:
        await storage.close()