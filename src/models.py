from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FetchResult:
    url: str
    content: Optional[str]
    success: bool
    error: Optional[str] = None
    status_code: Optional[int] = None


@dataclass
class ParsedPage:
    url: str
    title: str = ""
    text: str = ""
    links: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    images: list[dict] = field(default_factory=list)
    headings: dict = field(default_factory=lambda: {
        "h1": [],
        "h2": [],
        "h3": []
    })
    tables: list[list[list[str]]] = field(default_factory=list)
    lists: list[dict] = field(default_factory=list)
    error: Optional[str] = None


@dataclass(order=True)
class QueueItem:
    priority: int
    depth: int
    url: str = field(compare=False)


@dataclass
class CrawlStats:
    processed: int = 0
    failed: int = 0
    queued: int = 0
    active: int = 0