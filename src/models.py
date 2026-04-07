from dataclasses import dataclass
from typing import Optional


@dataclass
class FetchResult:
    url: str
    content: Optional[str]
    success: bool
    error: Optional[str] = None
    status_code: Optional[int] = None