from typing import Optional


class CrawlerError(Exception):
    def __init__(self, message: str, url: Optional[str] = None) -> None:
        super().__init__(message)
        self.url = url
        self.message = message


class TransientError(CrawlerError):
    pass


class PermanentError(CrawlerError):
    pass


class NetworkError(CrawlerError):
    pass


class ParseError(CrawlerError):
    pass


class RobotsBlockedError(PermanentError):
    pass


class CircuitBreakerOpenError(TransientError):
    pass