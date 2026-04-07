import asyncio
import time
from typing import Awaitable, Callable, Optional

from src.errors import NetworkError, TransientError
from src.utils import setup_logger


logger = setup_logger()


class RetryStrategy:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: Optional[list] = None,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
        type_limits: Optional[dict] = None,
        type_backoff: Optional[dict] = None
    ) -> None:
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = retry_on or [TransientError, NetworkError]
        self.base_delay = base_delay
        self.max_delay = max_delay

        self.type_limits = type_limits or {
            "TransientError": max_retries,
            "NetworkError": max_retries,
            "PermanentError": 0,
            "ParseError": 0,
            "RobotsBlockedError": 0,
            "CircuitBreakerOpenError": max_retries,
        }

        self.type_backoff = type_backoff or {
            "TransientError": backoff_factor,
            "NetworkError": backoff_factor,
            "CircuitBreakerOpenError": backoff_factor,
        }

        self.total_retries = 0
        self.successful_retries = 0
        self.total_retry_delay = 0.0
        self.errors_by_type = {}

    def _should_retry(self, error: Exception) -> bool:
        return any(isinstance(error, error_type) for error_type in self.retry_on)

    def _get_type_name(self, error: Exception) -> str:
        return type(error).__name__

    def _get_max_retries_for_error(self, error: Exception) -> int:
        return self.type_limits.get(self._get_type_name(error), self.max_retries)

    def _get_backoff_for_error(self, error: Exception) -> float:
        return self.type_backoff.get(self._get_type_name(error), self.backoff_factor)

    def _calculate_delay(self, attempt_number: int, error: Exception) -> float:
        factor = self._get_backoff_for_error(error)
        delay = self.base_delay * (factor ** (attempt_number - 1))
        return min(delay, self.max_delay)

    async def execute_with_retry(
        self,
        coro: Callable[..., Awaitable],
        *args,
        **kwargs
    ):
        last_error = None
        success_after_retry = False

        for attempt in range(1, self.max_retries + 2):
            try:
                result = await coro(*args, **kwargs)

                if attempt > 1:
                    self.successful_retries += 1

                return result

            except Exception as error:
                last_error = error
                error_name = self._get_type_name(error)
                self.errors_by_type[error_name] = self.errors_by_type.get(error_name, 0) + 1

                should_retry = self._should_retry(error)
                allowed_retries = self._get_max_retries_for_error(error)

                if not should_retry:
                    logger.error(
                        f"🚫 Повтор запрещён | type={error_name} | attempt={attempt} | error={error}"
                    )
                    raise

                if attempt > allowed_retries:
                    logger.error(
                        f"🚫 Лимит повторов исчерпан | type={error_name} | attempt={attempt} | error={error}"
                    )
                    raise

                delay = self._calculate_delay(attempt, error)
                self.total_retries += 1
                self.total_retry_delay += delay

                logger.warning(
                    f"🔄 Повтор запроса | type={error_name} | attempt={attempt} | next_retry_in={delay:.2f}s | error={error}"
                )

                await asyncio.sleep(delay)

        if last_error is not None:
            raise last_error

    def get_stats(self) -> dict:
        average_retry_delay = 0.0
        if self.total_retries > 0:
            average_retry_delay = self.total_retry_delay / self.total_retries

        return {
            "total_retries": self.total_retries,
            "successful_retries": self.successful_retries,
            "average_retry_delay": average_retry_delay,
            "errors_by_type": dict(self.errors_by_type),
        }