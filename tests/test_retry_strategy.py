import time

import pytest

from src.errors import NetworkError, PermanentError, TransientError
from src.retry_strategy import RetryStrategy


@pytest.mark.asyncio
async def test_retry_on_transient_error_then_success():
    strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=2.0,
        retry_on=[TransientError, NetworkError],
        base_delay=0.01
    )

    state = {"count": 0}

    async def flaky():
        state["count"] += 1
        if state["count"] < 3:
            raise TransientError("temporary")
        return "ok"

    result = await strategy.execute_with_retry(flaky)

    assert result == "ok"
    assert state["count"] == 3
    assert strategy.get_stats()["successful_retries"] == 1


@pytest.mark.asyncio
async def test_no_retry_on_permanent_error():
    strategy = RetryStrategy(
        max_retries=3,
        retry_on=[TransientError, NetworkError],
        base_delay=0.01
    )

    state = {"count": 0}

    async def always_404():
        state["count"] += 1
        raise PermanentError("404")

    with pytest.raises(PermanentError):
        await strategy.execute_with_retry(always_404)

    assert state["count"] == 1


@pytest.mark.asyncio
async def test_exponential_backoff():
    strategy = RetryStrategy(
        max_retries=2,
        backoff_factor=2.0,
        retry_on=[TransientError],
        base_delay=0.05
    )

    state = {"count": 0}

    async def always_fail():
        state["count"] += 1
        raise TransientError("503")

    start = time.perf_counter()

    with pytest.raises(TransientError):
        await strategy.execute_with_retry(always_fail)

    elapsed = time.perf_counter() - start

    assert state["count"] == 3
    assert elapsed >= 0.14


@pytest.mark.asyncio
async def test_retry_stats_count_errors():
    strategy = RetryStrategy(
        max_retries=1,
        retry_on=[TransientError],
        base_delay=0.01
    )

    async def always_fail():
        raise TransientError("timeout")

    with pytest.raises(TransientError):
        await strategy.execute_with_retry(always_fail)

    stats = strategy.get_stats()
    assert stats["total_retries"] == 1
    assert stats["errors_by_type"]["TransientError"] >= 1