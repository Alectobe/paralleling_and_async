import time

import pytest

from src.rate_limiter import RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_same_domain():
    limiter = RateLimiter(requests_per_second=2.0, per_domain=True)

    start = time.perf_counter()
    await limiter.acquire("example.com")
    await limiter.acquire("example.com")
    elapsed = time.perf_counter() - start

    assert elapsed >= 0.45


@pytest.mark.asyncio
async def test_rate_limiter_different_domains():
    limiter = RateLimiter(requests_per_second=2.0, per_domain=True)

    start = time.perf_counter()
    await limiter.acquire("example.com")
    await limiter.acquire("other.com")
    elapsed = time.perf_counter() - start

    assert elapsed < 0.45


@pytest.mark.asyncio
async def test_rate_limiter_global_limit():
    limiter = RateLimiter(requests_per_second=2.0, per_domain=False)

    start = time.perf_counter()
    await limiter.acquire("example.com")
    await limiter.acquire("other.com")
    elapsed = time.perf_counter() - start

    assert elapsed >= 0.45