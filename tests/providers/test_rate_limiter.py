import time

from providers.news.rate_limiter import RateLimiter


def test_can_call_initially():
    limiter = RateLimiter()
    assert limiter.can_call("test_source", 10)


def test_cannot_call_immediately_after():
    limiter = RateLimiter()
    limiter.wait_if_needed("test_source", 1)
    assert not limiter.can_call("test_source", 60)


def test_can_call_after_interval():
    limiter = RateLimiter()
    limiter.wait_if_needed("test_source", 0)
    time.sleep(0.1)
    assert limiter.can_call("test_source", 0)
