from __future__ import annotations
import functools, logging
from typing import Callable, TypeVar
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential, before_sleep_log

log = logging.getLogger(__name__)
F = TypeVar("F", bound=Callable)
_RETRYABLE = (requests.Timeout, requests.ConnectionError, requests.HTTPError, OSError)

def with_retry(max_attempts: int = 3, backoff_base: float = 2.0) -> Callable[[F], F]:
    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        @retry(stop=stop_after_attempt(max_attempts), wait=wait_exponential(multiplier=backoff_base, min=1, max=60),
               retry=retry_if_exception_type(_RETRYABLE), before_sleep=before_sleep_log(log, logging.WARNING), reraise=True)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        return wrapper
    return decorator
