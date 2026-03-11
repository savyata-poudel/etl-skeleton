from __future__ import annotations
import logging, time
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

class ApiClient:
    def __init__(self, base_url: str, token: str, rate_limit_per_sec: float = 5.0):
        self.base = base_url.strip("/")
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": token})
        self.min_interval = 1.0 / rate_limit_per_sec
        self._last: float = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.time()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30),
           retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError, requests.HTTPError)),
           reraise=True)
    def get(self, path: str, params: dict) -> dict:
        self._throttle()
        url = f"{self.base}/{path.lstrip('/')}"
        log.info("GET %s params=%s", url, params)
        r = self.session.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def paginate(self, endpoint: str, page_param: str = "page", per_page_param: str = "per_page",
                 start_page: int = 1, per_page: int = 6, data_key: str = "data"):
        page = start_page
        while True:
            payload = self.get(endpoint, {page_param: page, per_page_param: per_page})
            records = payload.get(data_key, [])
            total_pages = payload.get("total_pages", 1)
            log.info("API page %d/%d  records=%d", page, total_pages, len(records))
            if not records:
                break
            for rec in records:
                rec["_source_page"] = page
                yield rec
            if page >= total_pages:
                break
            page += 1
