from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests


class ApiError(RuntimeError):
    """Raised when a public Polymarket API request cannot be completed."""


class HttpClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        retries: int = 3,
        backoff: float = 0.5,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "polyarb-research-scanner/0.1",
            }
        )

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    def post(self, path: str, json_body: Any) -> Any:
        return self._request("POST", path, json=json_body)

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_error: Optional[BaseException] = None

        for attempt in range(self.retries):
            try:
                response = self.session.request(method, url, timeout=self.timeout, **kwargs)
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise ApiError(f"{method} {url} returned {response.status_code}")
                response.raise_for_status()
                if not response.content:
                    return None
                return response.json()
            except (requests.RequestException, ValueError, ApiError) as exc:
                last_error = exc
                if attempt == self.retries - 1:
                    break
                time.sleep(self.backoff * (2**attempt))

        raise ApiError(f"{method} {url} failed after {self.retries} attempts: {last_error}")
