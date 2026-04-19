from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

import httpx

from polyarb.api.http import ApiError


class AsyncHttpClient:
    """Async counterpart to `HttpClient`, backed by a shared `httpx.AsyncClient`.

    Defaults are tuned for latency over tolerance: a 4s per-call timeout
    (vs. the sync client's 10s) so a single slow endpoint can't stall the
    pipeline, and 3 retries with 0.5s exponential backoff. All calls share
    one HTTP/2 connection pool.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 4.0,
        retries: int = 3,
        backoff: float = 0.5,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "AsyncHttpClient":
        if self._client is None:
            self._client = httpx.AsyncClient(
                http2=False,  # HTTP/2 requires `h2`; keep optional for cleaner installs.
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "polyarb-research-scanner/0.1",
                },
                timeout=self.timeout,
            )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return await self._request("GET", path, params=params)

    async def post(self, path: str, json_body: Any) -> Any:
        return await self._request("POST", path, json=json_body)

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        if self._client is None:
            raise RuntimeError("AsyncHttpClient used outside its async-context")
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_error: Optional[BaseException] = None

        for attempt in range(self.retries):
            try:
                response = await self._client.request(method, url, **kwargs)
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise ApiError(f"{method} {url} returned {response.status_code}")
                response.raise_for_status()
                if not response.content:
                    return None
                return response.json()
            except (httpx.HTTPError, ValueError, ApiError) as exc:
                last_error = exc
                if attempt == self.retries - 1:
                    break
                await asyncio.sleep(self.backoff * (2 ** attempt))

        raise ApiError(f"{method} {url} failed after {self.retries} attempts: {last_error}")
