from __future__ import annotations

import asyncio
import time
from typing import Dict, Iterable, List, Optional, Tuple

from polyarb.api.async_http import AsyncHttpClient
from polyarb.api.http import ApiError
from polyarb.models.orderbook import OrderBook


# Fee rates change rarely (they're pinned at market-create time on the CLOB
# contract). Cache by token for 24h so steady-state runs never re-fetch.
_FEE_CACHE_TTL_S = 24 * 3600


class AsyncClobClient:
    """Async CLOB client.

    `get_books` issues the batched POST `/books` first and, on failure,
    fans out the chunk into concurrent per-token `GET /book` calls rather
    than the sync client's one-at-a-time fallback loop. `get_fee_rates`
    concurrent-fetches missing tokens and caches them in-process.
    """

    def __init__(
        self,
        base_url: str = "https://clob.polymarket.com",
        client: Optional[AsyncHttpClient] = None,
        max_concurrent: int = 16,
    ) -> None:
        self.http = client or AsyncHttpClient(base_url)
        self._owns_http = client is None
        self.max_concurrent = max_concurrent
        self._fee_cache: Dict[str, Tuple[float, float]] = {}

    async def __aenter__(self) -> "AsyncClobClient":
        if self._owns_http:
            await self.http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._owns_http:
            await self.http.__aexit__(exc_type, exc, tb)

    async def get_books(
        self,
        token_ids: Iterable[str],
        batch_size: int = 500,
    ) -> Dict[str, OrderBook]:
        unique_ids = list(dict.fromkeys(str(token_id) for token_id in token_ids if token_id))
        if not unique_ids:
            return {}

        chunks = [unique_ids[i : i + batch_size] for i in range(0, len(unique_ids), batch_size)]
        sem = asyncio.Semaphore(self.max_concurrent)

        async def fetch_chunk(chunk: List[str]) -> List[dict]:
            async with sem:
                try:
                    payload = await self.http.post(
                        "/books",
                        [{"token_id": token_id} for token_id in chunk],
                    )
                    if isinstance(payload, list):
                        return [item for item in payload if isinstance(item, dict)]
                except ApiError:
                    pass
                # Fan-out fallback: concurrent per-token gets instead of a serial loop.
                return await self._fanout_books(chunk)

        payloads = await asyncio.gather(*(fetch_chunk(chunk) for chunk in chunks))

        books: Dict[str, OrderBook] = {}
        for payload in payloads:
            for item in payload:
                book = OrderBook.from_clob(item)
                if book.asset_id:
                    books[book.asset_id] = book
        return books

    async def _fanout_books(self, token_ids: List[str]) -> List[dict]:
        sem = asyncio.Semaphore(self.max_concurrent)

        async def fetch_one(token_id: str) -> Optional[dict]:
            async with sem:
                try:
                    payload = await self.http.get("/book", params={"token_id": token_id})
                except ApiError:
                    return None
                return payload if isinstance(payload, dict) else None

        results = await asyncio.gather(*(fetch_one(t) for t in token_ids))
        return [r for r in results if r is not None]

    async def get_fee_rates(self, token_ids: Iterable[str]) -> Dict[str, float]:
        unique_ids = list(dict.fromkeys(str(token_id) for token_id in token_ids if token_id))
        if not unique_ids:
            return {}

        rates: Dict[str, float] = {}
        misses: List[str] = []
        now = time.monotonic()
        for token_id in unique_ids:
            cached = self._fee_cache.get(token_id)
            if cached and now - cached[0] < _FEE_CACHE_TTL_S:
                rates[token_id] = cached[1]
            else:
                misses.append(token_id)

        if not misses:
            return rates

        sem = asyncio.Semaphore(self.max_concurrent)

        async def fetch_rate(token_id: str) -> Tuple[str, Optional[float]]:
            async with sem:
                try:
                    payload = await self.http.get("/fee-rate", params={"token_id": token_id})
                except ApiError:
                    return token_id, None
                if not isinstance(payload, dict):
                    return token_id, None
                base_fee = payload.get("base_fee")
                try:
                    return token_id, max(0.0, float(base_fee) / 1000.0)
                except (TypeError, ValueError):
                    return token_id, None

        fetched = await asyncio.gather(*(fetch_rate(t) for t in misses))
        for token_id, rate in fetched:
            if rate is None:
                continue
            rates[token_id] = rate
            self._fee_cache[token_id] = (now, rate)
        return rates
