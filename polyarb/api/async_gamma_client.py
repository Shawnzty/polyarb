from __future__ import annotations

import asyncio
from typing import List, Optional

from polyarb.api.async_http import AsyncHttpClient
from polyarb.api.http import ApiError
from polyarb.models.event import GammaEvent


class AsyncGammaClient:
    """Async Gamma discovery client.

    Pagination runs concurrently: we issue `ceil(limit_events / page_size)`
    `offset` requests in parallel (bounded to `max_concurrent`), then
    de-duplicate + filter client-side. The sync client's serial loop cost
    ~1 RTT per page; this collapses to ~1 RTT total.
    """

    def __init__(
        self,
        base_url: str = "https://gamma-api.polymarket.com",
        client: Optional[AsyncHttpClient] = None,
        max_concurrent: int = 6,
    ) -> None:
        self.http = client or AsyncHttpClient(base_url)
        self._owns_http = client is None
        self.max_concurrent = max_concurrent

    async def __aenter__(self) -> "AsyncGammaClient":
        if self._owns_http:
            await self.http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._owns_http:
            await self.http.__aexit__(exc_type, exc, tb)

    async def get_events(
        self,
        limit_events: int = 200,
        min_volume: float = 0.0,
        page_size: int = 100,
    ) -> List[GammaEvent]:
        effective_page_size = max(1, min(page_size, 500))
        pages_needed = max(1, (limit_events + effective_page_size - 1) // effective_page_size)
        offsets = [i * effective_page_size for i in range(pages_needed)]

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_page(offset: int):
            async with semaphore:
                try:
                    return await self.http.get(
                        "/events",
                        params={
                            "active": "true",
                            "closed": "false",
                            "limit": effective_page_size,
                            "offset": offset,
                            "order": "volume24hr",
                            "ascending": "false",
                        },
                    )
                except ApiError:
                    return None

        pages = await asyncio.gather(*(fetch_page(o) for o in offsets))

        events: List[GammaEvent] = []
        seen_ids = set()
        for page in pages:
            if not isinstance(page, list):
                continue
            for item in page:
                if not isinstance(item, dict):
                    continue
                event = GammaEvent.from_gamma(item)
                if event.id in seen_ids:
                    continue
                if not event.active or event.closed or event.volume < min_volume:
                    continue
                events.append(event)
                seen_ids.add(event.id)
                if len(events) >= limit_events:
                    return events
        return events
