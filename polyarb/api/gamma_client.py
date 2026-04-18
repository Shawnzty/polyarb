from __future__ import annotations

from typing import Iterable, List, Optional

from polyarb.api.http import HttpClient
from polyarb.models.event import GammaEvent


class GammaClient:
    """Read-only client for Polymarket Gamma discovery endpoints."""

    def __init__(self, base_url: str = "https://gamma-api.polymarket.com") -> None:
        self.http = HttpClient(base_url)

    def get_events(
        self,
        limit_events: int = 200,
        min_volume: float = 0.0,
        page_size: int = 100,
    ) -> List[GammaEvent]:
        events: List[GammaEvent] = []
        offset = 0
        effective_page_size = max(1, min(page_size, 500))

        while len(events) < limit_events:
            payload = self.http.get(
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
            if not isinstance(payload, list) or not payload:
                break

            parsed_page = [GammaEvent.from_gamma(item) for item in payload if isinstance(item, dict)]
            for event in parsed_page:
                if event.active and not event.closed and event.volume >= min_volume:
                    events.append(event)
                    if len(events) >= limit_events:
                        break

            if len(payload) < effective_page_size:
                break
            offset += effective_page_size

        return events


def collect_market_token_ids(events: Iterable[GammaEvent], include_no: bool = True) -> List[str]:
    token_ids = []
    seen = set()
    for event in events:
        for market in event.markets:
            candidates = [market.yes_token_id]
            if include_no:
                candidates.append(market.no_token_id)
            for token_id in candidates:
                if token_id and token_id not in seen:
                    token_ids.append(token_id)
                    seen.add(token_id)
    return token_ids
