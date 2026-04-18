from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional

from polyarb.models.event import GammaEvent
from polyarb.models.market import GammaMarket


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def market_end_datetime(market: GammaMarket, event: GammaEvent) -> Optional[datetime]:
    return parse_datetime(market.end_date) or parse_datetime(event.end_date)


def is_within_horizon(
    market: GammaMarket,
    event: GammaEvent,
    now: datetime,
    within_hours: Optional[float],
) -> bool:
    if within_hours is None:
        return True
    end = market_end_datetime(market, event)
    if end is None:
        return False
    return now <= end <= now + timedelta(hours=within_hours)


def filter_events_by_horizon(
    events: Iterable[GammaEvent],
    within_hours: Optional[float],
    now: Optional[datetime] = None,
) -> List[GammaEvent]:
    if within_hours is None:
        return list(events)
    now = now or datetime.now(timezone.utc)
    filtered: List[GammaEvent] = []
    for event in events:
        active_markets = [market for market in event.markets if market.active and not market.closed]
        if event.neg_risk:
            if active_markets and all(is_within_horizon(market, event, now, within_hours) for market in active_markets):
                filtered.append(event)
            continue
        markets = [market for market in event.markets if is_within_horizon(market, event, now, within_hours)]
        if markets:
            filtered.append(replace(event, markets=markets))
    return filtered
