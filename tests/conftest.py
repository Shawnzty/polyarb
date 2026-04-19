from __future__ import annotations

import time
from typing import Dict, Iterable, List

import pytest

from polyarb.models.event import GammaEvent
from polyarb.models.orderbook import OrderBook


def market_payload(
    market_id: str,
    title: str,
    yes_price: float,
    yes_token: str,
    no_token: str = None,
    active: bool = True,
    liquidity: float = 1500.0,
    volume: float = 30000.0,
    extra: dict = None,
) -> dict:
    no_token = no_token or f"{yes_token}-no"
    payload = {
        "id": market_id,
        "question": f"Will {title} win?",
        "slug": market_id,
        "description": "Fixture market rules",
        "endDate": "2026-12-31T00:00:00Z",
        "resolutionSource": "fixture-source",
        "groupItemTitle": title,
        "outcomes": "[\"Yes\", \"No\"]",
        "outcomePrices": f"[\"{yes_price}\", \"{1 - yes_price}\"]",
        "clobTokenIds": f"[\"{yes_token}\", \"{no_token}\"]",
        "active": active,
        "closed": False,
        "enableOrderBook": True,
        "acceptingOrders": True,
        "negRisk": True,
        "negRiskOther": title.lower() == "other",
        "feesEnabled": False,
        "volumeNum": volume,
        "volume24hrClob": volume / 10,
        "liquidityNum": liquidity,
        "spread": 0.01,
    }
    if extra:
        payload.update(extra)
    return payload


def event_payload(
    event_id: str,
    title: str,
    markets: List[dict],
    neg_risk: bool = True,
    neg_risk_augmented: bool = False,
    extra: dict = None,
) -> dict:
    payload = {
        "id": event_id,
        "title": title,
        "slug": event_id,
        "description": "Fixture event",
        "resolutionSource": "fixture-source",
        "active": True,
        "closed": False,
        "negRisk": neg_risk,
        "negRiskAugmented": neg_risk_augmented,
        "enableNegRisk": neg_risk,
        "showAllOutcomes": True,
        "volume": 100000.0,
        "volume24hr": 10000.0,
        "liquidity": 5000.0,
        "endDate": "2026-12-31T00:00:00Z",
        "markets": markets,
    }
    if extra:
        payload.update(extra)
    return payload


def book_payload(token_id: str, asks: list, bids: list = None, timestamp: str = None) -> dict:
    bids = bids or [{"price": "0.20", "size": "1000"}]
    # Default to now so the staleness gate (max_book_age_s) does not silently
    # drop every fixture. Tests that want to exercise staleness explicitly
    # pass an older timestamp.
    if timestamp is None:
        timestamp = str(int(time.time() * 1000))
    return {
        "market": f"market-{token_id}",
        "asset_id": token_id,
        "timestamp": timestamp,
        "bids": bids,
        "asks": asks,
    }


@pytest.fixture
def neg_risk_event() -> GammaEvent:
    return GammaEvent.from_gamma(
        event_payload(
            "fixture-neg",
            "Fixture Election Winner",
            [
                market_payload("m-a", "A", 0.30, "a-yes", "a-no"),
                market_payload("m-b", "B", 0.30, "b-yes", "b-no"),
                market_payload(
                    "m-other",
                    "Other",
                    0.39,
                    "other-yes",
                    "other-no",
                    extra={"question": "Will Any Other Candidate win?", "negRiskOther": True},
                ),
            ],
        )
    )


@pytest.fixture
def simple_books() -> Dict[str, OrderBook]:
    books = {}
    for token_id, price in {
        "a-yes": 0.30,
        "b-yes": 0.30,
        "other-yes": 0.39,
        "a-no": 0.70,
        "b-no": 0.70,
        "other-no": 0.61,
        "early-yes": 0.60,
        "early-no": 0.40,
        "later-yes": 0.55,
        "later-no": 0.45,
        "easy-yes": 0.50,
        "easy-no": 0.50,
        "hard-yes": 0.55,
        "hard-no": 0.45,
    }.items():
        books[token_id] = OrderBook.from_clob(
            book_payload(
                token_id,
                asks=[
                    {"price": f"{min(price + 0.02, 0.99):.2f}", "size": "1000"},
                    {"price": f"{price:.2f}", "size": "1000"},
                ],
                bids=[{"price": f"{max(price - 0.01, 0.01):.2f}", "size": "1000"}],
            )
        )
    return books


class FakeGammaClient:
    def __init__(self, events: Iterable[GammaEvent]):
        self.events = list(events)

    def get_events(self, limit_events: int = 200, min_volume: float = 0.0):
        return [event for event in self.events if event.volume >= min_volume][:limit_events]


class FakeClobClient:
    def __init__(self, books: Dict[str, OrderBook]):
        self.books = books

    def get_books(self, token_ids):
        return {token_id: self.books[token_id] for token_id in token_ids if token_id in self.books}

    def get_fee_rates(self, token_ids):
        return {}
