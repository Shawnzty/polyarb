from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Dict, List, Sequence

import pytest

from polyarb.models.event import GammaEvent
from polyarb.models.opportunity import ExecutionEstimate, Opportunity, OpportunityMarket
from polyarb.models.orderbook import OrderBook, OrderLevel
from polyarb.streaming.diff import diff_opportunities, opportunity_identity
from polyarb.streaming.order_book_cache import OrderBookCache
from polyarb.streaming.state_store import StateStore
from polyarb.streaming.watcher import Watcher, WatcherConfig
from tests.conftest import book_payload, event_payload, market_payload


def _make_opportunity(
    event_id: str = "evt-1",
    opp_type: str = "neg-risk-underround",
    market_ids: Sequence[str] = ("m-a", "m-b"),
    edge: float = 1.0,
    executable: bool = True,
) -> Opportunity:
    estimate = ExecutionEstimate(
        target_size=100.0,
        executable=executable,
        cost=99.0,
        payout=100.0,
        edge=edge,
        edge_pct=edge / 100.0,
        gross_cost=99.0,
        fee_cost=0.0,
        net_cost=99.0,
        leg_count=len(market_ids),
        missing_legs=[],
        note="",
        max_executable_size=100.0,
        atomic_risk=False,
        below_min_order=False,
        min_book_timestamp=None,
    )
    return Opportunity(
        type=opp_type,
        title="fixture",
        event={"id": event_id, "title": event_id, "slug": event_id},
        markets=[
            OpportunityMarket(
                id=mid,
                title=mid,
                yes_token_id=f"{mid}-yes",
                no_token_id=f"{mid}-no",
                yes_price=0.5,
                no_price=0.5,
                volume=0.0,
                liquidity=0.0,
            )
            for mid in market_ids
        ],
        theoretical={},
        execution_by_size={"100": estimate},
        liquidity={},
        confidence=0.9,
        warnings=[],
        explanation="",
    )


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


def test_order_book_cache_snapshot_and_delta_application():
    async def body():
        cache = OrderBookCache()
        book = OrderBook.from_clob(
            book_payload(
                "tok-1",
                asks=[{"price": "0.60", "size": "100"}, {"price": "0.62", "size": "200"}],
                bids=[{"price": "0.58", "size": "150"}],
            )
        )
        assert await cache.apply_snapshot(book) is True
        assert cache.get("tok-1").best_ask == 0.60

        # Non-material delta: add a deep level behind the top. best_bid/ask unchanged.
        assert await cache.apply_delta(
            "tok-1",
            asks=[OrderLevel(price=0.70, size=50.0)],
        ) is False

        # Material delta: top level gets eaten (size=0 removes it).
        assert await cache.apply_delta(
            "tok-1",
            asks=[OrderLevel(price=0.60, size=0.0)],
        ) is True
        assert cache.get("tok-1").best_ask == 0.62

    _run(body())


def test_order_book_cache_stale_tokens_flag_old_books():
    async def body():
        cache = OrderBookCache()
        now = 1_700_000_000.0
        fresh = OrderBook.from_clob(
            book_payload("fresh", asks=[{"price": "0.5", "size": "10"}],
                         timestamp=str(int(now * 1000)))
        )
        stale = OrderBook.from_clob(
            book_payload("stale", asks=[{"price": "0.5", "size": "10"}],
                         timestamp=str(int((now - 60) * 1000)))
        )
        await cache.apply_snapshots([fresh, stale])
        assert cache.stale_tokens(max_age_s=10.0, now=now) == ["stale"]

    _run(body())


def test_diff_opportunities_emits_new_changed_closed():
    prior = [_make_opportunity(event_id="evt-a", edge=1.0)]
    current_edge_jumped = _make_opportunity(event_id="evt-a", edge=5.0)
    freshly_born = _make_opportunity(event_id="evt-b", edge=2.0)

    diff = diff_opportunities(prior, [current_edge_jumped, freshly_born])
    assert [opportunity_identity(o) for o in diff.new] == [opportunity_identity(freshly_born)]
    assert len(diff.changed) == 1
    prior_seen, current_seen = diff.changed[0]
    assert prior_seen.execution_by_size["100"].edge == 1.0
    assert current_seen.execution_by_size["100"].edge == 5.0
    assert diff.closed == []


def test_diff_opportunities_suppresses_sub_threshold_wiggle():
    prior = [_make_opportunity(event_id="evt-a", edge=1.00)]
    current = [_make_opportunity(event_id="evt-a", edge=1.10)]
    diff = diff_opportunities(prior, current, edge_change_threshold=0.5)
    assert diff.is_empty()


def test_diff_opportunities_flags_executable_transition():
    # An opportunity flipping from executable to not is a CHANGED event
    # even if the edge delta is below the threshold.
    prior = [_make_opportunity(event_id="evt-a", edge=1.0, executable=True)]
    current = [_make_opportunity(event_id="evt-a", edge=1.0, executable=False)]
    diff = diff_opportunities(prior, current, edge_change_threshold=10.0)
    assert len(diff.changed) == 1


def test_state_store_appends_jsonl_lifecycle(tmp_path: Path):
    path = tmp_path / "state.jsonl"
    store = StateStore(path)
    prior = [_make_opportunity(event_id="evt-a", edge=1.0)]
    current = [_make_opportunity(event_id="evt-a", edge=5.0), _make_opportunity(event_id="evt-b")]
    diff = diff_opportunities(prior, current)

    store.record_scan("run-1", {"events": 5})
    store.record_diff("run-1", diff)

    records = store.records()
    kinds = [r["kind"] for r in records]
    assert "scan" in kinds
    assert kinds.count("new") == 1
    assert kinds.count("changed") == 1
    assert all("ts" in r for r in records)


class _FakeAsyncGamma:
    def __init__(self, events: List[GammaEvent]) -> None:
        self.events = events

    async def get_events(self, limit_events: int, min_volume: float) -> List[GammaEvent]:
        return [e for e in self.events if e.volume >= min_volume][:limit_events]


class _FakeAsyncClob:
    def __init__(self, books: Dict[str, OrderBook]) -> None:
        self.books = books

    async def get_books(self, token_ids: Sequence[str]) -> Dict[str, OrderBook]:
        return {t: self.books[t] for t in token_ids if t in self.books}

    async def get_fee_rates(self, token_ids: Sequence[str]) -> Dict[str, float]:
        return {}


def test_watcher_bootstrap_rescore_emits_new_then_stable(neg_risk_event, simple_books):
    async def body():
        config = WatcherConfig(target_sizes=[100.0], within_hours=None, limit_events=10)
        watcher = Watcher(
            config,
            _FakeAsyncGamma([neg_risk_event]),
            _FakeAsyncClob(simple_books),
        )
        await watcher.bootstrap()
        first_diff = await watcher.rescore()
        assert len(first_diff.new) == 1
        assert first_diff.closed == []

        # Second pass with no state change: diff should be empty.
        second_diff = await watcher.rescore()
        assert second_diff.is_empty()

    _run(body())


def test_watcher_rescore_reacts_to_book_change(neg_risk_event, simple_books):
    async def body():
        config = WatcherConfig(
            target_sizes=[100.0], within_hours=None, limit_events=10, edge_change_threshold=0.01
        )
        watcher = Watcher(
            config,
            _FakeAsyncGamma([neg_risk_event]),
            _FakeAsyncClob(simple_books),
        )
        await watcher.bootstrap()
        await watcher.rescore()

        # Drop one leg from the cache: the opportunity should close.
        watcher.cache.remove("a-yes")
        diff = await watcher.rescore()
        assert len(diff.closed) == 1
        assert diff.new == []

    _run(body())
