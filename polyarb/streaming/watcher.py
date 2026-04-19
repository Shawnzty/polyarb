from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Protocol, Sequence

from polyarb.api.gamma_client import collect_fee_token_ids, collect_market_token_ids
from polyarb.models.event import GammaEvent
from polyarb.models.opportunity import Opportunity
from polyarb.models.orderbook import OrderBook
from polyarb.ranking.scoring import score_opportunities
from polyarb.scanners.correlated_scanner import CorrelatedScanner
from polyarb.scanners.neg_risk_scanner import NegRiskScanner
from polyarb.streaming.diff import OpportunityDiff, diff_opportunities
from polyarb.streaming.order_book_cache import OrderBookCache
from polyarb.streaming.state_store import StateStore
from polyarb.timeutils import filter_events_by_horizon


class EventSource(Protocol):
    async def get_events(self, limit_events: int, min_volume: float) -> List[GammaEvent]: ...


class BookSource(Protocol):
    async def get_books(self, token_ids: Sequence[str]) -> Dict[str, OrderBook]: ...

    async def get_fee_rates(self, token_ids: Sequence[str]) -> Dict[str, float]: ...


DiffHandler = Callable[[OpportunityDiff, List[Opportunity]], Awaitable[None]]


@dataclass
class WatcherConfig:
    target_sizes: List[float]
    within_hours: Optional[float]
    limit_events: int = 200
    min_volume: float = 0.0
    max_results: int = 25
    max_book_age_s: Optional[float] = 60.0
    rank_by: str = "apy"
    neg_risk_only: bool = False
    correlated_only: bool = False
    risk_config: Dict[str, float] = field(default_factory=dict)
    edge_change_threshold: float = 0.5


class Watcher:
    """Bootstrap + re-score orchestrator.

    Today this is a bootstrap-only, single-pass orchestrator with a pluggable
    WS delta feed. The plan separates this layer from the scanners so an
    async WebSocket producer can push deltas into `OrderBookCache` and trigger
    `rescore()` without changing scanner code.
    """

    def __init__(
        self,
        config: WatcherConfig,
        events_source: EventSource,
        books_source: BookSource,
        state_store: Optional[StateStore] = None,
        diff_handler: Optional[DiffHandler] = None,
    ) -> None:
        self.config = config
        self.events_source = events_source
        self.books_source = books_source
        self.state_store = state_store or StateStore()
        self.diff_handler = diff_handler
        self.cache = OrderBookCache()
        self._events: List[GammaEvent] = []
        self._scanned_events: List[GammaEvent] = []
        self._fee_rates: Dict[str, float] = {}
        self._last_opportunities: List[Opportunity] = []

    async def bootstrap(self) -> None:
        self._events = await self.events_source.get_events(
            limit_events=self.config.limit_events,
            min_volume=self.config.min_volume,
        )
        self._scanned_events = filter_events_by_horizon(self._events, self.config.within_hours)
        include_no = not self.config.neg_risk_only
        token_ids = collect_market_token_ids(self._scanned_events, include_no=include_no)
        fee_token_ids = collect_fee_token_ids(self._scanned_events, include_no=include_no)

        books, fee_rates = await asyncio.gather(
            self.books_source.get_books(token_ids),
            self.books_source.get_fee_rates(fee_token_ids) if fee_token_ids else _empty_fees(),
        )
        await self.cache.apply_snapshots(books.values())
        self._fee_rates = fee_rates

    async def rescore(self, run_id: Optional[str] = None) -> OpportunityDiff:
        # A single scoring pass over the current cache state. Safe to call on
        # every WS delta once a book source is wired; the diff emitter
        # suppresses sub-threshold edge movement.
        run_id = run_id or uuid.uuid4().hex[:12]
        opportunities = self._run_scanners(self.cache.snapshot())
        scored = score_opportunities(
            opportunities,
            self.config.risk_config,
            rank_by=self.config.rank_by,
        )[: self.config.max_results]
        for rank, opportunity in enumerate(scored, start=1):
            opportunity.rank = rank

        diff = diff_opportunities(
            self._last_opportunities,
            scored,
            edge_change_threshold=self.config.edge_change_threshold,
        )
        self._last_opportunities = scored

        self.state_store.record_scan(
            run_id,
            {
                "events": len(self._scanned_events),
                "books": len(self.cache.tokens()),
                "opportunities": len(scored),
                **diff.summary(),
            },
        )
        self.state_store.record_diff(run_id, diff)
        if self.diff_handler is not None:
            await self.diff_handler(diff, scored)
        return diff

    @property
    def last_opportunities(self) -> List[Opportunity]:
        return list(self._last_opportunities)

    @property
    def events(self) -> List[GammaEvent]:
        return list(self._events)

    @property
    def scanned_events(self) -> List[GammaEvent]:
        return list(self._scanned_events)

    def _run_scanners(self, books_by_token: Dict[str, OrderBook]) -> List[Opportunity]:
        opportunities: List[Opportunity] = []
        if not self.config.correlated_only:
            opportunities.extend(
                NegRiskScanner(
                    self.config.target_sizes,
                    self._fee_rates,
                    max_book_age_s=self.config.max_book_age_s,
                ).scan(self._scanned_events, books_by_token)
            )
        if not self.config.neg_risk_only:
            opportunities.extend(
                CorrelatedScanner(
                    self.config.target_sizes,
                    self._fee_rates,
                    max_book_age_s=self.config.max_book_age_s,
                ).scan(self._scanned_events, books_by_token)
            )
        return opportunities

    def build_report(self, diff: OpportunityDiff) -> Dict[str, Any]:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_counts": {
                "events_fetched": len(self._events),
                "events": len(self._scanned_events),
                "markets": sum(len(event.markets) for event in self._scanned_events),
                "books": len(self.cache.tokens()),
                "fee_rates": len(self._fee_rates),
            },
            "diff": {
                "new": [opp.to_dict() for opp in diff.new],
                "changed": [
                    {"prior": prior.to_dict(), "current": current.to_dict()}
                    for prior, current in diff.changed
                ],
                "closed": [opp.to_dict() for opp in diff.closed],
            },
            "opportunities": [opportunity.to_dict() for opportunity in self._last_opportunities],
        }


async def _empty_fees() -> Dict[str, float]:
    return {}
