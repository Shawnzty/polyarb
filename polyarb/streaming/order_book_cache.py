from __future__ import annotations

import asyncio
import time
from typing import Dict, Iterable, List, Optional, Set

from polyarb.models.orderbook import OrderBook, OrderLevel


class OrderBookCache:
    """In-memory cache of order books, keyed by token_id.

    Fed by (a) REST-seeded snapshots at bootstrap and (b) WebSocket deltas in
    steady state. `apply_snapshot` replaces the book wholesale; `apply_delta`
    applies price-level diffs against the existing book. Returns True when the
    change is material (best-bid/best-ask moved, or a new level appeared at top),
    so the watcher only re-scores opportunities on meaningful moves.
    """

    def __init__(self) -> None:
        self._books: Dict[str, OrderBook] = {}
        self._lock = asyncio.Lock()

    def snapshot(self) -> Dict[str, OrderBook]:
        # Copy the dict so callers can iterate without holding the lock.
        return dict(self._books)

    def get(self, token_id: str) -> Optional[OrderBook]:
        return self._books.get(token_id)

    def tokens(self) -> Set[str]:
        return set(self._books.keys())

    async def apply_snapshot(self, book: OrderBook) -> bool:
        async with self._lock:
            return self._apply_snapshot_unlocked(book)

    async def apply_snapshots(self, books: Iterable[OrderBook]) -> List[str]:
        changed: List[str] = []
        async with self._lock:
            for book in books:
                if self._apply_snapshot_unlocked(book):
                    changed.append(book.asset_id)
        return changed

    def _apply_snapshot_unlocked(self, book: OrderBook) -> bool:
        if not book.asset_id:
            return False
        prior = self._books.get(book.asset_id)
        self._books[book.asset_id] = book
        if prior is None:
            return True
        return (prior.best_bid, prior.best_ask) != (book.best_bid, book.best_ask)

    async def apply_delta(
        self,
        token_id: str,
        *,
        bids: Optional[List[OrderLevel]] = None,
        asks: Optional[List[OrderLevel]] = None,
        timestamp: Optional[str] = None,
    ) -> bool:
        # Polymarket's `price_change` WS payload sends level-by-level updates:
        # a size of 0 removes that level; a non-zero size replaces it. We apply
        # that here against the cached book and re-sort — the lists are small
        # (a few dozen levels) so a resort is cheap vs. maintaining indices.
        async with self._lock:
            existing = self._books.get(token_id)
            if existing is None:
                # No snapshot yet; can't apply a delta blind. Caller should
                # re-seed via REST before subscribing.
                return False
            new_bids = _apply_level_updates(existing.bids, bids or [], reverse=True)
            new_asks = _apply_level_updates(existing.asks, asks or [], reverse=False)
            updated = OrderBook(
                market=existing.market,
                asset_id=existing.asset_id,
                timestamp=timestamp or existing.timestamp,
                bids=new_bids,
                asks=new_asks,
            )
            self._books[token_id] = updated
            return (existing.best_bid, existing.best_ask) != (updated.best_bid, updated.best_ask)

    def remove(self, token_id: str) -> None:
        self._books.pop(token_id, None)

    def stale_tokens(self, max_age_s: float, now: Optional[float] = None) -> List[str]:
        now = now if now is not None else time.time()
        stale: List[str] = []
        for token_id, book in self._books.items():
            ts = book.timestamp_seconds
            if ts is None or (now - ts) > max_age_s:
                stale.append(token_id)
        return stale


def _apply_level_updates(
    current: List[OrderLevel],
    updates: List[OrderLevel],
    *,
    reverse: bool,
) -> List[OrderLevel]:
    # Fold updates into a price-keyed map: size=0 removes, non-zero replaces.
    by_price: Dict[float, float] = {level.price: level.size for level in current}
    for update in updates:
        if update.price <= 0:
            continue
        if update.size <= 0:
            by_price.pop(update.price, None)
        else:
            by_price[update.price] = update.size
    return sorted(
        (OrderLevel(price=p, size=s) for p, s in by_price.items() if s > 0),
        key=lambda level: level.price,
        reverse=reverse,
    )
