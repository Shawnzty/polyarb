from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from polyarb.models.market import GammaMarket
from polyarb.models.opportunity import ExecutionEstimate
from polyarb.models.orderbook import OrderBook


def book_spread_warning(book: Optional[OrderBook], fallback_spread: Optional[float]) -> bool:
    spread = book.spread if book and book.spread is not None else fallback_spread
    return spread is not None and spread > 0.05


def estimate_basket_cost(
    legs: Iterable[Tuple[GammaMarket, Optional[str], str]],
    books_by_token: Dict[str, OrderBook],
    target_size: float,
) -> ExecutionEstimate:
    total_cost = 0.0
    missing: List[str] = []
    leg_count = 0

    for market, token_id, label in legs:
        leg_count += 1
        if not token_id:
            missing.append(f"{market.display_title}: missing {label} token")
            continue
        book = books_by_token.get(token_id)
        if not book or not book.asks:
            missing.append(f"{market.display_title}: missing {label} book")
            continue
        fill = book.buy_shares(target_size)
        if not fill.executable:
            missing.append(
                f"{market.display_title}: only {fill.filled_shares:.2f}/{target_size:.2f} shares"
            )
            continue
        total_cost += fill.cost

    executable = not missing and leg_count > 0
    payout = target_size
    edge = payout - total_cost if executable else None
    edge_pct = edge / payout if executable and payout else None
    return ExecutionEstimate(
        target_size=target_size,
        executable=executable,
        cost=total_cost if executable else None,
        payout=payout,
        edge=edge,
        edge_pct=edge_pct,
        missing_legs=missing,
        note="payout-notional basket" if executable else "insufficient depth",
    )
