from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from polyarb.models.market import GammaMarket
from polyarb.models.opportunity import ExecutionEstimate
from polyarb.models.orderbook import OrderBook


# Polymarket CLOB minimum order: $1 notional per leg. Source:
# https://docs.polymarket.com/developers/CLOB/orders/creating-orders
MIN_ORDER_NOTIONAL_USD = 1.0


def book_spread_warning(book: Optional[OrderBook], fallback_spread: Optional[float]) -> bool:
    spread = book.spread if book and book.spread is not None else fallback_spread
    return spread is not None and spread > 0.05


def _fee_rate_for_leg(
    market: GammaMarket,
    token_id: str,
    fee_rates_by_token: Dict[str, float],
) -> float:
    fee_rate = market.fee_rate
    if fee_rate is None:
        fee_rate = fee_rates_by_token.get(token_id, 0.0)
    return fee_rate or 0.0


def estimate_basket_cost(
    legs: Iterable[Tuple[GammaMarket, Optional[str], str]],
    books_by_token: Dict[str, OrderBook],
    target_size: float,
    fee_rates_by_token: Optional[Dict[str, float]] = None,
    *,
    enforce_min_order: bool = True,
) -> ExecutionEstimate:
    """Walk the CLOB asks to price a basket of `target_size` shares.

    `enforce_min_order=False` disables the per-leg $1 min-order gate. The
    scanners set this flag when calling with `target_size=1.0` to derive a
    theoretical per-share cost — at that size every leg is sub-$1 and would
    otherwise non-executable even when the real $100/$1000 sizes are fine.
    """
    fee_rates_by_token = fee_rates_by_token or {}
    legs = list(legs)
    missing: List[str] = []
    resolved: List[Tuple[GammaMarket, str, str, OrderBook, float]] = []
    per_leg_depth: List[float] = []

    for market, token_id, label in legs:
        if not token_id:
            missing.append(f"{market.display_title}: missing {label} token")
            continue
        book = books_by_token.get(token_id)
        if not book or not book.asks:
            missing.append(f"{market.display_title}: missing {label} book")
            continue
        fee_rate = _fee_rate_for_leg(market, token_id, fee_rates_by_token)
        resolved.append((market, token_id, label, book, fee_rate))
        per_leg_depth.append(book.total_ask_size)

    leg_count = len(legs)
    min_book_timestamp = _min_book_timestamp(resolved)
    if missing or not resolved:
        return ExecutionEstimate(
            target_size=target_size,
            executable=False,
            cost=None,
            gross_cost=None,
            fee_cost=None,
            net_cost=None,
            payout=target_size,
            edge=None,
            edge_pct=None,
            leg_count=leg_count,
            missing_legs=missing,
            note="insufficient depth",
            max_executable_size=0.0,
            atomic_risk=True,
            below_min_order=False,
            min_book_timestamp=min_book_timestamp,
        )

    # Shrink to the shallowest leg: sizing any leg above min per-leg depth
    # leaves us naked on partial fills (execution is sequential, not atomic).
    shallowest = min(per_leg_depth)
    effective_size = min(target_size, shallowest)
    atomic_risk = effective_size + 1e-9 < target_size

    total_gross_cost = 0.0
    total_fee_cost = 0.0
    per_leg_notional: List[float] = []
    for market, token_id, label, book, fee_rate in resolved:
        fill = book.buy_shares(effective_size, fee_rate=fee_rate)
        if not fill.executable:
            # Should not happen given shrink-to-min-depth, but keep a guard.
            missing.append(
                f"{market.display_title}: only {fill.filled_shares:.2f}/{effective_size:.2f} shares"
            )
            continue
        total_gross_cost += fill.gross_cost
        total_fee_cost += fill.fee_cost
        per_leg_notional.append(fill.gross_cost + fill.fee_cost)

    # Minimum-order check: the CLOB rejects any leg with notional < $1. A
    # "technically fillable" basket whose cheapest leg is below that floor is
    # not actually executable. Disabled for per-share probes — see docstring.
    if enforce_min_order:
        below_min_order = any(cost < MIN_ORDER_NOTIONAL_USD for cost in per_leg_notional) if per_leg_notional else True
    else:
        below_min_order = False

    executable = not missing and not atomic_risk and not below_min_order
    total_cost = total_gross_cost + total_fee_cost
    payout = effective_size
    edge = payout - total_cost if executable else None
    edge_pct = edge / payout if executable and payout else None

    if missing:
        note = "insufficient depth"
    elif atomic_risk:
        note = "atomic-risk: target exceeds shallowest leg"
    elif below_min_order:
        note = "below per-leg min-order notional"
    else:
        note = "payout-notional basket"

    return ExecutionEstimate(
        target_size=target_size,
        executable=executable,
        cost=total_cost if executable else None,
        gross_cost=total_gross_cost if executable else None,
        fee_cost=total_fee_cost if executable else None,
        net_cost=total_cost if executable else None,
        payout=payout,
        edge=edge,
        edge_pct=edge_pct,
        leg_count=leg_count,
        missing_legs=missing,
        note=note,
        max_executable_size=effective_size,
        atomic_risk=atomic_risk,
        below_min_order=below_min_order,
        min_book_timestamp=min_book_timestamp,
    )


def _min_book_timestamp(
    resolved: List[Tuple[GammaMarket, str, str, OrderBook, float]],
) -> Optional[float]:
    timestamps = [book.timestamp_seconds for _, _, _, book, _ in resolved if book.timestamp_seconds is not None]
    return min(timestamps) if timestamps else None
