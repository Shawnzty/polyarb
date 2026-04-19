from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional

from polyarb.models.parsing import as_float, clean_text


_ZERO = Decimal("0")
_ONE = Decimal("1")
_FILL_EPS = Decimal("1e-9")


def _d(value: float) -> Decimal:
    # Decimal(str(float)) rounds to the float's short-repr, which is what the
    # CLOB itself prints: 4dp prices, 2dp sizes. This avoids float-binary
    # residue (0.1 + 0.2 ≠ 0.3) that would otherwise leave shelf-boundary
    # fills at 1e-17 and trip the "remaining <= eps" check.
    return Decimal(str(value))


@dataclass(frozen=True)
class OrderLevel:
    price: float
    size: float


@dataclass(frozen=True)
class FillEstimate:
    requested_shares: float
    filled_shares: float
    cost: float
    avg_price: Optional[float]
    executable: bool
    gross_cost: float = 0.0
    fee_cost: float = 0.0

    @property
    def available_shares(self) -> float:
        return self.filled_shares


@dataclass(frozen=True)
class OrderBook:
    market: str
    asset_id: str
    timestamp: str
    bids: List[OrderLevel]
    asks: List[OrderLevel]

    @classmethod
    def from_clob(cls, payload: Dict[str, Any]) -> "OrderBook":
        bids = [
            OrderLevel(price=as_float(level.get("price")), size=as_float(level.get("size")))
            for level in payload.get("bids", [])
            if isinstance(level, dict)
        ]
        asks = [
            OrderLevel(price=as_float(level.get("price")), size=as_float(level.get("size")))
            for level in payload.get("asks", [])
            if isinstance(level, dict)
        ]
        return cls(
            market=clean_text(payload.get("market")),
            asset_id=clean_text(payload.get("asset_id")),
            timestamp=clean_text(payload.get("timestamp")),
            bids=sorted([level for level in bids if level.price > 0 and level.size > 0], key=lambda x: x.price, reverse=True),
            asks=sorted([level for level in asks if level.price > 0 and level.size > 0], key=lambda x: x.price),
        )

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return max(0.0, self.best_ask - self.best_bid)

    @property
    def total_ask_size(self) -> float:
        return float(sum(_d(level.size) for level in self.asks))

    @property
    def timestamp_seconds(self) -> Optional[float]:
        # The Polymarket CLOB `timestamp` field on book payloads is a string
        # in milliseconds since epoch. Return it as a Unix-seconds float so
        # callers can compare with `time.time()`/`datetime` trivially. Returns
        # None on missing/unparseable values — callers treat that as
        # "unknown age" rather than "fresh".
        if not self.timestamp:
            return None
        try:
            millis = int(self.timestamp)
        except (TypeError, ValueError):
            return None
        return millis / 1000.0

    def buy_shares(self, shares: float, fee_rate: float = 0.0) -> FillEstimate:
        remaining = _d(max(0.0, shares))
        filled = _ZERO
        gross_cost = _ZERO
        fee_cost = _ZERO
        rate = _d(max(0.0, fee_rate))
        for level in self.asks:
            if remaining <= _ZERO:
                break
            price = _d(level.price)
            size = _d(level.size)
            take = remaining if remaining < size else size
            gross_cost += take * price
            # Polymarket CLOB fee: fee = C * feeRate * p * (1 - p).
            # https://docs.polymarket.com/trading/fees
            fee_cost += take * rate * price * (_ONE - price)
            filled += take
            remaining -= take

        executable = remaining <= _FILL_EPS
        cost = gross_cost + fee_cost
        avg_price = float(gross_cost / filled) if filled > _ZERO else None
        return FillEstimate(
            requested_shares=shares,
            filled_shares=float(filled),
            cost=float(cost),
            avg_price=avg_price,
            executable=executable,
            gross_cost=float(gross_cost),
            fee_cost=float(fee_cost),
        )
