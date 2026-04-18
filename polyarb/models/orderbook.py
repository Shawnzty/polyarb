from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from polyarb.models.parsing import as_float, clean_text


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

    def buy_shares(self, shares: float) -> FillEstimate:
        remaining = max(0.0, shares)
        filled = 0.0
        cost = 0.0
        for level in self.asks:
            if remaining <= 1e-12:
                break
            take = min(remaining, level.size)
            cost += take * level.price
            filled += take
            remaining -= take

        executable = remaining <= 1e-9
        avg_price = cost / filled if filled > 0 else None
        return FillEstimate(
            requested_shares=shares,
            filled_shares=filled,
            cost=cost,
            avg_price=avg_price,
            executable=executable,
        )
