from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from polyarb.models.parsing import (
    as_bool,
    as_float,
    as_optional_float,
    clean_text,
    parse_json_dict,
    parse_json_list,
)


@dataclass(frozen=True)
class GammaMarket:
    id: str
    question: str
    slug: str
    description: str
    end_date: str
    resolution_source: str
    group_item_title: str
    outcomes: List[str]
    outcome_prices: List[float]
    clob_token_ids: List[str]
    active: bool
    closed: bool
    enable_order_book: bool
    accepting_orders: bool
    neg_risk: bool
    neg_risk_other: bool
    fees_enabled: bool
    fee_schedule: Dict[str, Any]
    maker_base_fee: Optional[float]
    taker_base_fee: Optional[float]
    fee_type: str
    volume: float
    volume24hr: float
    liquidity: float
    spread: Optional[float] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    raw: Dict[str, Any] = field(default_factory=dict, compare=False)

    @classmethod
    def from_gamma(cls, payload: Dict[str, Any]) -> "GammaMarket":
        outcomes = [str(item) for item in parse_json_list(payload.get("outcomes"))]
        outcome_prices = [as_float(item) for item in parse_json_list(payload.get("outcomePrices"))]
        clob_token_ids = [str(item) for item in parse_json_list(payload.get("clobTokenIds"))]

        return cls(
            id=clean_text(payload.get("id")),
            question=clean_text(payload.get("question")),
            slug=clean_text(payload.get("slug")),
            description=clean_text(payload.get("description")),
            end_date=clean_text(payload.get("endDate")),
            resolution_source=clean_text(payload.get("resolutionSource")),
            group_item_title=clean_text(payload.get("groupItemTitle")),
            outcomes=outcomes,
            outcome_prices=outcome_prices,
            clob_token_ids=clob_token_ids,
            active=as_bool(payload.get("active")),
            closed=as_bool(payload.get("closed")),
            enable_order_book=as_bool(payload.get("enableOrderBook")),
            accepting_orders=as_bool(payload.get("acceptingOrders"), default=True),
            neg_risk=as_bool(payload.get("negRisk")),
            neg_risk_other=as_bool(payload.get("negRiskOther")),
            fees_enabled=as_bool(payload.get("feesEnabled")),
            fee_schedule=parse_json_dict(payload.get("feeSchedule")),
            maker_base_fee=as_optional_float(payload.get("makerBaseFee")),
            taker_base_fee=as_optional_float(payload.get("takerBaseFee")),
            fee_type=clean_text(payload.get("feeType")),
            volume=as_float(payload.get("volumeNum"), as_float(payload.get("volume"))),
            volume24hr=as_float(payload.get("volume24hrClob"), as_float(payload.get("volume24hr"))),
            liquidity=as_float(payload.get("liquidityNum"), as_float(payload.get("liquidity"))),
            spread=as_optional_float(payload.get("spread")),
            best_bid=as_optional_float(payload.get("bestBid")),
            best_ask=as_optional_float(payload.get("bestAsk")),
            raw=payload,
        )

    @property
    def display_title(self) -> str:
        return self.group_item_title or self.question or self.id

    @property
    def yes_token_id(self) -> Optional[str]:
        index = self._outcome_index("yes")
        if index is None:
            index = 0
        return self.clob_token_ids[index] if index < len(self.clob_token_ids) else None

    @property
    def no_token_id(self) -> Optional[str]:
        index = self._outcome_index("no")
        if index is None:
            index = 1
        return self.clob_token_ids[index] if index < len(self.clob_token_ids) else None

    @property
    def yes_price(self) -> Optional[float]:
        index = self._outcome_index("yes")
        if index is None:
            index = 0
        return self.outcome_prices[index] if index < len(self.outcome_prices) else None

    @property
    def no_price(self) -> Optional[float]:
        index = self._outcome_index("no")
        if index is None:
            index = 1
        return self.outcome_prices[index] if index < len(self.outcome_prices) else None

    @property
    def fee_rate(self) -> Optional[float]:
        rate = self.fee_schedule.get("rate")
        if rate is None:
            return None
        try:
            return float(rate)
        except (TypeError, ValueError):
            return None

    def _outcome_index(self, label: str) -> Optional[int]:
        for index, outcome in enumerate(self.outcomes):
            if outcome.strip().lower() == label:
                return index
        return None
