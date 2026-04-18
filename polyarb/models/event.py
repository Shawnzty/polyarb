from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from polyarb.models.market import GammaMarket
from polyarb.models.parsing import as_bool, as_float, clean_text


@dataclass(frozen=True)
class GammaEvent:
    id: str
    title: str
    slug: str
    description: str
    resolution_source: str
    active: bool
    closed: bool
    neg_risk: bool
    neg_risk_augmented: bool
    enable_neg_risk: bool
    show_all_outcomes: bool
    volume: float
    volume24hr: float
    liquidity: float
    end_date: str
    markets: List[GammaMarket]
    raw: Dict[str, Any] = field(default_factory=dict, compare=False)

    @classmethod
    def from_gamma(cls, payload: Dict[str, Any]) -> "GammaEvent":
        markets_payload = payload.get("markets") or []
        markets = [
            GammaMarket.from_gamma(item)
            for item in markets_payload
            if isinstance(item, dict)
        ]
        return cls(
            id=clean_text(payload.get("id")),
            title=clean_text(payload.get("title")),
            slug=clean_text(payload.get("slug")),
            description=clean_text(payload.get("description")),
            resolution_source=clean_text(payload.get("resolutionSource")),
            active=as_bool(payload.get("active")),
            closed=as_bool(payload.get("closed")),
            neg_risk=as_bool(payload.get("negRisk")),
            neg_risk_augmented=as_bool(payload.get("negRiskAugmented")),
            enable_neg_risk=as_bool(payload.get("enableNegRisk")),
            show_all_outcomes=as_bool(payload.get("showAllOutcomes")),
            volume=as_float(payload.get("volume")),
            volume24hr=as_float(payload.get("volume24hr")),
            liquidity=as_float(payload.get("liquidity")),
            end_date=clean_text(payload.get("endDate")),
            markets=markets,
            raw=payload,
        )
