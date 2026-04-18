from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class OpportunityMarket:
    id: str
    title: str
    yes_token_id: Optional[str]
    no_token_id: Optional[str]
    yes_price: Optional[float]
    no_price: Optional[float]
    volume: float
    liquidity: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "yes_token_id": self.yes_token_id,
            "no_token_id": self.no_token_id,
            "yes_price": self.yes_price,
            "no_price": self.no_price,
            "volume": self.volume,
            "liquidity": self.liquidity,
        }


@dataclass
class ExecutionEstimate:
    target_size: float
    executable: bool
    cost: Optional[float]
    payout: float
    edge: Optional[float]
    edge_pct: Optional[float]
    missing_legs: List[str] = field(default_factory=list)
    note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target_size": self.target_size,
            "executable": self.executable,
            "cost": self.cost,
            "payout": self.payout,
            "edge": self.edge,
            "edge_pct": self.edge_pct,
            "missing_legs": self.missing_legs,
            "note": self.note,
        }


@dataclass
class Opportunity:
    type: str
    title: str
    event: Dict[str, Any]
    markets: List[OpportunityMarket]
    theoretical: Dict[str, Any]
    execution_by_size: Dict[str, ExecutionEstimate]
    liquidity: Dict[str, float]
    confidence: float
    warnings: List[str]
    explanation: str
    score: float = 0.0
    rank: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rank": self.rank,
            "type": self.type,
            "score": self.score,
            "confidence": self.confidence,
            "event": self.event,
            "markets": [market.to_dict() for market in self.markets],
            "theoretical": self.theoretical,
            "execution_by_size": {
                size: estimate.to_dict() for size, estimate in self.execution_by_size.items()
            },
            "liquidity": self.liquidity,
            "warnings": self.warnings,
            "explanation": self.explanation,
        }
