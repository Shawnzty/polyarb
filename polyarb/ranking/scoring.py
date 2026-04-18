from __future__ import annotations

import math
from typing import Iterable, List

from polyarb.models.opportunity import Opportunity


WARNING_PENALTIES = {
    "missing-book": 25.0,
    "missing-price": 25.0,
    "missing-token": 25.0,
    "empty-ask-book": 25.0,
    "insufficient-depth": 20.0,
    "low-depth": 12.0,
    "wide-spread": 10.0,
    "low-liquidity": 10.0,
    "other-outcome": 15.0,
    "augmented-neg-risk": 15.0,
    "fees-enabled": 5.0,
    "logic-link-confidence": 5.0,
    "orderbook-disabled": 20.0,
    "not-accepting-orders": 20.0,
}


def score_opportunities(opportunities: Iterable[Opportunity]) -> List[Opportunity]:
    scored = list(opportunities)
    for opportunity in scored:
        opportunity.score = score_opportunity(opportunity)
    scored.sort(key=lambda item: item.score, reverse=True)
    for rank, opportunity in enumerate(scored, start=1):
        opportunity.rank = rank
    return scored


def score_opportunity(opportunity: Opportunity) -> float:
    executable_edges = [
        estimate.edge_pct
        for estimate in opportunity.execution_by_size.values()
        if estimate.executable and estimate.edge_pct is not None
    ]
    positive_executable_edge = max([edge for edge in executable_edges if edge > 0], default=0.0)
    executable_count = sum(1 for estimate in opportunity.execution_by_size.values() if estimate.executable)

    theoretical_edge = opportunity.theoretical.get("edge")
    theoretical_positive = max(float(theoretical_edge or 0.0), 0.0)
    volume = max(
        opportunity.liquidity.get("event_volume", 0.0),
        opportunity.liquidity.get("market_volume_sum", 0.0),
        0.0,
    )
    liquidity = max(
        opportunity.liquidity.get("event_liquidity", 0.0),
        opportunity.liquidity.get("market_liquidity_sum", 0.0),
        0.0,
    )
    liquidity_component = min(20.0, math.log10(max(volume + liquidity, 1.0)) * 2.0)

    score = (
        positive_executable_edge * 2000.0
        + theoretical_positive * 50.0
        + opportunity.confidence * 20.0
        + executable_count * 5.0
        + liquidity_component
    )

    if not executable_edges:
        score -= 50.0
    if opportunity.type == "neg-risk-overround":
        score -= 12.0
    if opportunity.type.startswith("correlated") and opportunity.confidence < 0.95:
        score -= (0.95 - opportunity.confidence) * 20.0

    for warning in opportunity.warnings:
        score -= WARNING_PENALTIES.get(warning, 0.0)

    return round(score, 4)
