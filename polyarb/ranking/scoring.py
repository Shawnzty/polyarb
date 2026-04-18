from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from polyarb.models.opportunity import ExecutionEstimate, Opportunity


DEFAULT_RISK_CONFIG = {
    "leg_risk_bps_per_extra_leg": 25.0,
    "other_outcome_penalty_bps": 100.0,
    "augmented_neg_risk_penalty_bps": 50.0,
}

STALE_WARNING_BPS = {
    "missing-book": 100.0,
    "empty-ask-book": 100.0,
    "insufficient-depth": 50.0,
    "wide-spread": 25.0,
    "not-accepting-orders": 100.0,
    "orderbook-disabled": 100.0,
}


def score_opportunities(
    opportunities: Iterable[Opportunity],
    risk_config: Optional[Dict[str, float]] = None,
) -> List[Opportunity]:
    scored = list(opportunities)
    config = {**DEFAULT_RISK_CONFIG, **(risk_config or {})}
    for opportunity in scored:
        opportunity.score = score_opportunity(opportunity, config)
    scored.sort(key=lambda item: item.score, reverse=True)
    for rank, opportunity in enumerate(scored, start=1):
        opportunity.rank = rank
    return scored


def score_opportunity(opportunity: Opportunity, risk_config: Dict[str, float]) -> float:
    best = best_executable_estimate(opportunity)
    if best is None:
        return round(-warning_cost(opportunity, 100.0, risk_config), 4)

    edge = best.edge or 0.0
    leg_risk = max(0, best.leg_count - 1) * risk_config["leg_risk_bps_per_extra_leg"] / 10_000.0 * best.payout
    rule_risk = warning_cost(opportunity, best.payout, risk_config)
    score = edge * opportunity.confidence - leg_risk - rule_risk
    if opportunity.type == "neg-risk-overround":
        score -= 0.01 * best.payout
    return round(score, 4)


def best_executable_estimate(opportunity: Opportunity) -> Optional[ExecutionEstimate]:
    estimates = [
        estimate
        for estimate in opportunity.execution_by_size.values()
        if estimate.executable and estimate.edge is not None
    ]
    if not estimates:
        return None
    return max(estimates, key=lambda estimate: estimate.edge or float("-inf"))


def warning_cost(
    opportunity: Opportunity,
    payout: float,
    risk_config: Dict[str, float],
) -> float:
    bps = 0.0
    if "other-outcome" in opportunity.warnings:
        bps += risk_config["other_outcome_penalty_bps"]
    if "augmented-neg-risk" in opportunity.warnings:
        bps += risk_config["augmented_neg_risk_penalty_bps"]
    for warning, warning_bps in STALE_WARNING_BPS.items():
        if warning in opportunity.warnings:
            bps += warning_bps
    return bps / 10_000.0 * payout
