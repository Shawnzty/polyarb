from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from polyarb.models.opportunity import ExecutionEstimate, Opportunity
from polyarb.timeutils import parse_datetime


DEFAULT_RISK_CONFIG = {
    "leg_risk_bps_per_extra_leg": 25.0,
    "other_outcome_penalty_bps": 100.0,
    "augmented_neg_risk_penalty_bps": 50.0,
    "neg_risk_overround_penalty_bps": 100.0,
}

STALE_WARNING_BPS = {
    "missing-book": 100.0,
    "empty-ask-book": 100.0,
    "insufficient-depth": 50.0,
    "wide-spread": 25.0,
    "not-accepting-orders": 100.0,
    "orderbook-disabled": 100.0,
    "atomic-risk": 100.0,
}

HOURS_PER_YEAR = 8760.0
# Floor the time-to-resolution used in APY computation. Without it, a market
# resolving in ten minutes that prints a 10bp noise tick would inflate to
# astronomical APY and dominate the ranking. 1h is roughly "the shortest
# horizon over which we can actually deploy capital".
MIN_HOURS_FLOOR = 1.0
# Fallback horizon when no end-date is attached to any leg. Defaulting long
# (1 year) keeps APY conservative for fixture/test paths; real opportunities
# always carry a resolution date.
DEFAULT_HOURS_FALLBACK = HOURS_PER_YEAR

RANK_BY_CHOICES = ("apy", "edge_pct", "edge_dollar")


def score_opportunities(
    opportunities: Iterable[Opportunity],
    risk_config: Optional[Dict[str, float]] = None,
    rank_by: str = "apy",
    now: Optional[datetime] = None,
) -> List[Opportunity]:
    if rank_by not in RANK_BY_CHOICES:
        raise ValueError(f"rank_by must be one of {RANK_BY_CHOICES}")
    scored = list(opportunities)
    config = {**DEFAULT_RISK_CONFIG, **(risk_config or {})}
    now = now or datetime.now(timezone.utc)
    for opportunity in scored:
        _populate_metrics(opportunity, rank_by, now)
        opportunity.score = score_opportunity(opportunity, config, rank_by)
    scored.sort(
        key=lambda item: (
            item.score,
            -len(item.warnings),
            -_leg_count_or_inf(item),
            _best_depth(item),
        ),
        reverse=True,
    )
    for rank, opportunity in enumerate(scored, start=1):
        opportunity.rank = rank
    return scored


def score_opportunity(
    opportunity: Opportunity,
    risk_config: Dict[str, float],
    rank_by: str = "apy",
) -> float:
    best = best_executable_estimate(opportunity, rank_by=rank_by)
    if best is None:
        # No executable leg — penalise by the warning drag in dollars. Kept
        # legacy: ensures non-executable candidates sort below any real
        # executable one.
        return round(-warning_cost_dollars(opportunity, 100.0, risk_config), 6)

    edge = best.edge or 0.0
    payout = best.payout
    leg_risk_bps = max(0, best.leg_count - 1) * risk_config["leg_risk_bps_per_extra_leg"]
    warning_bps = warning_cost_bps(opportunity, risk_config)
    total_bps = leg_risk_bps + warning_bps

    if rank_by == "edge_dollar":
        leg_risk = leg_risk_bps / 10_000.0 * payout
        rule_risk = warning_bps / 10_000.0 * payout
        score = edge * opportunity.confidence - leg_risk - rule_risk
        return round(score, 6)

    horizon_h = opportunity.time_to_res_h or DEFAULT_HOURS_FALLBACK
    edge_pct = opportunity.edge_pct or 0.0
    time_factor = HOURS_PER_YEAR / max(MIN_HOURS_FLOOR, horizon_h)
    risk_drag_rate = total_bps / 10_000.0 * time_factor

    if rank_by == "edge_pct":
        per_trade_drag = total_bps / 10_000.0
        return round(edge_pct * opportunity.confidence - per_trade_drag, 6)

    # Default: APY.
    apy = edge_pct * time_factor
    return round(apy * opportunity.confidence - risk_drag_rate, 6)


def best_executable_estimate(
    opportunity: Opportunity,
    rank_by: str = "apy",
) -> Optional[ExecutionEstimate]:
    candidates = [
        estimate
        for estimate in opportunity.execution_by_size.values()
        if estimate.executable and estimate.edge is not None
    ]
    if not candidates:
        return None
    if rank_by == "edge_dollar":
        return max(candidates, key=lambda e: e.edge or float("-inf"))
    # APY / edge_pct: pick the size with the highest per-$ return. For a fixed
    # book depth, edge_pct is monotone non-increasing in size (later fills walk
    # deeper into the ask stack), so the smallest executable size usually wins.
    return max(candidates, key=lambda e: e.edge_pct or float("-inf"))


def warning_cost_bps(opportunity: Opportunity, risk_config: Dict[str, float]) -> float:
    bps = 0.0
    if "other-outcome" in opportunity.warnings:
        bps += risk_config["other_outcome_penalty_bps"]
    if "augmented-neg-risk" in opportunity.warnings:
        bps += risk_config["augmented_neg_risk_penalty_bps"]
    for warning, warning_bps in STALE_WARNING_BPS.items():
        if warning in opportunity.warnings:
            bps += warning_bps
    if opportunity.type == "neg-risk-overround":
        bps += risk_config["neg_risk_overround_penalty_bps"]
    return bps


def warning_cost_dollars(
    opportunity: Opportunity,
    payout: float,
    risk_config: Dict[str, float],
) -> float:
    return warning_cost_bps(opportunity, risk_config) / 10_000.0 * payout


def warning_cost(
    opportunity: Opportunity,
    payout: float,
    risk_config: Dict[str, float],
) -> float:
    # Preserved for callers pinned to the old dollar-amount API.
    return warning_cost_dollars(opportunity, payout, risk_config)


def _populate_metrics(opportunity: Opportunity, rank_by: str, now: datetime) -> None:
    best = best_executable_estimate(opportunity, rank_by=rank_by)
    if best is None or not best.net_cost or best.net_cost <= 0:
        opportunity.capital_at_risk = None
        opportunity.edge_pct = None
        opportunity.time_to_res_h = _time_to_res_hours(opportunity, now)
        opportunity.apy = None
        return

    opportunity.capital_at_risk = best.net_cost
    opportunity.edge_pct = (best.edge or 0.0) / best.net_cost
    opportunity.time_to_res_h = _time_to_res_hours(opportunity, now)
    if opportunity.time_to_res_h is None:
        opportunity.apy = None
    else:
        time_factor = HOURS_PER_YEAR / max(MIN_HOURS_FLOOR, opportunity.time_to_res_h)
        opportunity.apy = opportunity.edge_pct * time_factor


def _time_to_res_hours(opportunity: Opportunity, now: datetime) -> Optional[float]:
    latest: Optional[datetime] = None
    for market in opportunity.markets:
        end = parse_datetime(market.end_date)
        if end is None:
            continue
        if latest is None or end > latest:
            latest = end
    if latest is None:
        return None
    delta = (latest - now).total_seconds() / 3600.0
    return max(0.0, delta)


def _leg_count_or_inf(opportunity: Opportunity) -> int:
    best = best_executable_estimate(opportunity)
    return best.leg_count if best is not None else 10_000


def _best_depth(opportunity: Opportunity) -> float:
    best = best_executable_estimate(opportunity)
    return best.max_executable_size if best is not None else 0.0
