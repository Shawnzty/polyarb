from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from polyarb.models.opportunity import ExecutionEstimate, Opportunity


def opportunity_identity(opportunity: Opportunity) -> str:
    # Stable identity across polls. The (event_id, type, sorted market ids) tuple
    # uniquely identifies a logical opportunity: same event, same basket pattern,
    # same constituent markets. Rank and edge move turn-by-turn; identity must not.
    event_id = str(opportunity.event.get("id", ""))
    market_ids = sorted(str(market.id) for market in opportunity.markets)
    return f"{event_id}|{opportunity.type}|{','.join(market_ids)}"


def _best_edge(execution_by_size: Dict[str, ExecutionEstimate]) -> Optional[float]:
    executable = [e.edge for e in execution_by_size.values() if e.executable and e.edge is not None]
    return max(executable) if executable else None


@dataclass(frozen=True)
class OpportunityDiff:
    new: List[Opportunity]
    changed: List[Tuple[Opportunity, Opportunity]]
    closed: List[Opportunity]

    def is_empty(self) -> bool:
        return not (self.new or self.changed or self.closed)

    def summary(self) -> Dict[str, int]:
        return {"new": len(self.new), "changed": len(self.changed), "closed": len(self.closed)}


def diff_opportunities(
    previous: Iterable[Opportunity],
    current: Iterable[Opportunity],
    *,
    edge_change_threshold: float = 0.5,
) -> OpportunityDiff:
    # `edge_change_threshold` is in dollars of executable edge — noise floor
    # below which we suppress CHANGED events to avoid spamming the output
    # channel on every 1-cent book wobble. 50¢ is a reasonable default for
    # the current target-size scale ($100..$1000).
    prev_by_id = {opportunity_identity(opp): opp for opp in previous}
    curr_by_id = {opportunity_identity(opp): opp for opp in current}

    new = [curr_by_id[key] for key in curr_by_id.keys() - prev_by_id.keys()]
    closed = [prev_by_id[key] for key in prev_by_id.keys() - curr_by_id.keys()]

    changed: List[Tuple[Opportunity, Opportunity]] = []
    for key in prev_by_id.keys() & curr_by_id.keys():
        prior = prev_by_id[key]
        current_opp = curr_by_id[key]
        prior_edge = _best_edge(prior.execution_by_size) or 0.0
        current_edge = _best_edge(current_opp.execution_by_size) or 0.0
        prior_exec = any(e.executable for e in prior.execution_by_size.values())
        current_exec = any(e.executable for e in current_opp.execution_by_size.values())
        if prior_exec != current_exec or abs(current_edge - prior_edge) >= edge_change_threshold:
            changed.append((prior, current_opp))

    return OpportunityDiff(new=new, changed=changed, closed=closed)
