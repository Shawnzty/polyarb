from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from polyarb.models.opportunity import Opportunity
from polyarb.streaming.diff import OpportunityDiff, opportunity_identity


class StateStore:
    """Append-only JSONL log of opportunity lifecycle events.

    One file, one record per line. Each record is self-describing
    (`kind` ∈ {"new", "changed", "closed", "scan"}) so tailing the file in a
    notebook or piping to Kafka is trivial. No schema migrations, no indexes —
    this is a research audit trail, not an OLTP store.
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path) if path else None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def record_scan(self, run_id: str, counts: Dict[str, int]) -> None:
        self._write({"kind": "scan", "run_id": run_id, "counts": counts})

    def record_diff(self, run_id: str, diff: OpportunityDiff) -> None:
        for opp in diff.new:
            self._write(
                {
                    "kind": "new",
                    "run_id": run_id,
                    "opportunity_id": opportunity_identity(opp),
                    "opportunity": opp.to_dict(),
                }
            )
        for prior, current in diff.changed:
            self._write(
                {
                    "kind": "changed",
                    "run_id": run_id,
                    "opportunity_id": opportunity_identity(current),
                    "prior_edge": _best_edge(prior),
                    "current_edge": _best_edge(current),
                    "opportunity": current.to_dict(),
                }
            )
        for opp in diff.closed:
            self._write(
                {
                    "kind": "closed",
                    "run_id": run_id,
                    "opportunity_id": opportunity_identity(opp),
                }
            )

    def records(self) -> List[Dict[str, Any]]:
        # Exposed for tests / replay tooling. The store is append-only so a full
        # read remains cheap until files grow to O(100k) records.
        if self.path is None or not self.path.exists():
            return []
        with self.path.open("r", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def _write(self, record: Dict[str, Any]) -> None:
        if self.path is None:
            return
        record = {"ts": datetime.now(timezone.utc).isoformat(), **record}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")


def _best_edge(opportunity: Opportunity) -> Optional[float]:
    edges = [
        estimate.edge
        for estimate in opportunity.execution_by_size.values()
        if estimate.executable and estimate.edge is not None
    ]
    return max(edges) if edges else None
