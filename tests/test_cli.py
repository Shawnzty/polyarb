import io
import json
from typing import Dict, List, Sequence

from polyarb.cli import main
from polyarb.models.event import GammaEvent
from polyarb.models.orderbook import OrderBook
from tests.conftest import FakeClobClient, FakeGammaClient


class _AsyncGammaAdapter:
    def __init__(self, events: List[GammaEvent]) -> None:
        self.events = events

    async def get_events(self, limit_events: int, min_volume: float) -> List[GammaEvent]:
        return [e for e in self.events if e.volume >= min_volume][:limit_events]


class _AsyncClobAdapter:
    def __init__(self, books: Dict[str, OrderBook]) -> None:
        self.books = books

    async def get_books(self, token_ids: Sequence[str]) -> Dict[str, OrderBook]:
        return {t: self.books[t] for t in token_ids if t in self.books}

    async def get_fee_rates(self, token_ids: Sequence[str]) -> Dict[str, float]:
        return {}


def test_cli_json_smoke(neg_risk_event, simple_books):
    stdout = io.StringIO()

    code = main(
        ["scan", "--json", "--limit-events", "1", "--target-sizes", "100", "--all-horizons"],
        gamma_client=FakeGammaClient([neg_risk_event]),
        clob_client=FakeClobClient(simple_books),
        stdout=stdout,
    )

    assert code == 0
    report = json.loads(stdout.getvalue())
    assert report["source_counts"]["events"] == 1
    assert report["opportunities"][0]["type"] == "neg-risk-underround"


def test_cli_human_smoke(neg_risk_event, simple_books):
    stdout = io.StringIO()

    code = main(
        ["scan", "--limit-events", "1", "--target-sizes", "100", "--all-horizons"],
        gamma_client=FakeGammaClient([neg_risk_event]),
        clob_client=FakeClobClient(simple_books),
        stdout=stdout,
    )

    assert code == 0
    output = stdout.getvalue()
    assert "Polyarb research scan" in output
    assert "neg-risk-underround" in output


def test_cli_watch_once_matches_scan_on_frozen_universe(neg_risk_event, simple_books):
    # Plan verification #7: `watch --once` returns identical opportunities to
    # `scan` on the same frozen universe. Both go through the same scoring
    # pipeline; the watch path wraps it with bootstrap + diff emission.
    common_args = ["--json", "--limit-events", "1", "--target-sizes", "100", "--all-horizons"]

    scan_stdout = io.StringIO()
    main(
        ["scan", *common_args],
        gamma_client=FakeGammaClient([neg_risk_event]),
        clob_client=FakeClobClient(simple_books),
        stdout=scan_stdout,
    )
    scan_report = json.loads(scan_stdout.getvalue())

    watch_stdout = io.StringIO()
    main(
        ["watch", "--once", *common_args],
        events_source=_AsyncGammaAdapter([neg_risk_event]),
        books_source=_AsyncClobAdapter(simple_books),
        stdout=watch_stdout,
    )
    watch_report = json.loads(watch_stdout.getvalue())

    scan_ids = [o["event"]["id"] + "|" + o["type"] for o in scan_report["opportunities"]]
    watch_ids = [o["event"]["id"] + "|" + o["type"] for o in watch_report["opportunities"]]
    assert scan_ids == watch_ids
    assert [o["score"] for o in scan_report["opportunities"]] == [
        o["score"] for o in watch_report["opportunities"]
    ]
    # First watch pass: every opportunity is NEW.
    assert len(watch_report["diff"]["new"]) == len(watch_report["opportunities"])
