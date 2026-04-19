from argparse import Namespace
from datetime import datetime, timedelta, timezone

from polyarb.cli import run_scan
from polyarb.config import load_config
from polyarb.models.event import GammaEvent
from polyarb.timeutils import filter_events_by_horizon
from tests.conftest import FakeClobClient, FakeGammaClient, event_payload, market_payload


def dated_event(event_id, hours_from_now):
    end = (datetime.now(timezone.utc) + timedelta(hours=hours_from_now)).isoformat()
    return GammaEvent.from_gamma(
        event_payload(
            event_id,
            f"Event {event_id}",
            [
                market_payload("m-a", "A", 0.30, "a-yes", "a-no", extra={"endDate": end}),
                market_payload("m-b", "B", 0.30, "b-yes", "b-no", extra={"endDate": end}),
                market_payload("m-c", "C", 0.30, "other-yes", "other-no", extra={"endDate": end}),
            ],
            extra={"endDate": end},
        )
    )


def test_horizon_filter_defaults_to_near_term():
    now = datetime(2026, 4, 18, tzinfo=timezone.utc)
    near_end = (now + timedelta(hours=2)).isoformat()
    far_end = (now + timedelta(days=2)).isoformat()
    near = GammaEvent.from_gamma(
        event_payload("near", "Near", [market_payload("near-a", "A", 0.3, "a-yes", extra={"endDate": near_end})], extra={"endDate": near_end})
    )
    far = GammaEvent.from_gamma(
        event_payload("far", "Far", [market_payload("far-a", "A", 0.3, "b-yes", extra={"endDate": far_end})], extra={"endDate": far_end})
    )

    filtered = filter_events_by_horizon([near, far], 24, now=now)

    assert [event.id for event in filtered] == ["near"]


def test_config_loading_and_cli_override_precedence(tmp_path, simple_books):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "scan:\n  within_hours: 1\n  target_sizes: [100]\nrisk:\n  leg_risk_bps_per_extra_leg: 25\n",
        encoding="utf-8",
    )
    event = dated_event("within-three-hours", 2)
    base_args = {
        "command": "scan",
        "json": False,
        "config": str(config_path),
        "min_volume": 0.0,
        "target_sizes": None,
        "limit_events": 1,
        "max_results": 25,
        "neg_risk_only": False,
        "correlated_only": False,
        "rank_by": "apy",
    }

    report = run_scan(
        Namespace(**base_args, within_hours=None, all_horizons=False),
        gamma_client=FakeGammaClient([event]),
        clob_client=FakeClobClient(simple_books),
    )
    override_report = run_scan(
        Namespace(**base_args, within_hours=3, all_horizons=False),
        gamma_client=FakeGammaClient([event]),
        clob_client=FakeClobClient(simple_books),
    )

    assert load_config(str(config_path))["scan"]["within_hours"] == 1
    assert report["source_counts"]["events"] == 0
    assert override_report["source_counts"]["events"] == 1
