import io
import json

from polyarb.cli import main
from tests.conftest import FakeClobClient, FakeGammaClient


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
