from polyarb.models.event import GammaEvent
from polyarb.scanners.correlated_scanner import CorrelatedScanner
from tests.conftest import event_payload, market_payload


def test_correlated_time_monotonicity_detection(simple_books):
    event = GammaEvent.from_gamma(
        event_payload(
            "time",
            "Ceasefire by...?",
            [
                market_payload(
                    "early",
                    "March 31",
                    0.60,
                    "early-yes",
                    "early-no",
                    extra={"question": "Ceasefire by March 31, 2026?", "negRisk": False},
                ),
                market_payload(
                    "later",
                    "June 30",
                    0.55,
                    "later-yes",
                    "later-no",
                    extra={"question": "Ceasefire by June 30, 2026?", "negRisk": False},
                ),
            ],
            neg_risk=False,
        )
    )

    opportunities = CorrelatedScanner([100.0]).scan([event], simple_books)

    assert len(opportunities) == 1
    assert opportunities[0].type == "correlated-time"
    assert round(opportunities[0].theoretical["violation"], 4) == 0.05
    assert opportunities[0].execution_by_size["100"].executable is True


def test_correlated_threshold_up_detection(simple_books):
    event = GammaEvent.from_gamma(
        event_payload(
            "threshold-up",
            "Bitcoin above ___ on December 31?",
            [
                market_payload(
                    "easy",
                    "↑ 80,000",
                    0.50,
                    "easy-yes",
                    "easy-no",
                    extra={"question": "Will Bitcoin reach $80,000 by December 31, 2026?", "negRisk": False},
                ),
                market_payload(
                    "hard",
                    "↑ 90,000",
                    0.55,
                    "hard-yes",
                    "hard-no",
                    extra={"question": "Will Bitcoin reach $90,000 by December 31, 2026?", "negRisk": False},
                ),
            ],
            neg_risk=False,
        )
    )

    opportunities = CorrelatedScanner([100.0]).scan([event], simple_books)

    assert len(opportunities) == 1
    assert opportunities[0].type == "correlated-threshold"
    assert opportunities[0].markets[0].title == "↑ 80,000"
    assert opportunities[0].markets[1].title == "↑ 90,000"


def test_correlated_threshold_down_detection(simple_books):
    event = GammaEvent.from_gamma(
        event_payload(
            "threshold-down",
            "Bitcoin dip to ___ in April?",
            [
                market_payload(
                    "easy",
                    "↓ 80,000",
                    0.40,
                    "easy-yes",
                    "easy-no",
                    extra={"question": "Will Bitcoin dip to $80,000 in April?", "negRisk": False},
                ),
                market_payload(
                    "hard",
                    "↓ 70,000",
                    0.45,
                    "hard-yes",
                    "hard-no",
                    extra={"question": "Will Bitcoin dip to $70,000 in April?", "negRisk": False},
                ),
            ],
            neg_risk=False,
        )
    )

    opportunities = CorrelatedScanner([100.0]).scan([event], simple_books)

    assert len(opportunities) == 1
    assert opportunities[0].type == "correlated-threshold"
    assert opportunities[0].markets[0].title == "↓ 80,000"
    assert opportunities[0].markets[1].title == "↓ 70,000"


def test_presidential_path_implication_removed(simple_books):
    nominee = GammaEvent.from_gamma(
        event_payload(
            "nominee",
            "Democratic Presidential Nominee 2028",
            [market_payload("nominee-a", "Alice", 0.20, "easy-yes", "easy-no")],
            neg_risk=True,
        )
    )
    winner = GammaEvent.from_gamma(
        event_payload(
            "winner",
            "Presidential Election Winner 2028",
            [market_payload("winner-a", "Alice", 0.40, "hard-yes", "hard-no")],
            neg_risk=True,
        )
    )

    opportunities = CorrelatedScanner([100.0]).scan([nominee, winner], simple_books)

    assert opportunities == []


def test_range_bucket_temperature_markets_are_suppressed(simple_books):
    event = GammaEvent.from_gamma(
        event_payload(
            "range-weather",
            "Highest temperature in NYC on April 18?",
            [
                market_payload("easy", "60-61°F", 0.05, "easy-yes", "easy-no", extra={"question": "Will the highest temperature be between 60-61°F?", "negRisk": False}),
                market_payload("hard", "62-63°F", 0.20, "hard-yes", "hard-no", extra={"question": "Will the highest temperature be between 62-63°F?", "negRisk": False}),
            ],
            neg_risk=False,
        )
    )

    opportunities = CorrelatedScanner([100.0]).scan([event], simple_books)

    assert opportunities == []


def test_same_deadline_categorical_markets_are_not_time_links(simple_books):
    event = GammaEvent.from_gamma(
        event_payload(
            "countries",
            "Which countries will send warships by April 30?",
            [
                market_payload("fr", "France", 0.20, "easy-yes", "easy-no", extra={"question": "Will France send warships by April 30, 2026?", "negRisk": False}),
                market_payload("uk", "United Kingdom", 0.40, "hard-yes", "hard-no", extra={"question": "Will the United Kingdom send warships by April 30, 2026?", "negRisk": False}),
            ],
            neg_risk=False,
        )
    )

    opportunities = CorrelatedScanner([100.0]).scan([event], simple_books)

    assert opportunities == []


def test_resolution_source_mismatch_suppresses_correlated_link(simple_books):
    event = GammaEvent.from_gamma(
        event_payload(
            "source-mismatch",
            "Bitcoin above ___ on December 31?",
            [
                market_payload("easy", "↑ 80,000", 0.50, "easy-yes", "easy-no", extra={"question": "Will Bitcoin reach $80,000 by December 31, 2026?", "resolutionSource": "coinbase", "negRisk": False}),
                market_payload("hard", "↑ 90,000", 0.55, "hard-yes", "hard-no", extra={"question": "Will Bitcoin reach $90,000 by December 31, 2026?", "resolutionSource": "binance", "negRisk": False}),
            ],
            neg_risk=False,
        )
    )

    opportunities = CorrelatedScanner([100.0]).scan([event], simple_books)

    assert opportunities == []
