from polyarb.models.event import GammaEvent
from polyarb.models.orderbook import OrderBook
from polyarb.scanners.correlated_scanner import CorrelatedScanner
from tests.conftest import book_payload, event_payload, market_payload


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


def test_correlated_gamma_violation_but_clob_package_does_not_pay(simple_books):
    # Gamma reports harder=0.55 > easier=0.50 (violation), but CLOB asks make
    # the (easier YES + harder NO) package cost > $1 after fees. CLOB-first
    # gating must reject.
    event = GammaEvent.from_gamma(
        event_payload(
            "gamma-only-violation",
            "Bitcoin above ___ on December 31?",
            [
                market_payload("easy", "↑ 80,000", 0.50, "ez-yes", "ez-no", extra={"question": "Will Bitcoin reach $80,000 by December 31, 2026?", "negRisk": False}),
                market_payload("hard", "↑ 90,000", 0.55, "hd-yes", "hd-no", extra={"question": "Will Bitcoin reach $90,000 by December 31, 2026?", "negRisk": False}),
            ],
            neg_risk=False,
        )
    )
    # CLOB: easier YES ask = 0.58, harder NO ask = 0.52 → package = 1.10, no arb.
    books = {
        "ez-yes": OrderBook.from_clob(book_payload("ez-yes", asks=[{"price": "0.58", "size": "1000"}], bids=[{"price": "0.57", "size": "1000"}])),
        "hd-no":  OrderBook.from_clob(book_payload("hd-no",  asks=[{"price": "0.52", "size": "1000"}], bids=[{"price": "0.51", "size": "1000"}])),
    }

    opportunities = CorrelatedScanner([100.0]).scan([event], books)

    assert opportunities == []


def test_correlated_gamma_no_violation_but_clob_package_pays(simple_books):
    # Gamma reports harder=0.49 < easier=0.50 (no apparent violation), but the
    # CLOB (easier YES + harder NO) package costs < $1 after fees. CLOB-first
    # gating must still emit the opportunity — Gamma is not the gate.
    event = GammaEvent.from_gamma(
        event_payload(
            "clob-only-violation",
            "Bitcoin above ___ on December 31?",
            [
                market_payload("easy", "↑ 80,000", 0.50, "ez-yes", "ez-no", extra={"question": "Will Bitcoin reach $80,000 by December 31, 2026?", "negRisk": False}),
                market_payload("hard", "↑ 90,000", 0.49, "hd-yes", "hd-no", extra={"question": "Will Bitcoin reach $90,000 by December 31, 2026?", "negRisk": False}),
            ],
            neg_risk=False,
        )
    )
    # CLOB: easier YES ask = 0.50, harder NO ask = 0.48 → package = 0.98, clean $0.02 edge.
    books = {
        "ez-yes": OrderBook.from_clob(book_payload("ez-yes", asks=[{"price": "0.50", "size": "1000"}], bids=[{"price": "0.49", "size": "1000"}])),
        "hd-no":  OrderBook.from_clob(book_payload("hd-no",  asks=[{"price": "0.48", "size": "1000"}], bids=[{"price": "0.47", "size": "1000"}])),
    }

    opportunities = CorrelatedScanner([100.0]).scan([event], books)

    assert len(opportunities) == 1
    assert opportunities[0].theoretical["price_source"] == "clob_best_ask_post_fee"


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
