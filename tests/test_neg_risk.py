from polyarb.models.event import GammaEvent
from polyarb.models.orderbook import OrderBook
from polyarb.scanners.neg_risk_scanner import NegRiskScanner
from tests.conftest import book_payload, event_payload, market_payload


def test_neg_risk_underround_and_execution(neg_risk_event, simple_books):
    opportunities = NegRiskScanner([100.0, 2500.0]).scan([neg_risk_event], simple_books)

    assert len(opportunities) == 1
    opportunity = opportunities[0]
    assert opportunity.type == "neg-risk-underround"
    assert opportunity.theoretical["sum_yes"] == 0.99
    assert round(opportunity.theoretical["residual"], 4) == 0.01
    assert opportunity.execution_by_size["100"].executable is True
    assert opportunity.execution_by_size["100"].cost == 99
    assert opportunity.execution_by_size["100"].edge == 1
    assert opportunity.execution_by_size["2500"].executable is False
    assert "other-outcome" in opportunity.warnings
    assert "augmented-neg-risk" not in opportunity.warnings


def test_neg_risk_augmented_event_is_blocked(simple_books):
    # Augmented neg-risk events can grow their outcome set mid-trade.
    # Buying all current YES tokens is no longer a guaranteed $1 payout.
    event = GammaEvent.from_gamma(
        event_payload(
            "fixture-aug",
            "Augmented Event",
            [
                market_payload("m-a", "A", 0.30, "a-yes", "a-no"),
                market_payload("m-b", "B", 0.30, "b-yes", "b-no"),
            ],
            neg_risk_augmented=True,
        )
    )

    scanner = NegRiskScanner([100.0])
    opportunities = scanner.scan([event], simple_books)

    assert opportunities == []
    assert scanner.blocked_events == [{"event_id": "fixture-aug", "reason": "augmented-neg-risk"}]


def test_neg_risk_inactive_child_is_blocked(simple_books):
    # An inactive/closed child while the event is still open breaks MECE:
    # the dropped candidate can still resolve YES and zero out the rest of the basket.
    event = GammaEvent.from_gamma(
        event_payload(
            "fixture-inactive",
            "Inactive Child Event",
            [
                market_payload("m-a", "A", 0.30, "a-yes", "a-no"),
                market_payload("m-b", "B", 0.30, "b-yes", "b-no"),
                market_payload("m-c", "C", 0.30, "c-yes", "c-no", active=False),
            ],
        )
    )

    scanner = NegRiskScanner([100.0])
    opportunities = scanner.scan([event], simple_books)

    assert opportunities == []
    assert scanner.blocked_events == [
        {"event_id": "fixture-inactive", "reason": "inactive-child-market"}
    ]


def test_neg_risk_overround_detection():
    event = GammaEvent.from_gamma(
        event_payload(
            "fixture-over",
            "Fixture Overround",
            [
                market_payload("m-a", "A", 0.40, "a-yes", "a-no"),
                market_payload("m-b", "B", 0.40, "b-yes", "b-no"),
                market_payload("m-c", "C", 0.30, "other-yes", "other-no"),
            ],
        )
    )
    books = {
        "a-yes": OrderBook.from_clob(book_payload("a-yes", asks=[{"price": "0.41", "size": "1000"}], bids=[{"price": "0.40", "size": "1000"}])),
        "b-yes": OrderBook.from_clob(book_payload("b-yes", asks=[{"price": "0.41", "size": "1000"}], bids=[{"price": "0.40", "size": "1000"}])),
        "other-yes": OrderBook.from_clob(book_payload("other-yes", asks=[{"price": "0.31", "size": "1000"}], bids=[{"price": "0.30", "size": "1000"}])),
    }

    opportunities = NegRiskScanner([100.0]).scan([event], books)

    assert len(opportunities) == 1
    assert opportunities[0].type == "neg-risk-overround"
    assert round(opportunities[0].theoretical["sum_yes"], 4) == 1.1
    assert round(opportunities[0].theoretical["residual"], 4) == -0.1


def test_neg_risk_missing_gamma_price_is_warned_but_clob_can_classify(simple_books):
    event = GammaEvent.from_gamma(
        event_payload(
            "fixture-missing-other",
            "Fixture Missing Other",
            [
                market_payload("m-a", "A", 0.30, "a-yes", "a-no"),
                market_payload("m-b", "B", 0.30, "b-yes", "b-no"),
                market_payload(
                    "m-other",
                    "Other",
                    0.0,
                    "other-yes",
                    extra={
                        "question": "Will Any Other Candidate win?",
                        "outcomePrices": None,
                        "negRiskOther": True,
                    },
                ),
            ],
        )
    )

    opportunities = NegRiskScanner([100.0]).scan([event], simple_books)

    assert len(opportunities) == 1
    assert opportunities[0].execution_by_size["100"].executable is True
    assert "other-outcome" in opportunities[0].warnings
    assert "missing-price" in opportunities[0].warnings


def test_neg_risk_inactive_other_is_blocked(simple_books):
    # Previously this case emitted an opportunity with only an "other-outcome" warning.
    # After the MECE gate, inactive children hard-block: the dropped candidate can still
    # resolve YES and zero every other leg in the basket.
    event = GammaEvent.from_gamma(
        event_payload(
            "fixture-inactive-other",
            "Fixture Inactive Other",
            [
                market_payload("m-a", "A", 0.30, "a-yes", "a-no"),
                market_payload("m-b", "B", 0.30, "b-yes", "b-no"),
                market_payload(
                    "m-other",
                    "Other",
                    0.39,
                    "other-yes",
                    "other-no",
                    active=False,
                    extra={"question": "Will Any Other Candidate win?", "negRiskOther": True},
                ),
            ],
        )
    )

    scanner = NegRiskScanner([100.0])
    opportunities = scanner.scan([event], simple_books)

    assert opportunities == []
    assert scanner.blocked_events == [
        {"event_id": "fixture-inactive-other", "reason": "inactive-child-market"}
    ]


def test_neg_risk_stale_book_is_blocked(neg_risk_event):
    # Books older than max_book_age_s must be rejected: a stale price is a bet that
    # the resting levels haven't already been picked off.
    now = 1_700_000_000.0
    stale_ms = str(int((now - 30.0) * 1000))
    books = {
        token_id: OrderBook.from_clob(
            book_payload(
                token_id,
                asks=[{"price": f"{min(price + 0.02, 0.99):.2f}", "size": "1000"},
                      {"price": f"{price:.2f}", "size": "1000"}],
                bids=[{"price": f"{max(price - 0.01, 0.01):.2f}", "size": "1000"}],
                timestamp=stale_ms,
            )
        )
        for token_id, price in {
            "a-yes": 0.30, "b-yes": 0.30, "other-yes": 0.39,
        }.items()
    }

    scanner = NegRiskScanner([100.0], max_book_age_s=10.0, now=now)
    opportunities = scanner.scan([neg_risk_event], books)

    assert opportunities == []
    assert scanner.blocked_events == [{"event_id": "fixture-neg", "reason": "stale-book"}]


def test_neg_risk_theoretical_uses_clob_asks_not_gamma_prices():
    event = GammaEvent.from_gamma(
        event_payload(
            "clob-theory",
            "CLOB Theory",
            [
                market_payload("m-a", "A", 0.90, "a-yes", "a-no"),
                market_payload("m-b", "B", 0.90, "b-yes", "b-no"),
            ],
        )
    )
    books = {
        "a-yes": OrderBook.from_clob(book_payload("a-yes", asks=[{"price": "0.40", "size": "1000"}], bids=[{"price": "0.39", "size": "1000"}])),
        "b-yes": OrderBook.from_clob(book_payload("b-yes", asks=[{"price": "0.40", "size": "1000"}], bids=[{"price": "0.39", "size": "1000"}])),
    }

    opportunities = NegRiskScanner([100.0]).scan([event], books)

    assert len(opportunities) == 1
    assert opportunities[0].type == "neg-risk-underround"
    assert opportunities[0].theoretical["price_source"] == "clob_best_ask_post_fee"
    assert round(opportunities[0].theoretical["sum_yes"], 4) == 0.8
