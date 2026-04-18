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
    assert "augmented-neg-risk" in opportunity.warnings


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


def test_neg_risk_inactive_other_still_warns(simple_books):
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

    opportunities = NegRiskScanner([100.0]).scan([event], simple_books)

    assert len(opportunities) == 1
    assert opportunities[0].execution_by_size["100"].executable is True
    assert "other-outcome" in opportunities[0].warnings
    assert "missing-price" not in opportunities[0].warnings


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
