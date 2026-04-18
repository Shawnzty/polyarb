from polyarb.models.event import GammaEvent
from polyarb.models.orderbook import OrderBook
from polyarb.scanners.execution import estimate_basket_cost
from tests.conftest import book_payload, event_payload, market_payload


def test_post_fee_execution_cost_uses_level_price():
    market = GammaEvent.from_gamma(
        event_payload(
            "fees",
            "Fee Event",
            [
                market_payload(
                    "m-a",
                    "A",
                    0.49,
                    "a-yes",
                    extra={"feesEnabled": True, "feeSchedule": {"rate": 0.10}},
                )
            ],
        )
    ).markets[0]
    books = {
        "a-yes": OrderBook.from_clob(
            book_payload(
                "a-yes",
                asks=[{"price": "0.49", "size": "100"}],
                bids=[{"price": "0.48", "size": "100"}],
            )
        )
    }

    estimate = estimate_basket_cost([(market, market.yes_token_id, "Yes")], books, 100)

    assert estimate.gross_cost == 49
    assert round(estimate.fee_cost, 4) == 2.499
    assert round(estimate.net_cost, 4) == 51.499


def test_profitable_before_fees_can_be_net_negative_after_fees():
    event = GammaEvent.from_gamma(
        event_payload(
            "fee-neg",
            "Fee Neg Risk",
            [
                market_payload("m-a", "A", 0.49, "a-yes", extra={"feesEnabled": True, "feeSchedule": {"rate": 0.10}}),
                market_payload("m-b", "B", 0.49, "b-yes", extra={"feesEnabled": True, "feeSchedule": {"rate": 0.10}}),
            ],
        )
    )
    books = {
        "a-yes": OrderBook.from_clob(book_payload("a-yes", asks=[{"price": "0.495", "size": "100"}])),
        "b-yes": OrderBook.from_clob(book_payload("b-yes", asks=[{"price": "0.495", "size": "100"}])),
    }

    estimate = estimate_basket_cost(
        [(market, market.yes_token_id, "Yes") for market in event.markets],
        books,
        100,
    )

    assert estimate.gross_cost == 99
    assert estimate.net_cost > 100
    assert estimate.edge < 0
