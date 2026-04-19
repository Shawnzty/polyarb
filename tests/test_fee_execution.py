import pytest

from polyarb.models.event import GammaEvent
from polyarb.models.orderbook import OrderBook
from polyarb.scanners.execution import estimate_basket_cost
from tests.conftest import book_payload, event_payload, market_payload


@pytest.mark.parametrize(
    "price, rate, shares, expected_fee",
    [
        (0.50, 0.04, 100, 100 * 0.04 * 0.50 * 0.50),
        (0.30, 0.04, 100, 100 * 0.04 * 0.30 * 0.70),
        (0.90, 0.072, 100, 100 * 0.072 * 0.90 * 0.10),
        (0.01, 0.05, 1000, 1000 * 0.05 * 0.01 * 0.99),
    ],
)
def test_fee_formula_matches_polymarket_docs(price, rate, shares, expected_fee):
    """Pin fee to the Polymarket formula: fee = C * feeRate * p * (1 - p).

    Authoritative source: https://docs.polymarket.com/trading/fees
    """
    book = OrderBook.from_clob(
        book_payload(
            "pin-yes",
            asks=[{"price": f"{price}", "size": str(shares)}],
        )
    )
    fill = book.buy_shares(shares, fee_rate=rate)
    assert fill.executable
    assert fill.fee_cost == pytest.approx(expected_fee, rel=1e-9)


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


def test_basket_atomic_risk_when_target_exceeds_shallowest_leg():
    # Leg A has 1000 depth, leg B has 100 depth. Target 500 cannot clear atomically:
    # if A fills and B doesn't, we hold a naked A token.
    event = GammaEvent.from_gamma(
        event_payload(
            "atomic",
            "Atomic Basket",
            [
                market_payload("m-a", "A", 0.40, "a-yes", extra={"negRisk": False}),
                market_payload("m-b", "B", 0.40, "b-yes", extra={"negRisk": False}),
            ],
            neg_risk=False,
        )
    )
    books = {
        "a-yes": OrderBook.from_clob(book_payload("a-yes", asks=[{"price": "0.40", "size": "1000"}])),
        "b-yes": OrderBook.from_clob(book_payload("b-yes", asks=[{"price": "0.40", "size": "100"}])),
    }

    estimate = estimate_basket_cost(
        [(market, market.yes_token_id, "Yes") for market in event.markets],
        books,
        500,
    )

    assert estimate.executable is False
    assert estimate.atomic_risk is True
    assert estimate.max_executable_size == 100
    assert "atomic-risk" in estimate.note


def test_basket_below_min_order_is_not_executable():
    # A $0.05 leg × 10 shares = $0.50 notional, below the $1 per-leg CLOB floor.
    event = GammaEvent.from_gamma(
        event_payload(
            "tiny",
            "Tiny Order",
            [market_payload("m-a", "A", 0.05, "a-yes", extra={"negRisk": False})],
            neg_risk=False,
        )
    )
    books = {
        "a-yes": OrderBook.from_clob(book_payload("a-yes", asks=[{"price": "0.05", "size": "1000"}])),
    }

    estimate = estimate_basket_cost(
        [(market, market.yes_token_id, "Yes") for market in event.markets],
        books,
        10,
    )

    assert estimate.executable is False
    assert estimate.below_min_order is True
    assert estimate.atomic_risk is False


def test_per_share_probe_bypasses_min_order():
    # A 1-share probe at $0.30 is always below $1 notional, but the probe is
    # a theoretical per-share cost derivation — it must not be gated.
    event = GammaEvent.from_gamma(
        event_payload(
            "probe",
            "Probe",
            [market_payload("m-a", "A", 0.30, "a-yes", extra={"negRisk": False})],
            neg_risk=False,
        )
    )
    books = {
        "a-yes": OrderBook.from_clob(book_payload("a-yes", asks=[{"price": "0.30", "size": "1000"}])),
    }

    estimate = estimate_basket_cost(
        [(market, market.yes_token_id, "Yes") for market in event.markets],
        books,
        1.0,
        enforce_min_order=False,
    )

    assert estimate.executable is True
    assert estimate.net_cost == pytest.approx(0.30, rel=1e-9)


def test_decimal_math_avoids_float_residue_at_shelf_boundary():
    # Requesting an exact 100 shares at $0.1 per share should produce gross=10.00
    # with no float residue that could trip the fill-epsilon executability check.
    book = OrderBook.from_clob(
        book_payload("px", asks=[{"price": "0.1", "size": "100"}])
    )
    fill = book.buy_shares(100.0)
    assert fill.executable is True
    assert fill.filled_shares == 100.0
    assert fill.gross_cost == 10.0


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
