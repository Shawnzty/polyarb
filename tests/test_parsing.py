from polyarb.models.event import GammaEvent
from polyarb.models.orderbook import OrderBook


def test_parse_real_like_gamma_json_string_fields():
    event = GammaEvent.from_gamma(
        {
            "id": "e1",
            "title": "Fixture",
            "slug": "fixture",
            "active": True,
            "closed": False,
            "negRisk": True,
            "negRiskAugmented": None,
            "volume": "1000.5",
            "liquidity": 250,
            "markets": [
                {
                    "id": "m1",
                    "question": "Will A win?",
                    "groupItemTitle": "A",
                    "outcomes": "[\"Yes\", \"No\"]",
                    "outcomePrices": "[\"0.25\", \"0.75\"]",
                    "clobTokenIds": "[\"yes-token\", \"no-token\"]",
                    "active": "true",
                    "closed": "false",
                    "enableOrderBook": True,
                    "volumeNum": "12.5",
                    "liquidityNum": "5",
                }
            ],
        }
    )

    market = event.markets[0]
    assert event.neg_risk is True
    assert market.outcomes == ["Yes", "No"]
    assert market.yes_price == 0.25
    assert market.no_price == 0.75
    assert market.yes_token_id == "yes-token"
    assert market.no_token_id == "no-token"


def test_orderbook_sorts_levels_for_execution_math():
    book = OrderBook.from_clob(
        {
            "market": "m",
            "asset_id": "yes",
            "timestamp": "1",
            "bids": [{"price": "0.10", "size": "1"}, {"price": "0.12", "size": "1"}],
            "asks": [{"price": "0.40", "size": "100"}, {"price": "0.30", "size": "100"}],
        }
    )

    assert book.best_bid == 0.12
    assert book.best_ask == 0.30
    fill = book.buy_shares(150)
    assert fill.executable is True
    assert fill.cost == 50
