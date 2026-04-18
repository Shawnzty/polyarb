from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from polyarb.models.event import GammaEvent
from polyarb.models.market import GammaMarket
from polyarb.models.opportunity import Opportunity, OpportunityMarket
from polyarb.models.orderbook import OrderBook
from polyarb.scanners.execution import book_spread_warning, estimate_basket_cost


UNDERROUND_THRESHOLD = 0.005
OVERROUND_THRESHOLD = -0.005


class NegRiskScanner:
    def __init__(
        self,
        target_sizes: Iterable[float],
        fee_rates_by_token: Dict[str, float] = None,
    ) -> None:
        self.target_sizes = list(target_sizes)
        self.fee_rates_by_token = fee_rates_by_token or {}

    def scan(
        self,
        events: Iterable[GammaEvent],
        books_by_token: Dict[str, OrderBook],
    ) -> List[Opportunity]:
        opportunities: List[Opportunity] = []
        for event in events:
            if not event.active or event.closed or not event.neg_risk:
                continue
            markets = self._active_markets(event)
            if len(markets) < 2:
                continue

            one_share_ask = estimate_basket_cost(
                [(market, market.yes_token_id, "Yes") for market in markets],
                books_by_token,
                1.0,
                self.fee_rates_by_token,
            )
            sum_best_bid = self._sum_best_bids(markets, books_by_token)
            residual = None

            if one_share_ask.executable and one_share_ask.net_cost is not None and 1.0 - one_share_ask.net_cost >= UNDERROUND_THRESHOLD:
                sum_yes = one_share_ask.net_cost
                residual = 1.0 - sum_yes
                opp_type = "neg-risk-underround"
                explanation = (
                    f"Post-fee best-ask Yes basket costs {sum_yes:.4f}, leaving residual {residual:.4f}."
                )
                confidence = 0.95
                price_source = "clob_best_ask_post_fee"
            elif sum_best_bid is not None and 1.0 - sum_best_bid <= OVERROUND_THRESHOLD:
                sum_yes = sum_best_bid
                residual = 1.0 - sum_yes
                opp_type = "neg-risk-overround"
                explanation = (
                    f"Best-bid Yes basket sums to {sum_yes:.4f}; this is a distortion/relative-value flag, not a clean buy-basket arb."
                )
                confidence = 0.80
                price_source = "clob_best_bid"
            else:
                continue

            warnings = self._warnings(event, event.markets, books_by_token)
            execution = {
                str(int(size) if size.is_integer() else size): estimate_basket_cost(
                    [(market, market.yes_token_id, "Yes") for market in markets],
                    books_by_token,
                    size,
                    self.fee_rates_by_token,
                )
                for size in self.target_sizes
            }
            if not any(estimate.executable for estimate in execution.values()):
                warnings.append("insufficient-depth")

            opportunities.append(
                Opportunity(
                    type=opp_type,
                    title=event.title,
                    event={"id": event.id, "title": event.title, "slug": event.slug},
                    markets=[self._opportunity_market(market) for market in markets],
                    theoretical={
                        "sum_yes": sum_yes,
                        "residual": residual,
                        "edge": residual,
                        "kind": "underround" if residual > 0 else "overround",
                        "price_source": price_source,
                        "fee_cost_for_one_share_basket": one_share_ask.fee_cost,
                    },
                    execution_by_size=execution,
                    liquidity={
                        "event_volume": event.volume,
                        "event_volume24hr": event.volume24hr,
                        "event_liquidity": event.liquidity,
                        "market_volume_sum": sum(market.volume for market in markets),
                        "market_liquidity_sum": sum(market.liquidity for market in markets),
                    },
                    confidence=confidence,
                    warnings=sorted(set(warnings)),
                    explanation=explanation,
                )
            )

        return opportunities

    def _sum_best_bids(
        self,
        markets: List[GammaMarket],
        books_by_token: Dict[str, OrderBook],
    ) -> Optional[float]:
        total = 0.0
        for market in markets:
            token_id = market.yes_token_id
            if not token_id:
                return None
            book = books_by_token.get(token_id)
            if not book or book.best_bid is None:
                return None
            total += book.best_bid
        return total

    def _active_markets(self, event: GammaEvent) -> List[GammaMarket]:
        markets = []
        for market in event.markets:
            if not market.active or market.closed:
                continue
            markets.append(market)
        return markets

    def _warnings(
        self,
        event: GammaEvent,
        markets: List[GammaMarket],
        books_by_token: Dict[str, OrderBook],
    ) -> List[str]:
        warnings: List[str] = []
        if event.neg_risk_augmented or event.enable_neg_risk:
            warnings.append("augmented-neg-risk")
        for market in markets:
            title = market.display_title.lower()
            question = market.question.lower()
            if market.neg_risk_other or title == "other" or " another " in question or "any other" in question:
                warnings.append("other-outcome")
            if not market.active or market.closed:
                continue
            if not market.enable_order_book:
                warnings.append("orderbook-disabled")
            if not market.accepting_orders:
                warnings.append("not-accepting-orders")
            if market.fees_enabled:
                warnings.append("fees-enabled")
            if market.liquidity < 1000:
                warnings.append("low-liquidity")
            if market.yes_price is None:
                warnings.append("missing-price")
            if not market.yes_token_id:
                warnings.append("missing-token")
                continue
            book = books_by_token.get(market.yes_token_id)
            if not book:
                warnings.append("missing-book")
            elif not book.asks:
                warnings.append("empty-ask-book")
            if book_spread_warning(book, market.spread):
                warnings.append("wide-spread")
        return warnings

    def _opportunity_market(self, market: GammaMarket) -> OpportunityMarket:
        return OpportunityMarket(
            id=market.id,
            title=market.display_title,
            yes_token_id=market.yes_token_id,
            no_token_id=market.no_token_id,
            yes_price=market.yes_price,
            no_price=market.no_price,
            volume=market.volume,
            liquidity=market.liquidity,
            end_date=market.end_date,
            resolution_source=market.resolution_source,
            fees_enabled=market.fees_enabled,
            fee_rate=market.fee_rate,
        )
