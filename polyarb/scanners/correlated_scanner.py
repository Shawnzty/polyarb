from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from polyarb.models.event import GammaEvent
from polyarb.models.market import GammaMarket
from polyarb.models.opportunity import Opportunity, OpportunityMarket
from polyarb.models.orderbook import OrderBook
from polyarb.scanners.execution import book_spread_warning, estimate_basket_cost


MIN_CONFIDENCE = 0.85
MIN_VIOLATION = 0.01
MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


@dataclass(frozen=True)
class Implication:
    type: str
    event: GammaEvent
    easier: GammaMarket
    harder: GammaMarket
    confidence: float
    explanation: str


class CorrelatedScanner:
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
        event_list = [event for event in events if event.active and not event.closed]
        implications: List[Implication] = []
        for event in event_list:
            implications.extend(self._time_implications(event))
            implications.extend(self._threshold_implications(event))
        return [
            self._to_opportunity(implication, books_by_token)
            for implication in implications
            if implication.confidence >= MIN_CONFIDENCE
            and self._violation(implication.easier, implication.harder) >= MIN_VIOLATION
        ]

    def _time_implications(self, event: GammaEvent) -> List[Implication]:
        title = event.title.lower()
        if " by" not in title and "by..." not in title and "by ...?" not in title:
            return []
        dated: List[Tuple[datetime, GammaMarket]] = []
        for market in event.markets:
            parsed = self._parse_date(market.group_item_title, market.question, event.end_date)
            if parsed and market.yes_price is not None:
                dated.append((parsed, market))
        dated.sort(key=lambda item: item[0])
        implications: List[Implication] = []
        for index in range(1, len(dated)):
            earlier_date, earlier = dated[index - 1]
            later_date, later = dated[index]
            if later_date <= earlier_date or not self._rules_match(event, earlier, later, allow_distinct_dates=True):
                continue
            implications.append(
                Implication(
                    type="correlated-time",
                    event=event,
                    easier=later,
                    harder=earlier,
                    confidence=0.90,
                    explanation=(
                        f"Later deadline '{later.display_title}' should be at least as likely as earlier deadline '{earlier.display_title}'."
                    ),
                )
            )
        return implications

    def _threshold_implications(self, event: GammaEvent) -> List[Implication]:
        grouped: Dict[str, List[Tuple[float, GammaMarket]]] = {"up": [], "down": []}
        for market in event.markets:
            if self._is_range_bucket(market):
                continue
            direction = self._threshold_direction(market)
            threshold = self._parse_threshold(market.group_item_title) or self._parse_threshold(market.question)
            if direction and threshold is not None and market.yes_price is not None:
                grouped[direction].append((threshold, market))

        implications: List[Implication] = []
        for direction, items in grouped.items():
            if len(items) < 2:
                continue
            items.sort(key=lambda item: item[0])
            if direction == "up":
                easier_to_harder = items
            else:
                easier_to_harder = list(reversed(items))
            for index in range(1, len(easier_to_harder)):
                easier = easier_to_harder[index - 1][1]
                harder = easier_to_harder[index][1]
                if not self._rules_match(event, easier, harder, allow_distinct_dates=False):
                    continue
                implications.append(
                    Implication(
                        type="correlated-threshold",
                        event=event,
                        easier=easier,
                        harder=harder,
                        confidence=0.90,
                        explanation=(
                            f"Easier threshold '{easier.display_title}' should be at least as likely as harder threshold '{harder.display_title}'."
                        ),
                    )
                )
        return implications

    def _to_opportunity(
        self,
        implication: Implication,
        books_by_token: Dict[str, OrderBook],
    ) -> Opportunity:
        easy_yes = implication.easier.yes_price or 0.0
        hard_yes = implication.harder.yes_price or 0.0
        hard_no = implication.harder.no_price
        if hard_no is None:
            hard_no = max(0.0, 1.0 - hard_yes)
        one_share_package = estimate_basket_cost(
            [
                (implication.easier, implication.easier.yes_token_id, "Yes"),
                (implication.harder, implication.harder.no_token_id, "No"),
            ],
            books_by_token,
            1.0,
            self.fee_rates_by_token,
        )
        package_cost = one_share_package.net_cost if one_share_package.executable and one_share_package.net_cost is not None else easy_yes + hard_no
        edge = 1.0 - package_cost
        violation = hard_yes - easy_yes
        warnings = self._warnings(implication, books_by_token)
        execution = {
            str(int(size) if size.is_integer() else size): estimate_basket_cost(
                [
                    (implication.easier, implication.easier.yes_token_id, "Yes"),
                    (implication.harder, implication.harder.no_token_id, "No"),
                ],
                books_by_token,
                size,
                self.fee_rates_by_token,
            )
            for size in self.target_sizes
        }
        if not any(estimate.executable for estimate in execution.values()):
            warnings.append("insufficient-depth")

        return Opportunity(
            type=implication.type,
            title=implication.event.title,
            event={
                "id": implication.event.id,
                "title": implication.event.title,
                "slug": implication.event.slug,
            },
            markets=[
                self._opportunity_market(implication.easier),
                self._opportunity_market(implication.harder),
            ],
            theoretical={
                "easier_yes": easy_yes,
                "harder_yes": hard_yes,
                "violation": violation,
                "package_cost": package_cost,
                "edge": edge,
                "kind": "implication-package",
                "price_source": "clob_best_ask_post_fee" if one_share_package.executable else "gamma_price_fallback",
                "fee_cost_for_one_share_package": one_share_package.fee_cost,
            },
            execution_by_size=execution,
            liquidity={
                "event_volume": implication.event.volume,
                "event_volume24hr": implication.event.volume24hr,
                "event_liquidity": implication.event.liquidity,
                "market_volume_sum": implication.easier.volume + implication.harder.volume,
                "market_liquidity_sum": implication.easier.liquidity + implication.harder.liquidity,
            },
            confidence=implication.confidence,
            warnings=sorted(set(warnings)),
            explanation=f"{implication.explanation} Report as a conservative logic anomaly unless execution is explicitly shown.",
        )

    def _warnings(
        self,
        implication: Implication,
        books_by_token: Dict[str, OrderBook],
    ) -> List[str]:
        warnings: List[str] = []
        if implication.confidence < 0.95:
            warnings.append("logic-link-confidence")
        for market, token_id in [
            (implication.easier, implication.easier.yes_token_id),
            (implication.harder, implication.harder.no_token_id),
        ]:
            if market.fees_enabled:
                warnings.append("fees-enabled")
            if market.liquidity < 1000:
                warnings.append("low-liquidity")
            book = books_by_token.get(token_id or "")
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

    def _violation(self, easier: GammaMarket, harder: GammaMarket) -> float:
        if easier.yes_price is None or harder.yes_price is None:
            return 0.0
        return harder.yes_price - easier.yes_price

    def _threshold_direction(self, market: GammaMarket) -> Optional[str]:
        text = f"{market.group_item_title} {market.question}".lower()
        if "↑" in text or "above" in text or "reach" in text or "at least" in text or "hit (high)" in text:
            return "up"
        if "↓" in text or "below" in text or "dip" in text or "hit (low)" in text:
            return "down"
        return None

    def _is_range_bucket(self, market: GammaMarket) -> bool:
        text = f"{market.group_item_title} {market.question}".lower()
        if any(term in text for term in ["between", "range", "o/u", "over/under", "total corners"]):
            return True
        return bool(re.search(r"\b\d+(?:\.\d+)?\s*[-–]\s*\d+(?:\.\d+)?\b", text))

    def _rules_match(
        self,
        event: GammaEvent,
        easier: GammaMarket,
        harder: GammaMarket,
        allow_distinct_dates: bool,
    ) -> bool:
        if (easier.fee_rate or 0.0) != (harder.fee_rate or 0.0) or easier.fees_enabled != harder.fees_enabled:
            return False
        if not allow_distinct_dates and self._normalize_text(easier.end_date) != self._normalize_text(harder.end_date):
            return False
        easier_source = self._normalize_text(easier.resolution_source or event.resolution_source)
        harder_source = self._normalize_text(harder.resolution_source or event.resolution_source)
        if easier_source or harder_source:
            return easier_source == harder_source
        easier_description = self._normalize_text(easier.description or event.description)
        harder_description = self._normalize_text(harder.description or event.description)
        return bool(easier_description and harder_description and easier_description == harder_description)

    def _parse_threshold(self, text: str) -> Optional[float]:
        normalized = text.replace(",", "")
        matches = re.findall(r"[$£€]?\s*(\d+(?:\.\d+)?)\s*([kKmMbB]?)", normalized)
        values = []
        for number, suffix in matches:
            value = float(number)
            if suffix.lower() == "k":
                value *= 1_000
            elif suffix.lower() == "m":
                value *= 1_000_000
            elif suffix.lower() == "b":
                value *= 1_000_000_000
            values.append(value)
        return max(values) if values else None

    def _parse_date(self, label: str, question: str, event_end_date: str) -> Optional[datetime]:
        text = f"{label} {question}"
        pattern = re.compile(
            r"\b("
            + "|".join(MONTHS.keys())
            + r")\s+(\d{1,2})(?:,\s*(\d{4}))?",
            re.I,
        )
        match = pattern.search(text)
        if not match:
            return None
        month = MONTHS[match.group(1).lower()]
        day = int(match.group(2))
        year = int(match.group(3)) if match.group(3) else self._infer_year(question, event_end_date)
        if not year:
            return None
        try:
            return datetime(year, month, day)
        except ValueError:
            return None

    def _infer_year(self, question: str, event_end_date: str) -> Optional[int]:
        year_match = re.search(r"\b(20\d{2})\b", question)
        if year_match:
            return int(year_match.group(1))
        if event_end_date:
            try:
                return datetime.fromisoformat(event_end_date.replace("Z", "+00:00")).year
            except ValueError:
                return None
        return None

    def _normalize_name(self, value: str) -> str:
        return re.sub(r"\s+", " ", value.strip().lower())

    def _normalize_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", (value or "").strip().lower())
