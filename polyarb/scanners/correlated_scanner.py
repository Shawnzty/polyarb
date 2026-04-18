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
    def __init__(self, target_sizes: Iterable[float]) -> None:
        self.target_sizes = list(target_sizes)

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
        implications.extend(self._presidential_path_implications(event_list))
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
            earlier = dated[index - 1][1]
            later = dated[index][1]
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

    def _presidential_path_implications(self, events: List[GammaEvent]) -> List[Implication]:
        nominee_events: Dict[Tuple[str, str], GammaEvent] = {}
        winner_events: Dict[str, GammaEvent] = {}
        nominee_re = re.compile(r"\b(democratic|republican)\s+presidential\s+nominee\s+(\d{4})\b", re.I)
        winner_re = re.compile(r"\bpresidential\s+election\s+winner\s+(\d{4})\b", re.I)

        for event in events:
            nominee_match = nominee_re.search(event.title)
            if nominee_match:
                nominee_events[(nominee_match.group(1).lower(), nominee_match.group(2))] = event
            winner_match = winner_re.search(event.title)
            if winner_match:
                winner_events[winner_match.group(1)] = event

        implications: List[Implication] = []
        for (_party, year), nominee_event in nominee_events.items():
            winner_event = winner_events.get(year)
            if not winner_event:
                continue
            nominee_by_name = {
                self._normalize_name(market.display_title): market
                for market in nominee_event.markets
                if market.yes_price is not None and self._normalize_name(market.display_title) != "other"
            }
            for winner_market in winner_event.markets:
                name = self._normalize_name(winner_market.display_title)
                nominee_market = nominee_by_name.get(name)
                if not nominee_market or winner_market.yes_price is None:
                    continue
                synthetic_event = GammaEvent(
                    id=f"{nominee_event.id}:{winner_event.id}",
                    title=f"{nominee_event.title} / {winner_event.title}",
                    slug=f"{nominee_event.slug}:{winner_event.slug}",
                    description="",
                    active=True,
                    closed=False,
                    neg_risk=False,
                    neg_risk_augmented=False,
                    enable_neg_risk=False,
                    show_all_outcomes=False,
                    volume=max(nominee_event.volume, winner_event.volume),
                    volume24hr=max(nominee_event.volume24hr, winner_event.volume24hr),
                    liquidity=max(nominee_event.liquidity, winner_event.liquidity),
                    end_date="",
                    markets=[nominee_market, winner_market],
                )
                implications.append(
                    Implication(
                        type="correlated-path",
                        event=synthetic_event,
                        easier=nominee_market,
                        harder=winner_market,
                        confidence=0.86,
                        explanation=(
                            f"{winner_market.display_title} winning the presidency should imply first winning the matching party nomination."
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
        package_cost = easy_yes + hard_no
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
        )

    def _violation(self, easier: GammaMarket, harder: GammaMarket) -> float:
        if easier.yes_price is None or harder.yes_price is None:
            return 0.0
        return harder.yes_price - easier.yes_price

    def _threshold_direction(self, market: GammaMarket) -> Optional[str]:
        text = f"{market.group_item_title} {market.question}".lower()
        if "↑" in text or "above" in text or "reach" in text or " hit (high)" in text or " high" in text:
            return "up"
        if "↓" in text or "below" in text or "dip" in text or " hit (low)" in text or " low" in text:
            return "down"
        return None

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
