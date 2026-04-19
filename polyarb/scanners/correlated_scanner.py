from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from polyarb.models.event import GammaEvent
from polyarb.models.market import GammaMarket
from polyarb.models.opportunity import ExecutionEstimate, Opportunity, OpportunityMarket
from polyarb.models.orderbook import OrderBook
from polyarb.scanners.execution import book_spread_warning, estimate_basket_cost


MIN_CONFIDENCE = 0.85
MIN_VIOLATION = 0.01
MONTHS_FULL = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]
MONTH_ABBR = [m[:3] for m in MONTHS_FULL]
MONTHS = {name: i + 1 for i, name in enumerate(MONTHS_FULL)}
MONTHS.update({abbr: i + 1 for i, abbr in enumerate(MONTH_ABBR)})

# Phrases that indicate a range bucket (e.g., "between $90k and $100k"). These
# must be excluded from simple threshold comparisons. Keep the list tight —
# bare "to $X" phrases like "dip to $80,000" are threshold statements, not
# range buckets, and must not appear here.
RANGE_PHRASES = (
    "between", "range", "o/u", "over/under", "total corners", "band",
)
# "from X to Y" range phrases (with optional currency markers on both numbers).
RANGE_FROM_TO_PATTERN = re.compile(
    r"\bfrom\s*[$£€]?\s*\d+(?:\.\d+)?\s*[kmb]?\s+(?:to|through|thru|and)\s*[$£€]?\s*\d+(?:\.\d+)?\s*[kmb]?\b",
    re.I,
)
# Match any two numbers separated by an ASCII or Unicode dash, with optional k/m/b suffixes.
RANGE_DASH_PATTERN = re.compile(
    r"\b\d+(?:\.\d+)?\s*[kmb]?\s*[-–—−]\s*\d+(?:\.\d+)?\s*[kmb]?\b", re.I
)
# Match a directional verb immediately followed by a currency/number, e.g. "above $80,000".
DIRECTIONAL_THRESHOLD_PATTERN = re.compile(
    r"(above|below|over|under|reach(?:es|ed)?|hit(?:s|ting)?|clears?|tops?|at least|at most|≥|>=|≤|<=|>|<)"
    r"\s*[$£€]?\s*(\d+(?:\.\d+)?)\s*([kmb]?)",
    re.I,
)
# Fallback: any number with optional currency and k/m/b suffix.
NUMERIC_PATTERN = re.compile(r"[$£€]?\s*(\d+(?:\.\d+)?)\s*([kmb]?)", re.I)
# Full month or 3-letter abbreviation followed by a day (and optional year).
MONTH_DAY_PATTERN = re.compile(
    r"\b(" + "|".join(MONTHS_FULL + MONTH_ABBR) + r")\s+(\d{1,2})(?:,\s*(\d{4}))?",
    re.I,
)
# ISO-style YYYY-MM-DD.
ISO_DATE_PATTERN = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")
# MM/DD[/YYYY] — ambiguous without locale, treated as US-style.
US_DATE_PATTERN = re.compile(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b")


def _apply_magnitude_suffix(value: float, suffix: str) -> float:
    lowered = (suffix or "").lower()
    if lowered == "k":
        return value * 1_000
    if lowered == "m":
        return value * 1_000_000
    if lowered == "b":
        return value * 1_000_000_000
    return value


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
        max_book_age_s: Optional[float] = None,
        now: Optional[float] = None,
    ) -> None:
        self.target_sizes = list(target_sizes)
        self.fee_rates_by_token = fee_rates_by_token or {}
        self.max_book_age_s = max_book_age_s
        self._now = now

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
        opportunities: List[Opportunity] = []
        for implication in implications:
            if implication.confidence < MIN_CONFIDENCE:
                continue
            package = estimate_basket_cost(
                [
                    (implication.easier, implication.easier.yes_token_id, "Yes"),
                    (implication.harder, implication.harder.no_token_id, "No"),
                ],
                books_by_token,
                1.0,
                self.fee_rates_by_token,
                enforce_min_order=False,
            )
            # CLOB-first gating: require the real post-fee package cost to prove
            # the violation. Gamma `yes_price` is never trusted as the gate.
            if not package.executable or package.net_cost is None:
                continue
            if package.net_cost > 1.0 - MIN_VIOLATION:
                continue
            if self._is_stale(package.min_book_timestamp):
                continue
            opportunities.append(self._to_opportunity(implication, books_by_token, package))
        return opportunities

    def _is_stale(self, min_book_timestamp: Optional[float]) -> bool:
        if self.max_book_age_s is None:
            return False
        if min_book_timestamp is None:
            return True
        now = self._now if self._now is not None else time.time()
        return (now - min_book_timestamp) > self.max_book_age_s

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
        one_share_package: ExecutionEstimate,
    ) -> Opportunity:
        easy_yes = implication.easier.yes_price or 0.0
        hard_yes = implication.harder.yes_price or 0.0
        package_cost = one_share_package.net_cost
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
                "price_source": "clob_best_ask_post_fee",
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
        if any(term in text for term in RANGE_PHRASES):
            return True
        if RANGE_FROM_TO_PATTERN.search(text):
            return True
        return bool(RANGE_DASH_PATTERN.search(text))

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
        # Prefer a directional verb right before a number: `above $80,000`, `reaches $100k`.
        directional = DIRECTIONAL_THRESHOLD_PATTERN.search(normalized)
        if directional:
            return _apply_magnitude_suffix(float(directional.group(2)), directional.group(3))
        # Fallback: if the text contains exactly one numeric match, use it.
        numeric_matches = NUMERIC_PATTERN.findall(normalized)
        if len(numeric_matches) == 1:
            number, suffix = numeric_matches[0]
            return _apply_magnitude_suffix(float(number), suffix)
        return None

    def _parse_date(self, label: str, question: str, event_end_date: str) -> Optional[datetime]:
        text = f"{label} {question}"
        for parser in (self._parse_iso_date, self._parse_month_day, self._parse_us_date):
            result = parser(text, question, event_end_date)
            if result is not None:
                return result
        return None

    def _parse_iso_date(self, text: str, question: str, event_end_date: str) -> Optional[datetime]:
        match = ISO_DATE_PATTERN.search(text)
        if not match:
            return None
        try:
            return datetime(
                int(match.group(1)), int(match.group(2)), int(match.group(3)), tzinfo=timezone.utc
            )
        except ValueError:
            return None

    def _parse_month_day(self, text: str, question: str, event_end_date: str) -> Optional[datetime]:
        match = MONTH_DAY_PATTERN.search(text)
        if not match:
            return None
        month = MONTHS.get(match.group(1).lower())
        if month is None:
            return None
        day = int(match.group(2))
        year = int(match.group(3)) if match.group(3) else self._infer_year(question, event_end_date)
        if not year:
            return None
        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
        except ValueError:
            return None

    def _parse_us_date(self, text: str, question: str, event_end_date: str) -> Optional[datetime]:
        match = US_DATE_PATTERN.search(text)
        if not match:
            return None
        month = int(match.group(1))
        day = int(match.group(2))
        if not (1 <= month <= 12 and 1 <= day <= 31):
            return None
        raw_year = match.group(3)
        if raw_year:
            year = int(raw_year)
            if year < 100:
                year += 2000
        else:
            year = self._infer_year(question, event_end_date)
        if not year:
            return None
        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
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
