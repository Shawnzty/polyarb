from __future__ import annotations

from typing import Dict, Iterable, List

from polyarb.api.http import ApiError, HttpClient
from polyarb.models.orderbook import OrderBook


class ClobClient:
    """Read-only client for Polymarket CLOB order books."""

    def __init__(self, base_url: str = "https://clob.polymarket.com") -> None:
        self.http = HttpClient(base_url)

    def get_books(self, token_ids: Iterable[str], batch_size: int = 500) -> Dict[str, OrderBook]:
        unique_ids = list(dict.fromkeys(str(token_id) for token_id in token_ids if token_id))
        books: Dict[str, OrderBook] = {}

        for start in range(0, len(unique_ids), batch_size):
            chunk = unique_ids[start : start + batch_size]
            if not chunk:
                continue
            try:
                payload = self.http.post("/books", [{"token_id": token_id} for token_id in chunk])
            except ApiError:
                payload = self._fallback_books(chunk)

            if not isinstance(payload, list):
                continue
            for item in payload:
                if not isinstance(item, dict):
                    continue
                book = OrderBook.from_clob(item)
                if book.asset_id:
                    books[book.asset_id] = book

        return books

    def _fallback_books(self, token_ids: List[str]) -> List[dict]:
        books = []
        for token_id in token_ids:
            try:
                payload = self.http.get("/book", params={"token_id": token_id})
            except ApiError:
                continue
            if isinstance(payload, dict):
                books.append(payload)
        return books
