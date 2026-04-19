from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from polyarb.api.async_clob_client import AsyncClobClient
from polyarb.api.async_gamma_client import AsyncGammaClient
from polyarb.api.async_http import AsyncHttpClient


def _make_transport(handler):
    async def wrapped(request: httpx.Request) -> httpx.Response:
        return await handler(request)

    return httpx.MockTransport(wrapped)


def test_async_gamma_paginates_concurrently():
    # Build a mock Gamma that returns 100 events per page and records the
    # timestamps of each request — if pagination is concurrent, the inter-
    # request gap is zero; if serial, it would compound.
    page_requests: list[float] = []

    loop = asyncio.new_event_loop()

    try:
        async def handler(request: httpx.Request) -> httpx.Response:
            page_requests.append(loop.time())
            offset = int(request.url.params.get("offset", "0"))
            events = [
                {
                    "id": f"e-{offset + i}",
                    "title": f"Event {offset + i}",
                    "slug": f"e-{offset + i}",
                    "active": True,
                    "closed": False,
                    "negRisk": False,
                    "volume": 1000,
                    "markets": [],
                }
                for i in range(100)
            ]
            # Simulate 50ms of server work per request. Three serial pages
            # would take 150ms; three concurrent ones take ~50ms total.
            await asyncio.sleep(0.05)
            return httpx.Response(200, content=json.dumps(events))

        async def run():
            http = AsyncHttpClient("https://gamma.test", client=httpx.AsyncClient(transport=_make_transport(handler)))
            async with http:
                client = AsyncGammaClient(client=http)
                start = loop.time()
                events = await client.get_events(limit_events=300, page_size=100)
                elapsed = loop.time() - start
                return events, elapsed

        events, elapsed = loop.run_until_complete(run())
    finally:
        loop.close()

    assert len(events) == 300
    # Three pages × 50ms serial would be ≥150ms; concurrent should finish
    # well under 120ms even on a loaded CI box.
    assert elapsed < 0.12, f"gamma pagination did not run concurrently (elapsed={elapsed:.3f}s)"


def test_async_clob_fanout_on_batch_failure():
    # The batched /books endpoint returns 500; the client must fan out to
    # concurrent per-token /book calls instead of a serial fallback.
    call_count = {"books": 0, "book": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/books"):
            call_count["books"] += 1
            return httpx.Response(500, content=b"")
        if path.endswith("/book"):
            call_count["book"] += 1
            token_id = request.url.params.get("token_id")
            await asyncio.sleep(0.02)
            return httpx.Response(
                200,
                content=json.dumps(
                    {
                        "market": f"m-{token_id}",
                        "asset_id": token_id,
                        "timestamp": "1",
                        "bids": [{"price": "0.50", "size": "100"}],
                        "asks": [{"price": "0.51", "size": "100"}],
                    }
                ),
            )
        return httpx.Response(404)

    async def run():
        http = AsyncHttpClient(
            "https://clob.test",
            client=httpx.AsyncClient(transport=_make_transport(handler)),
            retries=1,
        )
        async with http:
            client = AsyncClobClient(client=http)
            return await client.get_books([f"tok-{i}" for i in range(10)])

    loop = asyncio.new_event_loop()
    try:
        books = loop.run_until_complete(run())
    finally:
        loop.close()

    assert len(books) == 10
    assert call_count["books"] >= 1  # batch call attempted
    assert call_count["book"] == 10  # fan-out called once per token


def test_async_clob_fee_rate_cache_avoids_refetch():
    calls = {"count": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return httpx.Response(200, content=json.dumps({"base_fee": "50"}))

    async def run():
        http = AsyncHttpClient(
            "https://clob.test",
            client=httpx.AsyncClient(transport=_make_transport(handler)),
            retries=1,
        )
        async with http:
            client = AsyncClobClient(client=http)
            first = await client.get_fee_rates(["a", "b", "c"])
            second = await client.get_fee_rates(["a", "b", "c"])
            return first, second, calls["count"]

    loop = asyncio.new_event_loop()
    try:
        first, second, total_calls = loop.run_until_complete(run())
    finally:
        loop.close()

    assert first == second
    assert set(first.keys()) == {"a", "b", "c"}
    # First batch: 3 fetches. Second batch: all cached. Total == 3.
    assert total_calls == 3
