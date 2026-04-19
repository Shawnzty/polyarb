from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, TextIO

# Runs before any third-party imports so a missing distribution prompts the
# user (or surfaces a pip command on non-TTY pipelines) rather than blowing
# up with `ModuleNotFoundError: No module named 'httpx'`.
from polyarb._preflight import ensure_dependencies, missing_requirements

ensure_dependencies()

from polyarb.api.async_clob_client import AsyncClobClient
from polyarb.api.async_gamma_client import AsyncGammaClient
from polyarb.api.clob_client import ClobClient
from polyarb.api.gamma_client import GammaClient, collect_fee_token_ids, collect_market_token_ids
from polyarb.config import load_config
from polyarb.models.event import GammaEvent
from polyarb.models.opportunity import Opportunity
from polyarb.ranking.scoring import score_opportunities
from polyarb.scanners.correlated_scanner import CorrelatedScanner
from polyarb.scanners.neg_risk_scanner import NegRiskScanner
from polyarb.streaming.state_store import StateStore
from polyarb.streaming.watcher import Watcher, WatcherConfig
from polyarb.timeutils import filter_events_by_horizon


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="polyarb", description="Research-only Polymarket arbitrage scanner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan = subparsers.add_parser("scan", help="scan public Polymarket data")
    scan.add_argument("--json", action="store_true", help="print machine-readable JSON")
    scan.add_argument("--config", help="path to config.yaml")
    scan.add_argument("--min-volume", type=float, default=0.0, help="minimum event lifetime volume")
    scan.add_argument("--target-sizes", help="comma-separated payout notionals")
    scan.add_argument("--within-hours", type=float, help="only scan markets ending within this many hours")
    scan.add_argument("--all-horizons", action="store_true", help="disable configured horizon filtering")
    scan.add_argument("--limit-events", type=int, default=200, help="maximum active events to inspect")
    scan.add_argument("--max-results", type=int, default=25, help="maximum opportunities to print")
    scan.add_argument("--neg-risk-only", action="store_true", help="only run neg-risk scanner")
    scan.add_argument("--correlated-only", action="store_true", help="only run correlated scanner")
    scan.add_argument(
        "--rank-by",
        choices=("apy", "edge_pct", "edge_dollar"),
        default="apy",
        help="ranking metric: apy (default), edge_pct, or edge_dollar (legacy)",
    )

    watch = subparsers.add_parser(
        "watch",
        help="run an orderbook-watching loop that emits NEW/CHANGED/CLOSED diffs",
    )
    watch.add_argument("--json", action="store_true", help="print machine-readable JSON")
    watch.add_argument("--config", help="path to config.yaml")
    watch.add_argument("--min-volume", type=float, default=0.0)
    watch.add_argument("--target-sizes", help="comma-separated payout notionals")
    watch.add_argument("--within-hours", type=float)
    watch.add_argument("--all-horizons", action="store_true")
    watch.add_argument("--limit-events", type=int, default=200)
    watch.add_argument("--max-results", type=int, default=25)
    watch.add_argument("--neg-risk-only", action="store_true")
    watch.add_argument("--correlated-only", action="store_true")
    watch.add_argument(
        "--rank-by",
        choices=("apy", "edge_pct", "edge_dollar"),
        default="apy",
    )
    watch.add_argument(
        "--once",
        action="store_true",
        help="bootstrap + score once, emit diff, exit (no WS loop)",
    )
    watch.add_argument(
        "--state-path",
        help="append JSONL lifecycle log to this file (NEW/CHANGED/CLOSED per run)",
    )

    subparsers.add_parser(
        "check-deps",
        help="verify required third-party dependencies are installed; prompt to install if not",
    )
    return parser


def main(
    argv: Optional[List[str]] = None,
    gamma_client: Optional[GammaClient] = None,
    clob_client: Optional[ClobClient] = None,
    stdout: Optional[TextIO] = None,
    events_source: Optional[Any] = None,
    books_source: Optional[Any] = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "neg_risk_only", False) and getattr(args, "correlated_only", False):
        parser.error("--neg-risk-only and --correlated-only cannot be used together")

    stdout = stdout or sys.stdout
    if args.command == "scan":
        if gamma_client is None and clob_client is None:
            # Production path: async I/O against live APIs. ~1 RTT Gamma page,
            # concurrent CLOB book batching with fan-out on failure, cached
            # fee rates.
            report = asyncio.run(run_scan_async(args))
        else:
            # Test path: injected synchronous fake clients.
            report = run_scan(args, gamma_client=gamma_client, clob_client=clob_client)
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True), file=stdout)
        else:
            print(format_human(report), file=stdout)
        return 0

    if args.command == "watch":
        report = asyncio.run(
            run_watch_async(args, events_source=events_source, books_source=books_source)
        )
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True), file=stdout)
        else:
            print(format_watch_human(report), file=stdout)
        return 0

    if args.command == "check-deps":
        # By the time we got here, the module-level ensure_dependencies() call
        # already ran. If it returned, all deps are present; the user-facing
        # confirmation below is the only work left.
        missing = missing_requirements()
        if missing:
            names = ", ".join(req.spec for req in missing)
            print(f"Missing: {names}", file=stdout)
            return 1
        print("All required dependencies installed.", file=stdout)
        return 0

    parser.error(f"unknown command {args.command}")
    return 2


async def run_scan_async(args: argparse.Namespace) -> Dict[str, Any]:
    config = load_config(args.config)
    target_sizes = (
        parse_target_sizes(args.target_sizes)
        if args.target_sizes
        else [float(size) for size in config["scan"]["target_sizes"]]
    )
    within_hours = None
    if not args.all_horizons:
        within_hours = args.within_hours if args.within_hours is not None else float(config["scan"]["within_hours"])

    async with AsyncGammaClient() as gamma, AsyncClobClient() as clob:
        events = await gamma.get_events(limit_events=args.limit_events, min_volume=args.min_volume)
        scanned_events = filter_events_by_horizon(events, within_hours)
        include_no = not args.neg_risk_only
        token_ids = collect_market_token_ids(scanned_events, include_no=include_no)
        fee_token_ids = collect_fee_token_ids(scanned_events, include_no=include_no)
        books_by_token, fee_rates_by_token = await asyncio.gather(
            clob.get_books(token_ids),
            clob.get_fee_rates(fee_token_ids) if fee_token_ids else _empty_fee_map(),
        )

    return _assemble_report(
        args, config, events, scanned_events, books_by_token, fee_rates_by_token, target_sizes, within_hours
    )


async def _empty_fee_map() -> Dict[str, float]:
    return {}


async def run_watch_async(
    args: argparse.Namespace,
    events_source: Optional[Any] = None,
    books_source: Optional[Any] = None,
) -> Dict[str, Any]:
    # Only `--once` is supported today — the WS-driven steady-state loop will
    # plug into the same Watcher instance once wired. Keeping both modes on a
    # single code path means `watch --once` is bit-for-bit identical to a
    # bootstrap-only WS run, which is exactly the reproducibility invariant
    # the plan calls for.
    if not args.once:
        raise NotImplementedError(
            "continuous watch requires a WS delta feed; run with --once for now"
        )
    config = load_config(args.config)
    target_sizes = (
        parse_target_sizes(args.target_sizes)
        if args.target_sizes
        else [float(size) for size in config["scan"]["target_sizes"]]
    )
    within_hours = None
    if not args.all_horizons:
        within_hours = args.within_hours if args.within_hours is not None else float(config["scan"]["within_hours"])
    watcher_config = WatcherConfig(
        target_sizes=target_sizes,
        within_hours=within_hours,
        limit_events=args.limit_events,
        min_volume=args.min_volume,
        max_results=args.max_results,
        max_book_age_s=config["scan"].get("max_book_age_s"),
        rank_by=args.rank_by,
        neg_risk_only=args.neg_risk_only,
        correlated_only=args.correlated_only,
        risk_config=config.get("risk", {}),
    )
    state_store = StateStore(args.state_path) if args.state_path else StateStore()

    if events_source is not None and books_source is not None:
        watcher = Watcher(watcher_config, events_source, books_source, state_store=state_store)
        await watcher.bootstrap()
        diff = await watcher.rescore()
    else:
        async with AsyncGammaClient() as gamma, AsyncClobClient() as clob:
            watcher = Watcher(watcher_config, gamma, clob, state_store=state_store)
            await watcher.bootstrap()
            diff = await watcher.rescore()

    report = watcher.build_report(diff)
    report["config"] = {
        "min_volume": args.min_volume,
        "target_sizes": target_sizes,
        "within_hours": within_hours,
        "all_horizons": args.all_horizons,
        "config_path": args.config or "config.yaml",
        "limit_events": args.limit_events,
        "max_results": args.max_results,
        "neg_risk_only": args.neg_risk_only,
        "correlated_only": args.correlated_only,
        "rank_by": args.rank_by,
        "once": args.once,
        "state_path": args.state_path,
    }
    return report


def run_scan(
    args: argparse.Namespace,
    gamma_client: Optional[GammaClient] = None,
    clob_client: Optional[ClobClient] = None,
) -> Dict[str, Any]:
    config = load_config(args.config)
    target_sizes = (
        parse_target_sizes(args.target_sizes)
        if args.target_sizes
        else [float(size) for size in config["scan"]["target_sizes"]]
    )
    within_hours = None
    if not args.all_horizons:
        within_hours = args.within_hours if args.within_hours is not None else float(config["scan"]["within_hours"])

    gamma = gamma_client or GammaClient()
    clob = clob_client or ClobClient()

    events = gamma.get_events(limit_events=args.limit_events, min_volume=args.min_volume)
    scanned_events = filter_events_by_horizon(events, within_hours)
    include_no = not args.neg_risk_only
    token_ids = collect_market_token_ids(scanned_events, include_no=include_no)
    books_by_token = clob.get_books(token_ids)
    fee_token_ids = collect_fee_token_ids(scanned_events, include_no=include_no)
    fee_rates_by_token = (
        clob.get_fee_rates(fee_token_ids)
        if fee_token_ids and hasattr(clob, "get_fee_rates")
        else {}
    )

    return _assemble_report(
        args, config, events, scanned_events, books_by_token, fee_rates_by_token, target_sizes, within_hours
    )


def _assemble_report(
    args: argparse.Namespace,
    config: Dict[str, Any],
    events: List[GammaEvent],
    scanned_events: List[GammaEvent],
    books_by_token: Dict[str, Any],
    fee_rates_by_token: Dict[str, float],
    target_sizes: List[float],
    within_hours: Optional[float],
) -> Dict[str, Any]:
    max_book_age_s = config["scan"].get("max_book_age_s")
    opportunities: List[Opportunity] = []
    if not args.correlated_only:
        opportunities.extend(
            NegRiskScanner(target_sizes, fee_rates_by_token, max_book_age_s=max_book_age_s).scan(
                scanned_events, books_by_token
            )
        )
    if not args.neg_risk_only:
        opportunities.extend(
            CorrelatedScanner(target_sizes, fee_rates_by_token, max_book_age_s=max_book_age_s).scan(
                scanned_events, books_by_token
            )
        )

    scored = score_opportunities(
        opportunities,
        config.get("risk", {}),
        rank_by=args.rank_by,
    )[: args.max_results]
    for rank, opportunity in enumerate(scored, start=1):
        opportunity.rank = rank

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "min_volume": args.min_volume,
            "target_sizes": target_sizes,
            "within_hours": within_hours,
            "all_horizons": args.all_horizons,
            "config_path": args.config or "config.yaml",
            "limit_events": args.limit_events,
            "max_results": args.max_results,
            "neg_risk_only": args.neg_risk_only,
            "correlated_only": args.correlated_only,
            "rank_by": args.rank_by,
        },
        "source_counts": {
            "events_fetched": len(events),
            "events": len(scanned_events),
            "markets": sum(len(event.markets) for event in scanned_events),
            "books": len(books_by_token),
            "fee_rates": len(fee_rates_by_token),
        },
        "opportunities": [opportunity.to_dict() for opportunity in scored],
    }


def parse_target_sizes(value: str) -> List[float]:
    sizes: List[float] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        size = float(item)
        if size <= 0:
            raise argparse.ArgumentTypeError("target sizes must be positive")
        sizes.append(size)
    if not sizes:
        raise argparse.ArgumentTypeError("at least one target size is required")
    return sizes


def format_human(report: Dict[str, Any]) -> str:
    lines = [
        "Polyarb research scan",
        f"Generated: {report['generated_at']}",
        (
            f"Sources: {report['source_counts']['events']} scanned events "
            f"({report['source_counts'].get('events_fetched', report['source_counts']['events'])} fetched), "
            f"{report['source_counts']['markets']} markets, "
            f"{report['source_counts']['books']} books"
        ),
        "Horizon: "
        + ("all" if report["config"]["within_hours"] is None else f"{report['config']['within_hours']:.1f} hours"),
        f"Opportunities: {len(report['opportunities'])}",
    ]
    if not report["opportunities"]:
        lines.append("No high-confidence structural candidates found.")
        return "\n".join(lines)

    for opportunity in report["opportunities"]:
        lines.append("")
        lines.append(
            f"#{opportunity['rank']} {opportunity['type']} | score {opportunity['score']:.2f} | confidence {opportunity['confidence']:.2f}"
        )
        lines.append(f"Event: {opportunity['event']['title']}")
        lines.append(f"Theoretical: {format_theoretical(opportunity)}")
        lines.append(f"Executable: {format_execution(opportunity['execution_by_size'])}")
        lines.append(
            "Liquidity: "
            f"volume ${opportunity['liquidity'].get('event_volume', 0):,.0f}, "
            f"liquidity ${opportunity['liquidity'].get('event_liquidity', 0):,.0f}"
        )
        warnings = ", ".join(opportunity["warnings"]) if opportunity["warnings"] else "none"
        lines.append(f"Warnings: {warnings}")
        lines.append(f"Why: {opportunity['explanation']}")

    return "\n".join(lines)


def format_theoretical(opportunity: Dict[str, Any]) -> str:
    theoretical = opportunity["theoretical"]
    if "sum_yes" in theoretical:
        return (
            f"sum Yes {theoretical['sum_yes']:.4f}, "
            f"residual/edge {theoretical['edge']:+.4f}"
        )
    return (
        f"easier Yes {theoretical['easier_yes']:.4f}, "
        f"harder Yes {theoretical['harder_yes']:.4f}, "
        f"violation {theoretical['violation']:+.4f}, "
        f"package edge {theoretical['edge']:+.4f}"
    )


def format_watch_human(report: Dict[str, Any]) -> str:
    diff = report.get("diff", {})
    lines = [
        "Polyarb watch (bootstrap --once)",
        f"Generated: {report['generated_at']}",
        (
            f"Sources: {report['source_counts']['events']} scanned events "
            f"({report['source_counts'].get('events_fetched', 0)} fetched), "
            f"{report['source_counts']['markets']} markets, "
            f"{report['source_counts']['books']} books"
        ),
        f"Diff: {len(diff.get('new', []))} new, "
        f"{len(diff.get('changed', []))} changed, "
        f"{len(diff.get('closed', []))} closed",
        f"Opportunities tracked: {len(report['opportunities'])}",
    ]
    for opportunity in report["opportunities"]:
        lines.append("")
        lines.append(
            f"#{opportunity['rank']} {opportunity['type']} | score {opportunity['score']:.2f} "
            f"| confidence {opportunity['confidence']:.2f}"
        )
        lines.append(f"Event: {opportunity['event']['title']}")
        lines.append(f"Theoretical: {format_theoretical(opportunity)}")
        lines.append(f"Executable: {format_execution(opportunity['execution_by_size'])}")
    return "\n".join(lines)


def format_execution(execution_by_size: Dict[str, Dict[str, Any]]) -> str:
    chunks = []
    for size, estimate in execution_by_size.items():
        if estimate["executable"]:
            chunks.append(
                f"${size}: net ${estimate['net_cost']:.2f} "
                f"(fees ${estimate['fee_cost']:.2f}), edge ${estimate['edge']:+.2f} ({estimate['edge_pct']:+.2%})"
            )
        else:
            chunks.append(f"${size}: insufficient depth")
    return "; ".join(chunks)


if __name__ == "__main__":
    raise SystemExit(main())
