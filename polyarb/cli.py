from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, TextIO

from polyarb.api.clob_client import ClobClient
from polyarb.api.gamma_client import GammaClient, collect_fee_token_ids, collect_market_token_ids
from polyarb.config import load_config
from polyarb.models.event import GammaEvent
from polyarb.models.opportunity import Opportunity
from polyarb.ranking.scoring import score_opportunities
from polyarb.scanners.correlated_scanner import CorrelatedScanner
from polyarb.scanners.neg_risk_scanner import NegRiskScanner
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
    return parser


def main(
    argv: Optional[List[str]] = None,
    gamma_client: Optional[GammaClient] = None,
    clob_client: Optional[ClobClient] = None,
    stdout: Optional[TextIO] = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.neg_risk_only and args.correlated_only:
        parser.error("--neg-risk-only and --correlated-only cannot be used together")

    stdout = stdout or sys.stdout
    if args.command == "scan":
        report = run_scan(args, gamma_client=gamma_client, clob_client=clob_client)
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True), file=stdout)
        else:
            print(format_human(report), file=stdout)
        return 0

    parser.error(f"unknown command {args.command}")
    return 2


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

    opportunities: List[Opportunity] = []
    if not args.correlated_only:
        opportunities.extend(NegRiskScanner(target_sizes, fee_rates_by_token).scan(scanned_events, books_by_token))
    if not args.neg_risk_only:
        opportunities.extend(CorrelatedScanner(target_sizes, fee_rates_by_token).scan(scanned_events, books_by_token))

    scored = score_opportunities(opportunities, config.get("risk", {}))[: args.max_results]
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
