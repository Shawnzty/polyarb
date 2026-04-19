from datetime import datetime, timedelta, timezone

from polyarb.models.opportunity import ExecutionEstimate, Opportunity, OpportunityMarket
from polyarb.ranking.scoring import score_opportunities


def make_opportunity(title, edge, executable, warnings, end_date=None, leg_count=1):
    markets = []
    if end_date is not None:
        markets.append(
            OpportunityMarket(
                id=f"{title}-m",
                title=title,
                yes_token_id=f"{title}-y",
                no_token_id=f"{title}-n",
                yes_price=None,
                no_price=None,
                volume=0,
                liquidity=0,
                end_date=end_date,
            )
        )
    return Opportunity(
        type="neg-risk-underround",
        title=title,
        event={"id": title, "title": title, "slug": title},
        markets=markets,
        theoretical={"edge": edge},
        execution_by_size={
            "100": ExecutionEstimate(
                target_size=100,
                executable=executable,
                cost=100 - edge * 100 if executable else None,
                payout=100,
                edge=edge * 100 if executable else None,
                edge_pct=edge if executable else None,
                gross_cost=100 - edge * 100 if executable else None,
                fee_cost=0 if executable else None,
                net_cost=100 - edge * 100 if executable else None,
                leg_count=leg_count,
                missing_legs=[] if executable else ["missing"],
            )
        },
        liquidity={"event_volume": 100000, "event_liquidity": 10000},
        confidence=0.95,
        warnings=warnings,
        explanation="fixture",
    )


def test_executable_mediocre_edge_ranks_above_fake_headline_edge():
    executable = make_opportunity("small executable", 0.01, True, [])
    fake = make_opportunity(
        "large fake",
        0.20,
        False,
        ["missing-book", "insufficient-depth", "other-outcome", "augmented-neg-risk"],
    )

    ranked = score_opportunities([fake, executable])

    assert ranked[0].title == "small executable"
    assert ranked[0].score > ranked[1].score


def test_ev_scoring_penalizes_more_legs_and_rule_risk():
    two_leg = make_opportunity("two leg", 0.02, True, [])
    five_leg = make_opportunity("five leg other", 0.02, True, ["other-outcome", "augmented-neg-risk"])
    five_leg.execution_by_size["100"].leg_count = 5

    ranked = score_opportunities([five_leg, two_leg])

    assert ranked[0].title == "two leg"


def test_apy_scoring_favors_shorter_time_to_resolution():
    # Two opportunities, same dollar edge and executability. One resolves in
    # 1h, the other in 1 year. Under APY, the 1h candidate dominates.
    now = datetime(2026, 4, 19, tzinfo=timezone.utc)
    soon = make_opportunity("1h", 0.01, True, [], end_date=(now + timedelta(hours=1)).isoformat())
    later = make_opportunity("1y", 0.01, True, [], end_date=(now + timedelta(days=365)).isoformat())

    ranked = score_opportunities([later, soon], now=now)

    assert ranked[0].title == "1h"
    assert ranked[0].apy > ranked[1].apy


def test_rank_by_edge_dollar_ignores_time_to_resolution():
    now = datetime(2026, 4, 19, tzinfo=timezone.utc)
    soon_small = make_opportunity(
        "1h $5",
        0.05,
        True,
        [],
        end_date=(now + timedelta(hours=1)).isoformat(),
    )
    # Much larger dollar edge (20%) resolving in a year.
    later_big = make_opportunity(
        "1y $20",
        0.20,
        True,
        [],
        end_date=(now + timedelta(days=365)).isoformat(),
    )

    ranked = score_opportunities([soon_small, later_big], rank_by="edge_dollar", now=now)

    assert ranked[0].title == "1y $20"


def test_score_populates_apy_and_capital_at_risk_fields():
    now = datetime(2026, 4, 19, tzinfo=timezone.utc)
    opp = make_opportunity(
        "populate",
        0.02,
        True,
        [],
        end_date=(now + timedelta(days=30)).isoformat(),
    )

    ranked = score_opportunities([opp], now=now)

    result = ranked[0]
    assert result.capital_at_risk == 98
    assert result.edge_pct is not None and result.edge_pct > 0.02 - 1e-6
    assert result.time_to_res_h is not None and 700 < result.time_to_res_h < 721
    assert result.apy is not None and result.apy > 0
