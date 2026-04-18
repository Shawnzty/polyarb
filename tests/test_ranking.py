from polyarb.models.opportunity import ExecutionEstimate, Opportunity
from polyarb.ranking.scoring import score_opportunities


def make_opportunity(title, edge, executable, warnings):
    return Opportunity(
        type="neg-risk-underround",
        title=title,
        event={"id": title, "title": title, "slug": title},
        markets=[],
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
                leg_count=1,
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
