"""Use case 1 report: answer "how are we doing against target?" from the gold mart.

python -m dataplatform.gold.report
"""

from __future__ import annotations

from dataplatform.config import Settings
from dataplatform.lakehouse import delta, duck
from dataplatform.lakehouse.storage import storage_options

ICON = {
    "hit": "✅ hit",
    "missed": "❌ missed",
    "on_track": "🟢 on track",
    "at_risk": "🟡 at risk",
    "behind": "🔴 behind",
}


def _usd(x) -> str:
    return f"${x:,.0f}"


def main() -> None:
    settings = Settings.from_env()
    storage = storage_options(settings, settings.gold_uri)
    con = duck.connect()
    con.register("mart", delta.query({"m": f"{settings.gold_uri}/mart_sales_vs_target"}, "SELECT * FROM m", storage))
    con.register("fct", delta.query({"f": f"{settings.gold_uri}/fct_order_items"}, "SELECT * FROM f", storage))

    print("\nSALES PERFORMANCE VS. TARGET (USD)\n")
    for (month,) in con.execute("SELECT DISTINCT month_start FROM mart ORDER BY 1").fetchall():
        rev, tgt, status, days, dim = con.execute(
            "SELECT sum(revenue_usd), sum(target_usd), any_value(period_status), any_value(days_elapsed), "
            "any_value(days_in_month) FROM mart WHERE month_start = ?",
            [month],
        ).fetchone()
        label = f"{month:%B %Y}" + ("" if status == "closed" else f"  (day {days}/{dim}, projected)")
        print(f"{label}\n  total {_usd(rev)} vs target {_usd(tgt)} → {rev / tgt:.0%}")
        rows = con.execute(
            "SELECT category, revenue_usd, target_usd, attainment_pct, projected_attainment_pct, status "
            "FROM mart WHERE month_start = ? ORDER BY attainment_pct DESC",
            [month],
        ).fetchall()
        for cat, r, t, att, proj, st in rows:
            extra = "" if status == "closed" else f"  → projected {proj:.0%}"
            print(f"  {cat:12} {_usd(r):>10} / {_usd(t):>9}  {att:6.0%}{extra}   {ICON[st]}")
        print()

    print("INSIGHTS")
    current = con.execute("SELECT max(month_start) FROM mart").fetchone()[0]
    short = con.execute(
        "SELECT category, target_usd - projected_revenue_usd, status FROM mart "
        "WHERE month_start = ? AND status IN ('missed', 'behind', 'at_risk') ORDER BY 2 DESC",
        [current],
    ).fetchall()
    for cat, gap, st in short:
        verb = "missed" if st == "missed" else "is projected to miss"
        print(f"  • {cat} {verb} its {current:%B} target by {_usd(gap)}.")
    n_months = con.execute("SELECT count(DISTINCT month_start) FROM mart").fetchone()[0]
    for label, statuses in [("hit (or is on track for)", "('hit', 'on_track')"), ("missed", "('missed', 'behind')")]:
        for (cat,) in con.execute(
            f"SELECT category FROM mart WHERE status IN {statuses} GROUP BY 1 HAVING count(*) = {n_months}"
        ).fetchall():
            print(f"  • {cat} has {label} target in all {n_months} months.")
    fx = con.execute(
        "SELECT currency, sum(amount_usd) / (SELECT sum(amount_usd) FROM fct) FROM fct "
        "WHERE currency <> 'USD' GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    share = sum(s for _, s in fx)
    print(f"  • {share:.0%} of revenue is booked in foreign currencies ({', '.join(c for c, _ in fx)});")
    print("    it is converted at daily ECB rates, so FX moves change reported USD revenue.")


if __name__ == "__main__":
    main()
