"""
backtest_v2.py — Backtesting engine using settled observations from SQLite.

Signal:  market-only Gaussian posterior fitted to market prices, edge > 15% vs fee-adjusted breakeven
Sizing:  half-Kelly (0.5), capped at 15% of balance, $10 starting
Fee:     7%

Usage:   python3 src/backtest_v2.py
"""
import sys
import json
import numpy as np
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path
from scipy import stats

sys.path.insert(0, str(Path(__file__).parent))
import db

STARTING_BALANCE = 10.0
FEE_RATE         = 0.07
EDGE_THRESHOLD   = 0.15   # 15 percentage-point edge after fee breakeven
KELLY_FRACTION   = 0.50   # half-Kelly
MAX_POSITION_PCT = 0.15   # cap at 15% of current balance
TRAIN_FRACTION   = 0.60   # 60/40 walk-forward split
RESULTS_PATH     = Path(__file__).parent.parent / "data" / "backtest_results.json"


# ── Distribution helpers ───────────────────────────────────────────────────

def market_gaussian(buckets: list[dict]) -> tuple[float | None, float | None]:
    """Fit a Gaussian to normalized market prices. Returns (mean, sigma)."""
    pts = [(( b["floor"] + b["cap"]) / 2, b["norm_prob"]) for b in buckets]
    total = sum(p for _, p in pts)
    if total == 0:
        return None, None
    mean = sum(t * p for t, p in pts) / total
    var  = sum(p * (t - mean) ** 2 for t, p in pts) / total
    return mean, max(var ** 0.5, 0.5)   # floor sigma at 0.5°F


def gaussian_bucket_prob(floor: float, cap: float, mu: float, sigma: float) -> float:
    return float(stats.norm(mu, sigma).cdf(cap) - stats.norm(mu, sigma).cdf(floor))


def breakeven_yes(price: float) -> float:
    return price / ((1 - price) * (1 - FEE_RATE) + price)


def breakeven_no(price: float) -> float:
    return (1 - price) / (price * (1 - FEE_RATE) + (1 - price))


def half_kelly_contracts(direction: str, fair_prob: float, mkt_price: float,
                         balance: float) -> int:
    if direction == "YES":
        b = (1 - mkt_price) * (1 - FEE_RATE) / mkt_price
        p = fair_prob
    else:
        no_price = 1 - mkt_price
        b = mkt_price * (1 - FEE_RATE) / no_price
        p = 1 - fair_prob
    q = 1 - p
    raw_kelly = (p * b - q) / b if b > 0 else 0.0
    frac      = min(max(raw_kelly * KELLY_FRACTION, 0.0), MAX_POSITION_PCT)
    cost_per  = mkt_price if direction == "YES" else (1 - mkt_price)
    if cost_per <= 0:
        return 0
    return max(1, int(balance * frac / cost_per))


# ── Data loading ───────────────────────────────────────────────────────────

def load_settled_days() -> dict[str, list[dict]]:
    """
    Returns {target_date: [bucket_obs]} for days with ≥2 settled buckets
    and exactly one YES result (fully resolved event).
    """
    all_obs = db.get_all_observations()
    by_date: dict[str, list] = defaultdict(list)
    for o in all_obs:
        if o.get("result") not in ("yes", "no"):
            continue
        if o.get("floor") is None or o.get("cap") is None:
            continue
        price = o.get("price_mid")
        if not price or price <= 0:
            continue
        by_date[o["target_date"]].append(o)

    valid = {}
    for date, buckets in sorted(by_date.items()):
        yes_count = sum(1 for b in buckets if b["result"] == "yes")
        if len(buckets) >= 2 and yes_count == 1:
            valid[date] = buckets
    return valid


# ── Simulation ─────────────────────────────────────────────────────────────

def _normalize(buckets: list[dict]) -> list[dict]:
    """Add norm_prob field (market probabilities normalised to sum to 1)."""
    total = sum(b["price_mid"] for b in buckets)
    return [{**b, "norm_prob": b["price_mid"] / total} for b in buckets]


def simulate_day(raw_buckets: list[dict], balance: float) -> list[dict]:
    """Signal + size one day. Returns list of trade dicts (may be empty)."""
    buckets = _normalize(raw_buckets)

    # Skip if market has basically resolved
    max_price = max(b["price_mid"] for b in buckets)
    if max_price > 0.90 or max_price < 0.05:
        return []

    mu, sigma = market_gaussian(buckets)
    if mu is None:
        return []

    trades = []
    for b in sorted(buckets, key=lambda x: x["floor"]):
        mkt   = b["price_mid"]
        fair  = gaussian_bucket_prob(b["floor"], b["cap"], mu, sigma)
        bey   = breakeven_yes(mkt)
        ben   = breakeven_no(mkt)

        if fair - bey >= EDGE_THRESHOLD:
            direction, edge = "YES", fair - bey
        elif (1 - fair) - ben >= EDGE_THRESHOLD:
            direction, edge = "NO", (1 - fair) - ben
        else:
            continue

        n        = half_kelly_contracts(direction, fair, mkt, balance)
        cost_per = mkt if direction == "YES" else (1 - mkt)
        cost     = n * cost_per
        if cost > balance:                # hard cap: never bet more than balance
            n    = max(1, int(balance / cost_per))
            cost = n * cost_per

        won = (direction == "YES" and b["result"] == "yes") or \
              (direction == "NO"  and b["result"] == "no")

        if direction == "YES":
            pnl = n * (1 - mkt) * (1 - FEE_RATE) if won else -cost
        else:
            pnl = n * mkt * (1 - FEE_RATE) if won else -cost

        trades.append({
            "target_date": b["target_date"],
            "ticker":      b["ticker"],
            "floor":       b["floor"],
            "cap":         b["cap"],
            "direction":   direction,
            "mkt_price":   round(mkt,  4),
            "fair_prob":   round(fair, 4),
            "edge":        round(edge, 4),
            "contracts":   n,
            "cost":        round(cost, 4),
            "won":         won,
            "pnl":         round(pnl,  4),
            "result":      b["result"],
        })
    return trades


def run_simulation(days: dict[str, list[dict]]) -> list[dict]:
    """Simulate all days chronologically. Tracks running balance."""
    balance    = STARTING_BALANCE
    all_trades = []
    for date, buckets in days.items():
        for t in simulate_day(buckets, balance):
            balance += t["pnl"]
            all_trades.append({**t, "balance_after": round(balance, 4)})
    return all_trades


# ── Metrics ────────────────────────────────────────────────────────────────

def compute_metrics(trades: list[dict], all_dates: list[str]) -> dict:
    if not trades:
        return {
            "n_trades": 0, "win_rate": 0.0, "total_pnl": 0.0,
            "roi_pct": 0.0, "sharpe": 0.0, "max_drawdown": 0.0,
            "final_balance": STARTING_BALANCE,
        }
    pnl_by_date: dict[str, float] = defaultdict(float)
    for t in trades:
        pnl_by_date[t["target_date"]] += t["pnl"]

    daily_ret = [pnl_by_date.get(d, 0.0) / STARTING_BALANCE for d in all_dates]
    arr       = np.array(daily_ret)
    sharpe    = float(arr.mean() / arr.std() * np.sqrt(252)) if arr.std() > 0 else 0.0

    peak, max_dd, bal = STARTING_BALANCE, 0.0, STARTING_BALANCE
    for t in trades:
        bal  += t["pnl"]
        peak  = max(peak, bal)
        max_dd = max(max_dd, (peak - bal) / peak)

    total_pnl = sum(t["pnl"] for t in trades)
    wins      = sum(1 for t in trades if t["won"])
    return {
        "n_trades":      len(trades),
        "win_rate":      round(wins / len(trades), 4),
        "total_pnl":     round(total_pnl, 4),
        "roi_pct":       round(100 * total_pnl / STARTING_BALANCE, 2),
        "sharpe":        round(sharpe, 3),
        "max_drawdown":  round(max_dd, 4),
        "final_balance": round(STARTING_BALANCE + total_pnl, 4),
    }


def roi_by_month(trades: list[dict]) -> dict[str, dict]:
    months: dict[str, dict] = defaultdict(lambda: {"pnl": 0.0, "wins": 0, "n": 0})
    for t in trades:
        m = t["target_date"][:7]
        months[m]["pnl"]  += t["pnl"]
        months[m]["n"]    += 1
        months[m]["wins"] += int(t["won"])
    return {
        m: {
            "pnl":      round(d["pnl"], 4),
            "n_trades": d["n"],
            "win_rate": round(d["wins"] / d["n"], 4) if d["n"] else 0.0,
            "roi_pct":  round(100 * d["pnl"] / STARTING_BALANCE, 2),
        }
        for m, d in sorted(months.items())
    }


# ── Calibration curve ──────────────────────────────────────────────────────

def calibration_curve(days: dict[str, list[dict]]) -> list[dict]:
    """
    For every 10-cent price bucket (across ALL settled observations),
    report: count, wins, actual win%, implied win% (bucket midpoint), edge.
    """
    BIN_EDGES = list(range(0, 100, 10))
    bins: dict[int, dict] = {lo: {"count": 0, "wins": 0} for lo in BIN_EDGES}

    for buckets in days.values():
        for b in buckets:
            p_cents = b["price_mid"] * 100
            for lo in reversed(BIN_EDGES):
                if p_cents >= lo:
                    bins[lo]["count"] += 1
                    if b["result"] == "yes":
                        bins[lo]["wins"] += 1
                    break

    curve = []
    for lo in BIN_EDGES:
        hi = lo + 10
        d  = bins[lo]
        if d["count"] == 0:
            continue
        actual_pct  = 100 * d["wins"] / d["count"]
        implied_pct = lo + 5   # midpoint of bin
        curve.append({
            "bucket":      f"{lo}-{hi}¢",
            "price_lo":    lo / 100,
            "price_hi":    hi / 100,
            "count":       d["count"],
            "wins":        d["wins"],
            "actual_pct":  round(actual_pct, 1),
            "implied_pct": float(implied_pct),
            "edge_pct":    round(actual_pct - implied_pct, 1),
        })
    return curve


# ── Walk-forward ───────────────────────────────────────────────────────────

def walk_forward(days: dict[str, list[dict]]) -> dict:
    all_dates = sorted(days.keys())
    split     = int(len(all_dates) * TRAIN_FRACTION)
    train_dates, test_dates = all_dates[:split], all_dates[split:]

    train_days = {d: days[d] for d in train_dates}
    test_days  = {d: days[d] for d in test_dates}

    train_trades = run_simulation(train_days)
    test_trades  = run_simulation(test_days)

    return {
        "split_date":   test_dates[0] if test_dates else None,
        "train_events": len(train_dates),
        "test_events":  len(test_dates),
        "train":        compute_metrics(train_trades, train_dates),
        "test":         compute_metrics(test_trades,  test_dates),
        "train_equity": _equity_curve(train_trades, train_dates),
        "test_equity":  _equity_curve(test_trades,  test_dates),
    }


def _equity_curve(trades: list[dict], dates: list[str]) -> list[dict]:
    pnl_by_date: dict[str, float] = defaultdict(float)
    for t in trades:
        pnl_by_date[t["target_date"]] += t["pnl"]
    bal, curve = STARTING_BALANCE, []
    for d in dates:
        bal += pnl_by_date.get(d, 0.0)
        curve.append({"date": d, "balance": round(bal, 4)})
    return curve


# ── Report printer ─────────────────────────────────────────────────────────

def _hr(char="─", w=64):
    print(char * w)


def print_report(days, all_trades, metrics, monthly, calib, wf):
    all_dates = sorted(days.keys())

    # Count days actually traded (not skipped by stale guard)
    traded_days = len(set(t["target_date"] for t in all_trades))
    stale_days  = len(days) - sum(
        1 for buckets in days.values()
        if max(b["price_mid"] for b in buckets) <= 0.90
    )

    print()
    _hr("═")
    print("BACKTEST v2 — KXHIGHNY  |  Market-Only Gaussian Posterior")
    print(f'Run: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}')
    _hr("═")
    print(f"\nData:   {len(days)} settled days  |  {all_dates[0]} → {all_dates[-1]}")
    print(f"        {stale_days} days skipped (winner priced >90¢ — collected after resolution)")
    print(f"        {len(days)-stale_days} days with pre-resolution prices (usable for signal)")
    print(f"Signal: Gaussian posterior fitted to market prices  |  edge ≥ {EDGE_THRESHOLD*100:.0f}% (after fee breakeven)")
    print(f"Sizing: half-Kelly  |  max {MAX_POSITION_PCT*100:.0f}% of balance  |  {FEE_RATE*100:.0f}% fee")
    print(f"Start:  ${STARTING_BALANCE:.2f}")

    _hr()
    print("OVERALL PERFORMANCE")
    _hr()
    if metrics["n_trades"] == 0:
        print(f"  No trades generated at {EDGE_THRESHOLD*100:.0f}% edge threshold.")
        print(f"  (Market-only Gaussian closely tracks market prices by construction.)")
        print(f"  Consider running with a lower EDGE_THRESHOLD to see signal activity.")
    else:
        print(f"  Trades:         {metrics['n_trades']}")
        print(f"  Win rate:       {metrics['win_rate']*100:.1f}%")
        print(f"  Total P&L:      ${metrics['total_pnl']:+.4f}")
        print(f"  ROI:            {metrics['roi_pct']:+.2f}%  (on ${STARTING_BALANCE:.2f})")
        print(f"  Final balance:  ${metrics['final_balance']:.4f}")
        print(f"  Sharpe:         {metrics['sharpe']:+.3f}  (annualized daily)")
        print(f"  Max drawdown:   {metrics['max_drawdown']*100:.1f}%")

    if monthly:
        _hr()
        print("ROI BY MONTH")
        _hr()
        print(f"  {'Month':<10} {'Trades':>7} {'Win%':>7} {'P&L':>10} {'ROI':>8}")
        print(f"  {'─'*46}")
        for month, m in monthly.items():
            bar = ("▓" * int(abs(m["pnl"]) * 10)) if m["pnl"] > 0 else ("░" * int(abs(m["pnl"]) * 10))
            bar = bar[:10]
            sign = "+" if m["pnl"] >= 0 else ""
            print(f"  {month:<10} {m['n_trades']:>7} {m['win_rate']*100:>6.1f}%  "
                  f"${m['pnl']:>+7.4f} {m['roi_pct']:>+7.2f}%  {bar}")

    _hr()
    print("CALIBRATION CURVE  (all observations, by 10-cent price bucket)")
    _hr()
    print(f"  {'Bucket':<10} {'N':>5} {'Wins':>5} {'Actual%':>9} {'Implied%':>10} {'Edge':>7}")
    print(f"  {'─'*50}")
    for c in calib:
        edge_str = f"{c['edge_pct']:+.1f}%"
        note = ""
        if c["implied_pct"] >= 40:
            note = " ← favorite"
        elif c["implied_pct"] < 20:
            note = " ← tail"
        print(f"  {c['bucket']:<10} {c['count']:>5} {c['wins']:>5} "
              f"{c['actual_pct']:>8.1f}%  {c['implied_pct']:>9.1f}%  {edge_str:>7}{note}")
    total_obs = sum(c["count"] for c in calib)
    total_wins = sum(c["wins"] for c in calib)
    print(f"  {'─'*50}")
    print(f"  {'TOTAL':<10} {total_obs:>5} {total_wins:>5}  "
          f"{100*total_wins/total_obs:>8.1f}%")
    if total_obs < 200:
        print(f"\n  ⚠  Only {total_obs} observations — need ~200+ for statistical confidence.")

    _hr()
    print(f"WALK-FORWARD VALIDATION  (60% train / 40% test)")
    _hr()
    ti, te = wf["train"], wf["test"]
    print(f"  Split at:      {wf['split_date']}")
    print(f"  Train events:  {wf['train_events']}  |  Test events: {wf['test_events']}")
    print()
    print(f"  {'Metric':<22} {'In-sample':>12} {'Out-of-sample':>14}")
    print(f"  {'─'*52}")

    def _wf_row(label, iv, ov, fmt):
        print(f"  {label:<22} {fmt.format(iv):>12}   {fmt.format(ov):>13}")

    _wf_row("Trades",       ti["n_trades"],              te["n_trades"],              "{}")
    _wf_row("Win rate",     f"{ti['win_rate']*100:.1f}%", f"{te['win_rate']*100:.1f}%", "{}")
    _wf_row("Total P&L",   f"${ti['total_pnl']:+.4f}",   f"${te['total_pnl']:+.4f}",   "{}")
    _wf_row("ROI",         f"{ti['roi_pct']:+.2f}%",     f"{te['roi_pct']:+.2f}%",     "{}")
    _wf_row("Sharpe",      f"{ti['sharpe']:+.3f}",       f"{te['sharpe']:+.3f}",       "{}")
    _wf_row("Max drawdown", f"{ti['max_drawdown']*100:.1f}%", f"{te['max_drawdown']*100:.1f}%", "{}")

    if ti.get("sharpe", 0) != 0:
        decay = (ti["sharpe"] - te["sharpe"]) / abs(ti["sharpe"])
        print(f"\n  Sharpe decay (IS→OOS): {decay*100:+.1f}%")
        if decay < 0.25:
            verdict = "Strong generalization (decay <25%)"
        elif decay < 0.50:
            verdict = "Moderate generalization (decay 25-50%)"
        else:
            verdict = "Possible overfitting (decay >50%)"
        print(f"  Verdict: {verdict}")
    elif ti.get("n_trades", 0) == 0 and te.get("n_trades", 0) == 0:
        print("\n  No trades in either split — walk-forward not meaningful.")

    if all_trades:
        _hr()
        print("TRADE LOG")
        _hr()
        print(f"  {'Date':<12} {'Ticker':<32} {'Dir':>4} {'Mkt%':>6} {'Fair%':>6} "
              f"{'Edge%':>6} {'N':>3} {'P&L':>8} {'Bal':>8}  W?")
        print(f"  {'─'*88}")
        for t in all_trades:
            won_str = "✓" if t["won"] else "✗"
            print(f"  {t['target_date']:<12} {t['ticker'][-32:]:<32} "
                  f"{t['direction']:>4} {t['mkt_price']*100:>5.1f}% "
                  f"{t['fair_prob']*100:>5.1f}% {t['edge']*100:>5.1f}% "
                  f"{t['contracts']:>3}  ${t['pnl']:>+6.4f}  ${t['balance_after']:>6.4f}  {won_str}")

    print()
    _hr("═")
    if metrics["n_trades"] == 0:
        print("VERDICT: No trades — 15% edge threshold is strict for a market-fitted Gaussian.")
    elif metrics["sharpe"] > 0.5:
        print("VERDICT: Positive edge detected. Continue collecting data.")
    elif metrics["sharpe"] > 0:
        print("VERDICT: Weak positive edge — too few observations for confidence.")
    else:
        print("VERDICT: No detectable edge. Review signal or lower threshold.")
    _hr("═")
    print()


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    print("\nLoading settled observations...")
    days = load_settled_days()
    if len(days) < 5:
        print(f"Only {len(days)} settled days found — need at least 5.")
        return

    all_dates = sorted(days.keys())
    print(f"Found {len(days)} settled days: {all_dates[0]} → {all_dates[-1]}")

    all_trades = run_simulation(days)
    metrics    = compute_metrics(all_trades, all_dates)
    monthly    = roi_by_month(all_trades)
    calib      = calibration_curve(days)
    wf         = walk_forward(days)

    print_report(days, all_trades, metrics, monthly, calib, wf)

    result = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "edge_threshold":   EDGE_THRESHOLD,
            "kelly_fraction":   KELLY_FRACTION,
            "max_position_pct": MAX_POSITION_PCT,
            "fee_rate":         FEE_RATE,
            "starting_balance": STARTING_BALANCE,
        },
        "data": {
            "n_days":     len(days),
            "date_range": [all_dates[0], all_dates[-1]],
        },
        "metrics":           metrics,
        "roi_by_month":      monthly,
        "calibration_curve": calib,
        "walk_forward": {
            "split_date":   wf["split_date"],
            "train_events": wf["train_events"],
            "test_events":  wf["test_events"],
            "train":        wf["train"],
            "test":         wf["test"],
            "train_equity": wf["train_equity"],
            "test_equity":  wf["test_equity"],
        },
        "trades": all_trades,
    }

    RESULTS_PATH.write_text(json.dumps(result, indent=2))
    print(f"Results saved → {RESULTS_PATH}")


if __name__ == "__main__":
    main()
