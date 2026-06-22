import sys
import requests
import numpy as np
from scipy import stats
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
import os

sys.path.insert(0, str(Path(__file__).parent))

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY = os.getenv("KALSHI_API_KEY")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

def get_open_buckets():
    r = requests.get(f"{BASE_URL}/markets", headers=headers,
        params={"series_ticker": "KXHIGHNY", "status": "open", "limit": 50})
    markets = r.json().get("markets", [])
    events = {}
    for m in markets:
        event = m.get("event_ticker", "")
        if event not in events:
            events[event] = []
        events[event].append(m)
    if not events:
        return [], None
    next_event = min(events.keys(),
        key=lambda e: events[e][0].get("close_time", "9999"))
    buckets = events[next_event]
    target_date = buckets[0].get("close_time", "")[:10]
    return buckets, target_date

def get_mid(m):
    bid = m.get("yes_bid_dollars")
    ask = m.get("yes_ask_dollars")
    if bid and ask:
        return (float(bid) + float(ask)) / 2
    return None

def extract_market_distribution(buckets):
    range_buckets = []
    threshold_above = None
    threshold_below = None

    for m in buckets:
        floor = m.get("floor_strike")
        cap = m.get("cap_strike")
        mid = get_mid(m)
        if mid is None:
            continue
        if floor and cap:
            range_buckets.append({
                "floor": float(floor),
                "cap": float(cap),
                "mid_temp": (float(floor) + float(cap)) / 2,
                "raw_prob": mid
            })
        elif floor and not cap:
            threshold_above = {"floor": float(floor), "raw_prob": mid}
        elif cap and not floor:
            threshold_below = {"cap": float(cap), "raw_prob": mid}

    total = sum(b["raw_prob"] for b in range_buckets)
    if threshold_above:
        total += threshold_above["raw_prob"]
    if threshold_below:
        total += threshold_below["raw_prob"]

    for b in range_buckets:
        b["prob"] = b["raw_prob"] / total
    if threshold_above:
        threshold_above["prob"] = threshold_above["raw_prob"] / total
    if threshold_below:
        threshold_below["prob"] = threshold_below["raw_prob"] / total

    return range_buckets, threshold_above, threshold_below

def market_implied_stats(range_buckets, thresh_above=None, thresh_below=None):
    """
    Compute mean and std using ALL buckets including thresholds.
    Threshold buckets get assigned a representative temperature.
    """
    all_points = []

    for b in range_buckets:
        all_points.append((b["mid_temp"], b["prob"]))

    if thresh_above:
        rep_temp = thresh_above["floor"] + 2
        all_points.append((rep_temp, thresh_above["prob"]))

    if thresh_below:
        rep_temp = thresh_below["cap"] - 2
        all_points.append((rep_temp, thresh_below["prob"]))

    total_prob = sum(p for _, p in all_points)
    mean = sum(t * p for t, p in all_points) / total_prob
    variance = sum(p * (t - mean)**2 for t, p in all_points) / total_prob
    return mean, variance ** 0.5

def get_nws_forecast(target_date):
    try:
        r = requests.get(
            "https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly",
            headers={"User-Agent": "kalshi-research"}, timeout=10)
        periods = r.json()["properties"]["periods"]
        day_temps = [p["temperature"] for p in periods
                     if target_date in p["startTime"]]
        if not day_temps:
            return None, None
        nws_high = max(day_temps)
        temp_range = max(day_temps) - min(day_temps)
        nws_sigma = 3.5 if temp_range > 15 else 4.5
        return nws_high, nws_sigma
    except:
        return None, None

def conditional_nws_posterior(market_mean, market_std, nws_mean, nws_std):
    """
    Blend NWS only when its forecast is outside the market's 1-sigma range.
    Weight scales from 0% (at 1σ divergence) to 30% (at ≥3σ divergence).
    When NWS agrees with the market, returns market distribution unchanged.
    """
    if nws_mean is None or nws_std is None:
        return market_mean, market_std
    divergence = abs(nws_mean - market_mean) / market_std
    if divergence < 1.0:
        return market_mean, market_std
    nws_weight = min(0.30, (divergence - 1.0) * 0.15)
    mkt_weight = 1.0 - nws_weight
    mkt_prec   = mkt_weight / market_std ** 2
    nws_prec   = nws_weight / nws_std ** 2
    post_prec  = mkt_prec + nws_prec
    post_mean  = (mkt_prec * market_mean + nws_prec * nws_mean) / post_prec
    post_std   = (1.0 / post_prec) ** 0.5
    return post_mean, post_std


def load_empirical_prior(min_obs=5):
    """
    Historical win rate per (floor, cap) bucket from the observations table.
    Only returns rates for buckets with ≥min_obs settlements.
    """
    try:
        import db
        obs = db.get_all_observations()
    except Exception:
        return {}
    counts, wins = {}, {}
    for o in obs:
        if o.get('result') not in ('yes', 'no'):
            continue
        f, c = o.get('floor'), o.get('cap')
        if f is None or c is None:
            continue
        key = (float(f), float(c))
        counts[key] = counts.get(key, 0) + 1
        if o['result'] == 'yes':
            wins[key] = wins.get(key, 0) + 1
    return {k: wins.get(k, 0) / counts[k] for k in counts if counts[k] >= min_obs}

def posterior_bucket_prob(floor, cap, mu, sigma):
    return stats.norm(mu, sigma).cdf(cap) - stats.norm(mu, sigma).cdf(floor)

def posterior_above_prob(threshold, mu, sigma):
    return 1 - stats.norm(mu, sigma).cdf(threshold)

def posterior_below_prob(threshold, mu, sigma):
    return stats.norm(mu, sigma).cdf(threshold)

def breakeven_yes(price, fee=0.07):
    return price / ((1 - price) * (1 - fee) + price)

def breakeven_no(yes_price, fee=0.07):
    return (1 - yes_price) / (yes_price * (1 - fee) + (1 - yes_price))

# ── Main ──────────────────────────────────────────────────────────────────

print("=" * 65)
print("FAIR VALUE MODEL v2 — Bayesian Market + NWS Update")
print(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
print("=" * 65)

buckets, target_date = get_open_buckets()
if not buckets:
    print("No open markets")
    exit()

print(f"\nTarget date: {target_date}")

range_buckets, thresh_above, thresh_below = extract_market_distribution(buckets)

# ── Stale market check ────────────────────────────────────────────────────
max_range_price = max(b["raw_prob"] for b in range_buckets) if range_buckets else 0
min_range_price = min(b["raw_prob"] for b in range_buckets) if range_buckets else 0

if max_range_price < 0.05 or max_range_price > 0.90:
    print("\n⚠️  Market is near resolution.")
    if max_range_price > 0.90:
        print(f"   One bucket at {max_range_price*100:.0f}¢ — market has already decided.")
    else:
        print("   All buckets below 5¢ — market near resolution.")
    print("   Run this model before noon ET (14:00 UTC) for meaningful output.")
    exit()

market_mean, market_std = market_implied_stats(range_buckets, thresh_above, thresh_below)

print(f"\nMarket implied distribution:")
print(f"  Mean: {market_mean:.1f}°F")
print(f"  Std:  {market_std:.1f}°F")

nws_high, nws_sigma = get_nws_forecast(target_date)
print(f"\nNWS forecast:")
print(f"  High:  {nws_high}°F")
print(f"  Sigma: {nws_sigma}°F")
if nws_high and market_mean:
    div_sigma = abs(nws_high - market_mean) / market_std if market_std else 0
    print(f"  Divergence: {nws_high - market_mean:+.1f}°F  ({div_sigma:.1f}σ from market)")

post_mean, post_std = conditional_nws_posterior(market_mean, market_std, nws_high, nws_sigma)
nws_active = (post_mean != market_mean)
if nws_active:
    nws_weight = min(0.30, (abs(nws_high - market_mean) / market_std - 1.0) * 0.15)
    print(f"\nPosterior = Market + NWS blend (NWS weight: {nws_weight*100:.0f}%):")
else:
    print(f"\nPosterior = Market distribution (NWS within 1σ — not blended):")
print(f"  Mean: {post_mean:.1f}°F")
print(f"  Std:  {post_std:.1f}°F")

emp_prior = load_empirical_prior()
if emp_prior:
    print(f"\nEmpirical prior: {len(emp_prior)} buckets with ≥5 historical observations")

edge_buffer = 0.03
print(f"\n{'Bucket':<12} {'Mkt%':>6} {'Emp%':>6} {'Fair%':>6} {'Edge':>7} {'Signal':>14}")
print("-" * 62)

trades = []

for b in sorted(range_buckets, key=lambda x: x["floor"]):
    floor, cap = b["floor"], b["cap"]
    mkt_prob  = b["raw_prob"]
    pp_gauss  = posterior_bucket_prob(floor, cap, post_mean, post_std)
    emp_rate  = emp_prior.get((float(floor), float(cap)))
    # Composite fair value: empirical shifts gaussian by at most ±0.15
    if emp_rate is not None:
        adj       = max(-0.15, min(0.15, emp_rate - pp_gauss))
        fair_prob = pp_gauss + 0.40 * adj
    else:
        fair_prob = pp_gauss

    edge_yes = fair_prob - mkt_prob
    be_yes = breakeven_yes(mkt_prob)
    be_no  = breakeven_no(mkt_prob)
    label  = f"{floor:.0f}-{cap:.0f}°"
    emp_str = f"{emp_rate*100:.0f}%" if emp_rate is not None else "  N/A"

    if fair_prob > be_yes + edge_buffer and (emp_rate is None or emp_rate >= mkt_prob):
        signal = "⚡ BUY YES"
        trades.append(("YES", label, mkt_prob, fair_prob, fair_prob - be_yes))
    elif (1 - fair_prob) > be_no + edge_buffer and (emp_rate is None or emp_rate <= mkt_prob):
        signal = "⚡ BUY NO"
        trades.append(("NO", label, mkt_prob, fair_prob, (1-fair_prob) - be_no))
    else:
        signal = "— pass"

    print(f"{label:<12} {mkt_prob*100:>5.1f}% {emp_str:>6} "
          f"{fair_prob*100:>5.1f}% {edge_yes*100:>+6.1f}%  {signal}")

if thresh_above:
    floor    = thresh_above["floor"]
    mkt_prob = thresh_above["raw_prob"]
    pp_gauss = posterior_above_prob(floor, post_mean, post_std)
    fair_prob = pp_gauss  # no empirical prior for threshold buckets
    edge     = fair_prob - mkt_prob
    label    = f">{floor:.0f}°"
    be_yes   = breakeven_yes(mkt_prob)
    if fair_prob > be_yes + edge_buffer:
        signal = "⚡ BUY YES"
        trades.append(("YES", label, mkt_prob, fair_prob, fair_prob - be_yes))
    elif (1 - fair_prob) > breakeven_no(mkt_prob) + edge_buffer:
        signal = "⚡ BUY NO"
    else:
        signal = "— pass"
    print(f"{label:<12} {mkt_prob*100:>5.1f}%    N/A "
          f"{fair_prob*100:>5.1f}% {edge*100:>+6.1f}%  {signal}")

if thresh_below:
    cap      = thresh_below["cap"]
    mkt_prob = thresh_below["raw_prob"]
    pp_gauss = posterior_below_prob(cap, post_mean, post_std)
    fair_prob = pp_gauss
    edge     = fair_prob - mkt_prob
    label    = f"<{cap:.0f}°"
    be_yes   = breakeven_yes(mkt_prob)
    if fair_prob > be_yes + edge_buffer:
        signal = "⚡ BUY YES"
        trades.append(("YES", label, mkt_prob, fair_prob, fair_prob - be_yes))
    elif (1 - fair_prob) > breakeven_no(mkt_prob) + edge_buffer:
        signal = "⚡ BUY NO"
    else:
        signal = "— pass"
    print(f"{label:<12} {mkt_prob*100:>5.1f}%    N/A "
          f"{fair_prob*100:>5.1f}% {edge*100:>+6.1f}%  {signal}")

print(f"\n{'='*65}")
print("TRADE RECOMMENDATIONS")
print(f"{'='*65}")

if trades:
    for direction, bucket, price, fv, edge in trades:
        cost_per = price if direction == "YES" else (1 - price)
        contracts = max(1, min(10, int(0.10 * 10 / cost_per)))
        cost = contracts * cost_per
        print(f"\n  {direction} on {bucket}")
        print(f"  Market price:   {price*100:.1f}¢")
        print(f"  Posterior prob: {fv*100:.1f}%")
        print(f"  Edge after fee: {edge*100:.1f}%")
        print(f"  Suggested:      {contracts} contracts @ ${cost:.2f} total")
else:
    print("  No trades recommended — insufficient edge")

print(f"\nNote: Composite model — market gaussian + empirical bucket prior.")
print(f"NWS blended when forecast diverges >1σ from market (up to 30% weight).")