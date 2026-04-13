import requests
import numpy as np
from scipy import stats
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY = os.getenv("KALSHI_API_KEY")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

# ── 1. Build temperature distribution from NWS ───────────────────────────

def get_nws_distribution(target_date):
    """
    NWS gives a deterministic hourly forecast but we can estimate
    uncertainty using:
    1. The spread between max and surrounding hours (sharpness)
    2. Historical NWS forecast error at this lead time (~24h)
    
    NWS 24h forecast MAE for NYC is historically ~3.5°F
    We model the high temp as Normal(nws_predicted_high, sigma)
    where sigma is calibrated from historical error
    """
    try:
        r = requests.get(
            "https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly",
            headers={"User-Agent": "kalshi-research"}, timeout=10
        )
        periods = r.json()["properties"]["periods"]
        day_temps = [p["temperature"] for p in periods
                     if target_date in p["startTime"]]

        if not day_temps:
            return None, None, None

        nws_high = max(day_temps)

        # Estimate uncertainty from forecast shape
        # Tight peaked forecast = lower uncertainty
        # Flat plateau = higher uncertainty
        temp_range = max(day_temps) - min(day_temps)
        
        # Base sigma from historical NWS 24h MAE (~3.5°F for NYC)
        # Adjust based on forecast confidence indicators
        base_sigma = 3.5
        
        # If forecast is very peaked (large range), more confidence
        # If forecast is flat, less confidence
        if temp_range > 20:
            sigma = base_sigma * 0.8  # confident forecast
        elif temp_range < 10:
            sigma = base_sigma * 1.3  # uncertain forecast
        else:
            sigma = base_sigma

        return nws_high, sigma, day_temps

    except Exception as e:
        print(f"NWS error: {e}")
        return None, None, None

def bucket_probability(floor, cap, mu, sigma):
    """
    Probability that daily high falls in [floor, cap]
    using normal distribution
    """
    dist = stats.norm(mu, sigma)
    return dist.cdf(cap) - dist.cdf(floor)

def above_probability(threshold, mu, sigma):
    """Probability that daily high is above threshold"""
    return 1 - stats.norm(mu, sigma).cdf(threshold)

def below_probability(threshold, mu, sigma):
    """Probability that daily high is below threshold"""
    return stats.norm(mu, sigma).cdf(threshold)

# ── 2. Get Kalshi markets ────────────────────────────────────────────────

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
        return round((float(bid) + float(ask)) / 2, 4)
    return None

# ── 3. Fair value comparison ─────────────────────────────────────────────

def breakeven_yes(price, fee=0.07):
    return price / ((1 - price) * (1 - fee) + price)

def breakeven_no(yes_price, fee=0.07):
    return (1 - yes_price) / (yes_price * (1 - fee) + (1 - yes_price))

# ── 4. Main ───────────────────────────────────────────────────────────────

print("=" * 65)
print("FAIR VALUE MODEL — KXHIGHNY")
print(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
print("=" * 65)

buckets, target_date = get_open_buckets()
if not buckets:
    print("No open markets")
    exit()

print(f"\nTarget date: {target_date}")

nws_high, sigma, day_temps = get_nws_distribution(target_date)
if not nws_high:
    print("No NWS data")
    exit()

print(f"NWS predicted high: {nws_high}°F")
print(f"Model sigma:        {sigma:.2f}°F")
print(f"Model distribution: Normal({nws_high}, {sigma:.1f})")

# Separate range and threshold buckets
range_buckets = [b for b in buckets
                 if b.get("floor_strike") and b.get("cap_strike")]
threshold_buckets = [b for b in buckets
                     if not b.get("floor_strike") or not b.get("cap_strike")]

print(f"\n{'Bucket':<12} {'Mkt%':>7} {'FV%':>7} {'Edge':>7} {'BE_YES%':>9} {'Signal':>12}")
print("-" * 62)

edge_buffer = 0.03
trades = []

for m in sorted(range_buckets, key=lambda x: float(x["floor_strike"])):
    floor = float(m["floor_strike"])
    cap = float(m["cap_strike"])
    mid = get_mid(m)
    if mid is None:
        continue

    fv = bucket_probability(floor, cap, nws_high, sigma)
    edge_yes = fv - mid
    be_yes = breakeven_yes(mid)
    be_no = breakeven_no(mid)
    bucket_label = f"{floor:.0f}-{cap:.0f}°"

    # Determine signal
    signal = ""
    if fv > be_yes + edge_buffer:
        signal = "⚡ BUY YES"
        trades.append(("YES", bucket_label, mid, fv, fv - be_yes))
    elif (1 - fv) > be_no + edge_buffer:
        signal = "⚡ BUY NO"
        trades.append(("NO", bucket_label, mid, fv, (1-fv) - be_no))
    else:
        signal = "— pass"

    print(f"{bucket_label:<12} {mid*100:>6.1f}% {fv*100:>6.1f}% "
          f"{edge_yes*100:>+6.1f}% {be_yes*100:>8.1f}%  {signal}")

# Threshold buckets
for m in threshold_buckets:
    floor = m.get("floor_strike")
    cap = m.get("cap_strike")
    mid = get_mid(m)
    if mid is None:
        continue

    if floor and not cap:
        fv = above_probability(float(floor), nws_high, sigma)
        label = f">{floor}°"
    elif cap and not floor:
        fv = below_probability(float(cap), nws_high, sigma)
        label = f"<{cap}°"
    else:
        continue

    edge_yes = fv - mid
    be_yes = breakeven_yes(mid)
    signal = "⚡ BUY YES" if fv > be_yes + edge_buffer else \
             "⚡ BUY NO" if (1-fv) > breakeven_no(mid) + edge_buffer else "— pass"

    print(f"{label:<12} {mid*100:>6.1f}% {fv*100:>6.1f}% "
          f"{edge_yes*100:>+6.1f}% {be_yes*100:>8.1f}%  {signal}")

print(f"\n{'='*65}")
print("TRADE RECOMMENDATIONS")
print(f"{'='*65}")
if trades:
    for direction, bucket, price, fv, edge in trades:
        contracts = min(10, int(50 * edge / price)) if direction == "YES" else \
                    min(10, int(50 * edge / (1-price)))
        contracts = max(1, contracts)
        cost = contracts * price if direction == "YES" else contracts * (1-price)
        print(f"\n  {direction} on {bucket}")
        print(f"  Market price:  {price*100:.1f}¢")
        print(f"  Fair value:    {fv*100:.1f}%")
        print(f"  Edge after fee:{edge*100:.1f}%")
        print(f"  Suggested:     {contracts} contracts @ ${cost:.2f} total cost")
else:
    print("  No trades recommended today")

print(f"\nModel assumptions:")
print(f"  NWS 24h MAE for NYC: ~3.5°F (historical)")
print(f"  Distribution: Normal")
print(f"  Edge buffer: {edge_buffer*100:.0f}% above breakeven")
print(f"  Fee rate: 7%")