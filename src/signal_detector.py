import requests
import os
import csv
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path

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
        if not m.get("floor_strike") or not m.get("cap_strike"):
            continue
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

def get_nws_hourly(target_date):
    try:
        r = requests.get(
            "https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly",
            headers={"User-Agent": "kalshi-research"}, timeout=10)
        periods = r.json()["properties"]["periods"]
        hours = [(p["startTime"][11:16], p["temperature"])
                 for p in periods if target_date in p["startTime"]]
        return hours
    except:
        return []

def get_current_mid(m):
    bid = m.get("yes_bid_dollars")
    ask = m.get("yes_ask_dollars")
    if bid and ask:
        return round((float(bid) + float(ask)) / 2, 4)
    return None

# ── Main signal detection ─────────────────────────────────────────────────

print("=" * 60)
print(f"KALSHI SIGNAL DETECTOR")
print(f"Run time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
print("=" * 60)

# Get live market data
buckets, target_date = get_open_buckets()
if not buckets:
    print("No open markets found")
    exit()

print(f"\nTarget date: {target_date}")

# Get NWS hourly forecast
hours = get_nws_hourly(target_date)
nws_high = max(t for _, t in hours) if hours else None
nws_peak_time = max(hours, key=lambda x: x[1])[0] if hours else None

print(f"NWS forecast high: {nws_high}°F (peaks at {nws_peak_time} ET)")

# Get current prices
print(f"\nCurrent Kalshi prices:")
print(f"{'Bucket':<12} {'Bid':>8} {'Ask':>8} {'Mid':>8} {'Implied%':>10}")
print("-" * 50)

bucket_data = []
for m in sorted(buckets, key=lambda x: float(x["floor_strike"])):
    floor = float(m["floor_strike"])
    cap = float(m["cap_strike"])
    bid = float(m.get("yes_bid_dollars", 0))
    ask = float(m.get("yes_ask_dollars", 0))
    mid = get_current_mid(m)
    if mid is None:
        continue
    nws_tag = "← NWS" if nws_high and floor <= nws_high <= cap else ""
    print(f"{floor:.0f}-{cap:.0f}°{'':<6} {bid:>7.3f}  {ask:>7.3f}  {mid:>7.3f}  {mid*100:>8.1f}%  {nws_tag}")
    bucket_data.append({"floor": floor, "cap": cap, "mid": mid, "bid": bid, "ask": ask})

# ── Signal analysis ───────────────────────────────────────────────────────

print(f"\n{'='*60}")
print("SIGNAL ANALYSIS")
print(f"{'='*60}")

if not nws_high:
    print("No NWS forecast available")
else:
    # Find market implied high (weighted average of bucket midpoints)
    total_weight = sum(b["mid"] for b in bucket_data)
    if total_weight > 0:
        market_implied = sum(
            (b["floor"] + b["cap"]) / 2 * b["mid"]
            for b in bucket_data
        ) / total_weight
    else:
        market_implied = None

    print(f"NWS forecast high:      {nws_high}°F")
    print(f"Market implied high:    {market_implied:.1f}°F" if market_implied else "N/A")

    if market_implied:
        divergence = nws_high - market_implied
        print(f"Divergence (NWS-Mkt):  {divergence:+.1f}°F")

        print(f"\n--- Trade Signals ---")

        # Signal 1: NWS bucket is underpriced
        nws_bucket = next((b for b in bucket_data
                          if b["floor"] <= nws_high <= b["cap"]), None)
        if nws_bucket:
            mid = nws_bucket["mid"]
            bucket_label = f"{nws_bucket['floor']:.0f}-{nws_bucket['cap']:.0f}°"
            print(f"\n[NWS TARGET BUCKET]  {bucket_label}  priced at {mid*100:.1f}¢")
            if mid < 0.35:
                print(f"  ⚡ BUY YES — priced at {mid*100:.1f}¢, NWS says this is the bucket")
            elif mid > 0.55:
                print(f"  ✋ PASS — already expensive at {mid*100:.1f}¢")
            else:
                print(f"  ~ NEUTRAL — fair price at {mid*100:.1f}¢")

        # Signal 2: Buckets far from NWS that are overpriced
        print(f"\n[OVERPRICED TAILS]")
        found = False
        for b in bucket_data:
            bucket_label = f"{b['floor']:.0f}-{b['cap']:.0f}°"
            distance = min(abs(b["floor"] - nws_high), abs(b["cap"] - nws_high))
            # More than 4° from NWS high and priced above 8¢
            if distance > 4 and b["mid"] > 0.08:
                no_price = 1 - b["mid"]
                print(f"  ⚡ BUY NO on {bucket_label} — {b['mid']*100:.1f}¢ YES, "
                      f"{no_price*100:.1f}¢ NO, {distance:.0f}° from NWS forecast")
                found = True
        if not found:
            print("  No overpriced tails detected")

        # Signal 3: Overall divergence signal
        print(f"\n[DIVERGENCE SIGNAL]")
        if abs(divergence) >= 4:
            direction = "WARMER" if divergence > 0 else "COOLER"
            print(f"  ⚡ STRONG — NWS says {divergence:+.1f}° {direction} than market expects")
            print(f"  Consider shifting YES bets toward NWS target bucket")
        elif abs(divergence) >= 2:
            print(f"  ~ MODERATE divergence of {divergence:+.1f}°F — watch but don't act")
        else:
            print(f"  ✓ ALIGNED — NWS and market agree within 2°F")

print(f"\n{'='*60}")
print("Historical context (from our backtest):")
print("  Favorites win 68.8% of time (priced ~45¢)")
print("  10-20¢ buckets win only 4.5% (91.5% NO win rate)")
print("  Need 200+ obs for statistical significance")
print(f"{'='*60}")