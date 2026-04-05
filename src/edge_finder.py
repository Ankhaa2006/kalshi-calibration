import requests
import os
import re
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY = os.getenv("KALSHI_API_KEY")
NOAA_TOKEN = os.getenv("NOAA_TOKEN")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

def get_event_buckets(event_ticker):
    r = requests.get(f"{BASE_URL}/markets", headers=headers,
        params={"event_ticker": event_ticker, "status": "settled", "limit": 50})
    return r.json().get("markets", [])

def get_6am_price(series_ticker, market_ticker, date_str, retries=3):
    start = int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end   = start + 86400
    target = start + (11 * 3600)

    for attempt in range(retries):
        try:
            r = requests.get(
                f"{BASE_URL}/series/{series_ticker}/markets/{market_ticker}/candlesticks",
                headers=headers,
                params={"start_ts": start, "end_ts": end, "period_interval": 60},
                timeout=10
            )
            candles = r.json().get("candlesticks", [])
            if not candles:
                return None
            best = min(candles, key=lambda c: abs(c["end_period_ts"] - target))
            bid = float(best["yes_bid"]["close_dollars"])
            ask = float(best["yes_ask"]["close_dollars"])
            return round((bid + ask) / 2, 4)
        except Exception as e:
            print(f"  Retry {attempt+1} for {market_ticker}: {e}")
            time.sleep(2 ** attempt)  # exponential backoff: 1s, 2s, 4s
    return None

def load_noaa_cache():
    cache = {}
    for start, end in [("2024-01-01","2024-12-31"), ("2025-01-01","2025-12-31"), ("2026-01-01","2026-03-31")]:
        r = requests.get(
            "https://www.ncdc.noaa.gov/cdo-web/api/v2/data",
            headers={"token": NOAA_TOKEN},
            params={
                "datasetid": "GHCND",
                "stationid": "GHCND:USW00094728",
                "datatypeid": "TMAX",
                "startdate": start,
                "enddate": end,
                "units": "standard",
                "limit": 1000
            }
        )
        if not r.text.strip():
            print(f"  Warning: empty for {start} to {end}")
            continue
        for obs in r.json().get("results", []):
            cache[obs["date"][:10]] = float(obs["value"])
        print(f"  Loaded {start} to {end}")
    return cache

def parse_date(event_ticker):
    months = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
              "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
    m = re.search(r'-(\d{2})([A-Z]{3})(\d{2})$', event_ticker)
    if m:
        return f"20{m.group(1)}-{months.get(m.group(2),'00')}-{m.group(3)}"
    return None

def get_resolved_winners(series_ticker, max_pages=20):
    markets, cursor = [], None
    for _ in range(max_pages):
        params = {"series_ticker": series_ticker, "status": "settled", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        data = requests.get(f"{BASE_URL}/markets", headers=headers, params=params).json()
        batch = data.get("markets", [])
        markets.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break
    events = {}
    for m in markets:
        event = m.get("event_ticker", "")
        if event not in events:
            events[event] = []
        events[event].append(m)
    winners = {}
    for event_ticker, mkts in events.items():
        for m in mkts:
            if m.get("result") == "yes":
                winners[event_ticker] = m
                break
    return winners

# ── Main ──────────────────────────────────────────────────────────────────

print("Preloading NOAA data...")
noaa_cache = load_noaa_cache()
print(f"  Total: {len(noaa_cache)} days cached\n")

print("Fetching resolved winners...")
winners = get_resolved_winners("KXHIGHNY")
print(f"  Got {len(winners)} events\n")

all_events = sorted(winners.items())

print(f"{'Date':<12} {'Actual':>7} {'Rank':>6} {'Win$':>6} {'Fav$':>6} {'Fav won?':>9}")
print("-" * 55)

favorite_won_count = 0
total_range = 0
rank_distribution = {}

for event_ticker, winning_market in all_events:
    date_str = parse_date(event_ticker)
    if not date_str:
        continue

    actual = noaa_cache.get(date_str)
    buckets = get_event_buckets(event_ticker)
    range_buckets = [b for b in buckets if b.get("floor_strike") and b.get("cap_strike")]

    if not range_buckets:
        continue

    bucket_prices = {}
    for b in range_buckets:
        ticker = b["ticker"]
        price = get_6am_price("KXHIGHNY", ticker, date_str)
        bucket_prices[ticker] = {
            "floor": float(b["floor_strike"]),
            "cap": float(b["cap_strike"]),
            "price_6am": price,
            "result": b.get("result")
        }
        time.sleep(0.1)

    # Only keep buckets with prices
    priced = {t: i for t, i in bucket_prices.items() if i["price_6am"] is not None}
    if not priced:
        continue

    sorted_buckets = sorted(priced.items(), key=lambda x: x[1]["price_6am"], reverse=True)
    winner_ticker = winning_market.get("ticker")

    winner_rank = None
    winner_price = None
    favorite_ticker = sorted_buckets[0][0]
    favorite_price = sorted_buckets[0][1]["price_6am"]

    for rank, (ticker, info) in enumerate(sorted_buckets):
        if ticker == winner_ticker:
            winner_rank = rank + 1
            winner_price = info["price_6am"]
            break

    if winner_rank is None:
        continue

    total_range += 1
    fav_won = favorite_ticker == winner_ticker
    if fav_won:
        favorite_won_count += 1

    rank_distribution[winner_rank] = rank_distribution.get(winner_rank, 0) + 1

    print(f"{date_str:<12} {str(actual)+'°F':>7} {str(winner_rank)+'/'+str(len(sorted_buckets)):>6} "
          f"{winner_price:>6.2f} {favorite_price:>6.2f} {'✅' if fav_won else '❌':>9}")

    time.sleep(0.2)

print(f"\n{'='*55}")
print(f"Total range-bucket events analyzed: {total_range}")
print(f"Favorite (highest priced) won:      {favorite_won_count} ({100*favorite_won_count/total_range:.1f}%)")
print(f"\nWinner rank distribution:")
for rank in sorted(rank_distribution):
    count = rank_distribution[rank]
    bar = "█" * count
    print(f"  Rank {rank}: {count:>3} times  {bar}")

# ── Calibration analysis: price vs actual win rate ───────────────────────
print("\n\n=== CALIBRATION ANALYSIS ===")
print("Collecting all bucket prices and outcomes...\n")

all_buckets = []  # (price_6am, did_win)

for event_ticker, winning_market in all_events:
    date_str = parse_date(event_ticker)
    if not date_str:
        continue
    buckets = get_event_buckets(event_ticker)
    range_buckets = [b for b in buckets if b.get("floor_strike") and b.get("cap_strike")]
    if not range_buckets:
        continue

    winner_ticker = winning_market.get("ticker")
    for b in range_buckets:
        ticker = b["ticker"]
        price = get_6am_price("KXHIGHNY", ticker, date_str)
        if price is None:
            continue
        did_win = 1 if ticker == winner_ticker else 0
        all_buckets.append((price, did_win))
    time.sleep(0.2)

# Group into price bins
bins = {
    "0-10¢":  {"count": 0, "wins": 0},
    "10-20¢": {"count": 0, "wins": 0},
    "20-30¢": {"count": 0, "wins": 0},
    "30-40¢": {"count": 0, "wins": 0},
    "40-50¢": {"count": 0, "wins": 0},
    "50-60¢": {"count": 0, "wins": 0},
    "60-70¢": {"count": 0, "wins": 0},
    "70-100¢":{"count": 0, "wins": 0},
}

for price, did_win in all_buckets:
    p = price * 100
    if p < 10:   b = "0-10¢"
    elif p < 20: b = "10-20¢"
    elif p < 30: b = "20-30¢"
    elif p < 40: b = "30-40¢"
    elif p < 50: b = "40-50¢"
    elif p < 60: b = "50-60¢"
    elif p < 70: b = "60-70¢"
    else:        b = "70-100¢"
    bins[b]["count"] += 1
    bins[b]["wins"] += did_win

print(f"{'Price bin':<10} {'Count':>6} {'Wins':>6} {'Actual%':>9} {'Implied%':>9} {'Edge':>8}")
print("-" * 55)
for label, data in bins.items():
    if data["count"] == 0:
        continue
    actual_pct = 100 * data["wins"] / data["count"]
    implied = float(label.split("-")[0].replace("¢","") if label != "70-100¢" else "85")
    implied_pct = implied + 5  # midpoint of bin
    edge = actual_pct - implied_pct
    print(f"{label:<10} {data['count']:>6} {data['wins']:>6} {actual_pct:>8.1f}% {implied_pct:>8.1f}% {edge:>+7.1f}%")