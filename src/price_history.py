import requests
import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY = os.getenv("KALSHI_API_KEY")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

def get_candlesticks(series_ticker, market_ticker, start_dt, end_dt, interval_minutes=60):
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    r = requests.get(
        f"{BASE_URL}/series/{series_ticker}/markets/{market_ticker}/candlesticks",
        headers=headers,
        params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": interval_minutes}
    )
    return r.json().get("candlesticks", [])

def get_opening_price(series_ticker, market_ticker, date_str):
    """
    Get the 6am ET price for a market on a given date
    6am ET = 11am UTC
    Returns mid price (avg of bid and ask)
    """
    # Full day in UTC
    start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end   = start + timedelta(hours=23, minutes=59)

    candles = get_candlesticks(series_ticker, market_ticker, start, end)

    if not candles:
        return None, None

    # Find the candle closest to 11:00 UTC (6am ET)
    target_ts = int((start + timedelta(hours=11)).timestamp())
    best = min(candles, key=lambda c: abs(c["end_period_ts"] - target_ts))

    ts = datetime.fromtimestamp(best["end_period_ts"], tz=timezone.utc)
    bid = float(best["yes_bid"]["close_dollars"])
    ask = float(best["yes_ask"]["close_dollars"])
    mid = (bid + ask) / 2

    return mid, ts

# ── Test: March 16 — all buckets, what were they priced at 6am ET? ───────

print("March 16 2026 — Kalshi bucket prices at ~6am ET")
print(f"Actual high: 57°F → winning bucket: 57-58°\n")

# All buckets for that day (from our earlier analysis)
buckets = [
    ("KXHIGHNY-26MAR16-B47.5", "47-48°"),
    ("KXHIGHNY-26MAR16-B49.5", "49-50°"),
    ("KXHIGHNY-26MAR16-B51.5", "51-52°"),
    ("KXHIGHNY-26MAR16-B53.5", "53-54°"),
    ("KXHIGHNY-26MAR16-B55.5", "55-56°"),
    ("KXHIGHNY-26MAR16-B57.5", "57-58°"),  # ← winner
    ("KXHIGHNY-26MAR16-B59.5", "59-60°"),
    ("KXHIGHNY-26MAR16-B61.5", "61-62°"),
]

total_mid = 0
print(f"{'Bucket':<10} {'Mid price':>10} {'Winner?':>8}")
print("-" * 35)
for ticker, label in buckets:
    mid, ts = get_opening_price("KXHIGHNY", ticker, "2026-03-16")
    if mid is not None:
        winner = "← WIN" if "B57.5" in ticker else ""
        print(f"{label:<10} {mid:>9.2f}¢  {winner}")
        total_mid += mid

print(f"\nSum of mids: {total_mid:.2f}¢")
print(f"\n(6am ET prices — this is what traders saw before the day started)")