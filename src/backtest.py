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

# ── Reuse functions from edge_finder ─────────────────────────────────────

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
            time.sleep(2 ** attempt)
    return None

def load_noaa_cache():
    cache = {}
    for start, end in [("2025-12-01","2025-12-31"), ("2026-01-01","2026-03-31")]:
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
            continue
        for obs in r.json().get("results", []):
            cache[obs["date"][:10]] = float(obs["value"])
    return cache

def get_resolved_winners(series_ticker, max_pages=50):
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

def parse_date(event_ticker):
    months = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
              "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
    m = re.search(r'-(\d{2})([A-Z]{3})(\d{2})$', event_ticker)
    if m:
        return f"20{m.group(1)}-{months.get(m.group(2),'00')}-{m.group(3)}"
    return None

# ── Backtest strategies ───────────────────────────────────────────────────

def backtest(events, noaa_cache, bet_size=10):
    """
    Test two strategies across all events:

    Strategy 1 — Buy YES on favorite
    Always bet on the highest priced bucket at 6am

    Strategy 2 — Buy NO on overpriced tails
    Bet NO on any bucket priced 10-20¢ (our calibration showed these win only 4.5%)
    """

    s1_pnl = []  # (date, bet, outcome, profit)
    s2_pnl = []

    print("Running backtest...")
    print(f"Bet size: ${bet_size} per trade\n")

    for event_ticker, winning_market in sorted(events.items()):
        date_str = parse_date(event_ticker)
        if not date_str or date_str not in noaa_cache:
            continue

        buckets = get_event_buckets(event_ticker)
        range_buckets = [b for b in buckets if b.get("floor_strike") and b.get("cap_strike")]
        if not range_buckets:
            continue

        # Get 6am prices for all buckets
        prices = {}
        for b in range_buckets:
            ticker = b["ticker"]
            price = get_6am_price("KXHIGHNY", ticker, date_str)
            if price:
                prices[ticker] = {
                    "price": price,
                    "floor": float(b["floor_strike"]),
                    "cap": float(b["cap_strike"]),
                    "won": b.get("result") == "yes"
                }
            time.sleep(0.15)

        if not prices:
            continue

        winner_ticker = winning_market.get("ticker")

        # ── Strategy 1: Buy YES on favorite ──────────────────────────────
        favorite = max(prices.items(), key=lambda x: x[1]["price"])
        fav_ticker, fav_info = favorite
        fav_price = fav_info["price"]
        fav_won = fav_ticker == winner_ticker

        # Cost = bet_size * price, payout = bet_size if wins
        cost_s1 = bet_size * fav_price
        profit_s1 = bet_size - cost_s1 if fav_won else -cost_s1
        s1_pnl.append({
            "date": date_str,
            "bucket": f"{fav_info['floor']:.0f}-{fav_info['cap']:.0f}°",
            "price": fav_price,
            "won": fav_won,
            "profit": round(profit_s1, 2)
        })

        # ── Strategy 2: Buy NO on 10-20¢ buckets ─────────────────────────
        for ticker, info in prices.items():
            if 0.10 <= info["price"] <= 0.20:
                # Buying NO means we pay (1 - price), win 1 if it loses
                no_price = 1 - info["price"]
                cost_s2 = bet_size * (1 - no_price)  # = bet_size * price
                bucket_lost = ticker != winner_ticker
                profit_s2 = bet_size - cost_s2 if bucket_lost else -cost_s2
                s2_pnl.append({
                    "date": date_str,
                    "bucket": f"{info['floor']:.0f}-{info['cap']:.0f}°",
                    "price": info["price"],
                    "won": bucket_lost,
                    "profit": round(profit_s2, 2)
                })

        time.sleep(0.3)

    return s1_pnl, s2_pnl

def print_results(name, pnl, bet_size):
    if not pnl:
        print(f"{name}: no trades")
        return

    total_profit = sum(t["profit"] for t in pnl)
    wins = sum(1 for t in pnl if t["won"])
    total = len(pnl)
    win_rate = 100 * wins / total
    total_invested = sum(bet_size * t["price"] for t in pnl)
    roi = 100 * total_profit / total_invested if total_invested else 0

    print(f"\n{'='*55}")
    print(f"Strategy: {name}")
    print(f"{'='*55}")
    print(f"Total trades:     {total}")
    print(f"Win rate:         {win_rate:.1f}%")
    print(f"Total invested:   ${total_invested:.2f}")
    print(f"Total profit:     ${total_profit:.2f}")
    print(f"ROI:              {roi:.1f}%")
    print(f"Avg profit/trade: ${total_profit/total:.2f}")

    print(f"\nWorst 5 losses:")
    for t in sorted(pnl, key=lambda x: x["profit"])[:5]:
        print(f"  {t['date']}  {t['bucket']:<10}  price={t['price']:.2f}  profit=${t['profit']:.2f}")

    print(f"\nBest 5 wins:")
    for t in sorted(pnl, key=lambda x: x["profit"], reverse=True)[:5]:
        print(f"  {t['date']}  {t['bucket']:<10}  price={t['price']:.2f}  profit=${t['profit']:.2f}")

# ── Main ──────────────────────────────────────────────────────────────────

print("Loading data...")
noaa_cache = load_noaa_cache()
winners = get_resolved_winners("KXHIGHNY")
print(f"Events: {len(winners)}, NOAA days: {len(noaa_cache)}\n")

s1_pnl, s2_pnl = backtest(winners, noaa_cache, bet_size=10)

print_results("Buy YES on Favorite", s1_pnl, bet_size=10)
print_results("Buy NO on 10-20¢ buckets", s2_pnl, bet_size=10)