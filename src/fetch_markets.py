import requests
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY = os.getenv("KALSHI_API_KEY")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}


def get_all_markets_for_series(series_ticker):
    markets = []
    cursor = None
    while True:
        params = {"series_ticker": series_ticker, "status": "open", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        r = requests.get(f"{BASE_URL}/markets", headers=headers, params=params)
        data = r.json()
        batch = data.get("markets", [])
        markets.extend(batch)
        cursor = data.get("cursor")
        if not cursor or len(batch) == 0:
            break
    return markets


def analyze_probability_sum(series_ticker):
    markets = get_all_markets_for_series(series_ticker)

    events = {}
    for m in markets:
        event = m.get("event_ticker", "unknown")
        if event not in events:
            events[event] = []
        events[event].append(m)

    print(f"\n{'='*60}")
    print(f"Series: {series_ticker} — {len(markets)} markets across {len(events)} events")
    print(f"{'='*60}")

    for event_ticker, mkts in sorted(events.items()):
        yes_prices = []
        print(f"\nEvent: {event_ticker}")

        for m in sorted(mkts, key=lambda x: x.get("ticker", "")):
            yes_bid = m.get("yes_bid_dollars")
            yes_ask = m.get("yes_ask_dollars")
            volume = m.get("volume_fp", 0)
            title = m.get("title", "")[:55]

            if yes_bid is not None:
                try:
                    mid = (float(yes_bid) + float(yes_ask)) / 2
                    yes_prices.append(mid)
                    print(f"  bid=${yes_bid}  ask=${yes_ask}  mid={mid:.2f}  vol={volume:>8}  | {title}")
                except (ValueError, TypeError):
                    print(f"  [skipped] {title}")

        if yes_prices:
            total = sum(yes_prices)
            gap = 1.0 - total
            print(f"\n  >> Sum of mids: {total:.4f}")
            print(f"  >> Gap from $1: {gap:+.4f}  {'⚠️  MISPRICED' if abs(gap) > 0.05 else '✅ roughly fair'}")


analyze_probability_sum("KXHIGHNY")