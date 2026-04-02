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

def get_settled_markets(series_ticker, max_pages=10):
    """Pull settled markets for a series with pagination"""
    markets = []
    cursor = None
    page = 0

    while page < max_pages:
        params = {
            "series_ticker": series_ticker,
            "status": "settled",
            "limit": 100
        }
        if cursor:
            params["cursor"] = cursor

        r = requests.get(f"{BASE_URL}/markets", headers=headers, params=params)
        data = r.json()
        batch = data.get("markets", [])
        markets.extend(batch)

        cursor = data.get("cursor")
        page += 1
        print(f"Page {page}: fetched {len(batch)} markets (total: {len(markets)})")

        if not cursor or len(batch) == 0:
            break

    return markets

def parse_resolution(markets):
    """
    For each event date, find which bucket resolved YES
    and what the market priced it at open
    """
    events = {}
    for m in markets:
        event = m.get("event_ticker", "")
        if event not in events:
            events[event] = []
        events[event].append(m)

    results = []
    for event_ticker, mkts in sorted(events.items()):
        winner = None
        for m in mkts:
            if m.get("result") == "yes":
                winner = m
                break

        if winner:
            results.append({
                "event": event_ticker,
                "winning_ticker": winner.get("ticker"),
                "title": winner.get("title", "")[:60],
                "last_price": winner.get("last_price_dollars"),
                "volume": winner.get("volume_fp")
            })

    return results

# Run it
print("Fetching settled KXHIGHNY markets...\n")
markets = get_settled_markets("KXHIGHNY", max_pages=20)
print(f"\nTotal settled markets: {len(markets)}")

results = parse_resolution(markets)
print(f"Events with resolved winner: {len(results)}\n")

print(f"{'Event':<25} {'Winning Bucket':<45} {'Last Price':>10} {'Volume':>10}")
print("-" * 95)
for r in results[-20:]:
    print(f"{r['event']:<25} {r['winning_ticker']:<45} {str(r['last_price']):>10} {str(r['volume']):>10}")