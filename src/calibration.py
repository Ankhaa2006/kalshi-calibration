import requests
import os
import re
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY = os.getenv("KALSHI_API_KEY")
NOAA_TOKEN = os.getenv("NOAA_TOKEN")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

# ── 1. NOAA Central Park official highs ──────────────────────────────────

def get_noaa_highs():
    all_highs = {}
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
        if r.status_code != 200 or not r.text.strip():
            print(f"  Warning: NOAA returned empty for {start} to {end} (status {r.status_code})")
            continue
        for obs in r.json().get("results", []):
            date = obs["date"][:10]
            all_highs[date] = float(obs["value"])
        print(f"  Fetched {start} to {end}: ok")
    return all_highs

# ── 2. Kalshi settled markets ─────────────────────────────────────────────

def get_settled_markets(series_ticker, max_pages=20):
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
    return markets

def get_resolved_winners(series_ticker):
    markets = get_settled_markets(series_ticker)
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

def get_all_markets_for_event(event_ticker):
    r = requests.get(f"{BASE_URL}/markets", headers=headers,
        params={"event_ticker": event_ticker, "status": "settled", "limit": 50})
    return r.json().get("markets", [])

# ── 3. Parse date ─────────────────────────────────────────────────────────

def parse_date(event_ticker):
    months = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
              "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
    m = re.search(r'-(\d{2})([A-Z]{3})(\d{2})$', event_ticker)
    if m:
        return f"20{m.group(1)}-{months.get(m.group(2),'00')}-{m.group(3)}"
    return None

# ── 4. Main analysis ──────────────────────────────────────────────────────

print("Fetching NOAA Central Park highs...")
actuals = get_noaa_highs()
print(f"  Got {len(actuals)} days\n")

print("Fetching Kalshi resolved winners...")
winners = get_resolved_winners("KXHIGHNY")
print(f"  Got {len(winners)} resolved events\n")

print("Analyzing...\n")

results = []
for event_ticker, winning_market in sorted(winners.items()):
    date_str = parse_date(event_ticker)
    if not date_str or date_str not in actuals:
        continue

    actual = actuals[date_str]
    ticker = winning_market.get("ticker", "")
    floor = winning_market.get("floor_strike")
    cap = winning_market.get("cap_strike")

    if floor is None or cap is None:
        continue  # skip threshold buckets for now

    floor, cap = float(floor), float(cap)
    in_range = floor <= actual <= cap
    error = 0 if in_range else (actual - floor if actual < floor else actual - cap)

    results.append({
        "date": date_str,
        "actual": actual,
        "floor": floor,
        "cap": cap,
        "in_range": in_range,
        "error": error,
        "ticker": ticker
    })

# ── 5. Print results ──────────────────────────────────────────────────────

print(f"{'Date':<12} {'Actual':>8} {'Bucket':>10} {'Match':>8} {'Error':>8}")
print("-" * 52)

correct = 0
errors = []
for r in results:
    status = "✅" if r["in_range"] else "❌"
    bucket = f"{r['floor']:.0f}-{r['cap']:.0f}°"
    print(f"{r['date']:<12} {r['actual']:>6.0f}°F  {bucket:>8}  {status:>6} {r['error']:>+7.1f}")
    if r["in_range"]:
        correct += 1
    elif r["error"] != 0:
        errors.append(r["error"])

total = len(results)
print(f"\n{'='*52}")
print(f"Total range-bucket events: {total}")
print(f"Correct (actual in bucket): {correct} ({100*correct/total:.1f}%)")
print(f"Mismatches:                 {total - correct}")
if errors:
    import statistics
    print(f"Avg error on miss:          {statistics.mean(errors):+.2f}°F")
    print(f"Max error:                  {max(errors, key=abs):+.2f}°F")