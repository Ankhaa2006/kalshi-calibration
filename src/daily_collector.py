import requests
import os
import re
import csv
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY = os.getenv("KALSHI_API_KEY")
NOAA_TOKEN = os.getenv("NOAA_TOKEN")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

DATA_FILE = Path('/Users/ankhbayarbatkhurel/kalshi-calibration/data/daily_observations.csv')

# ── 1. Get today's open KXHIGHNY markets ─────────────────────────────────

def get_todays_buckets():
    r = requests.get(f"{BASE_URL}/markets", headers=headers,
        params={"series_ticker": "KXHIGHNY", "status": "open", "limit": 50})
    markets = r.json().get("markets", [])

    # Find tomorrow's event (markets closing tomorrow)
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Filter to range buckets only
    buckets = []
    for m in markets:
        floor = m.get("floor_strike")
        cap = m.get("cap_strike")
        if floor and cap:
            close_time = m.get("close_time", "")
            if today in close_time or tomorrow in close_time:
                buckets.append(m)
    return buckets

# ── 2. Get current mid price ──────────────────────────────────────────────

def get_current_price(market):
    bid = market.get("yes_bid_dollars")
    ask = market.get("yes_ask_dollars")
    if bid and ask:
        return round((float(bid) + float(ask)) / 2, 4)
    return None

# ── 3. Get NWS forecast high for NYC today ───────────────────────────────

def get_nws_forecast_high(target_date_str):
    try:
        r = requests.get(
            "https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly",
            headers={"User-Agent": "kalshi-research"},
            timeout=10
        )
        periods = r.json()["properties"]["periods"]
        temps = [
            p["temperature"] for p in periods
            if target_date_str in p["startTime"]
        ]
        return max(temps) if temps else None
    except Exception as e:
        print(f"  NWS error: {e}")
        return None

# ── 4. Write to CSV ───────────────────────────────────────────────────────

def append_to_csv(rows):
    existing = set()
    file_exists = DATA_FILE.exists()

    if file_exists:
        with open(DATA_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Deduplicate by target_date + ticker + hour of collection
                key = (row["target_date"], row["ticker"])
                existing.add(key)

    new_rows = []
    for row in rows:
        key = (row["target_date"], row["ticker"])
        if key not in existing:
            new_rows.append(row)

    if not new_rows:
        print("  No new rows to write (already collected this hour)")
        return

    with open(DATA_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "collected_at", "target_date", "ticker",
            "floor", "cap", "price_mid",
            "nws_forecast_high", "actual_high", "result"
        ])
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)
    print(f"  Wrote {len(new_rows)} rows to {DATA_FILE}")

# ── 5. Backfill actual results for past observations ─────────────────────

def backfill_results():
    if not DATA_FILE.exists():
        return

    # Read existing data
    with open(DATA_FILE, "r") as f:
        rows = list(csv.DictReader(f))

    updated = 0
    for row in rows:
        if row["actual_high"] or row["result"]:
            continue  # already filled

        target_date = row["target_date"]
        ticker = row["ticker"]

        # Check if market has settled
        r = requests.get(f"{BASE_URL}/markets/{ticker}", headers=headers, timeout=10)
        m = r.json().get("market", {})

        if m.get("status") == "settled":
            row["result"] = m.get("result", "")
            row["actual_high"] = ""  # will fill from NOAA below
            updated += 1

        # Try NOAA for actual high
        if not row["actual_high"] and target_date:
            try:
                nr = requests.get(
                    "https://www.ncdc.noaa.gov/cdo-web/api/v2/data",
                    headers={"token": NOAA_TOKEN},
                    params={
                        "datasetid": "GHCND",
                        "stationid": "GHCND:USW00094728",
                        "datatypeid": "TMAX",
                        "startdate": target_date,
                        "enddate": target_date,
                        "units": "standard",
                        "limit": 10
                    },
                    timeout=10
                )
                if nr.text.strip():
                    results = nr.json().get("results", [])
                    if results:
                        row["actual_high"] = results[0]["value"]
            except:
                pass

    if updated:
        with open(DATA_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "collected_at", "target_date", "ticker",
                "floor", "cap", "price_mid",
                "nws_forecast_high", "actual_high", "result"
            ])
            writer.writeheader()
            writer.writerows(rows)
        print(f"  Backfilled {updated} rows")

# ── 6. Main ───────────────────────────────────────────────────────────────

def run_collection():
    now = datetime.now(timezone.utc)
    print(f"\n=== Daily Collection Run ===")
    print(f"Time: {now.strftime('%Y-%m-%d %H:%M UTC')}\n")

    # First backfill any past results
    print("Backfilling past results...")
    backfill_results()

    # Get today's open buckets
    print("\nFetching open buckets...")
    buckets = get_todays_buckets()
    print(f"  Found {len(buckets)} range buckets")

    if not buckets:
        print("  No buckets found — market may not be open yet")
        return

    # Determine target date from close_time
    target_date = buckets[0].get("close_time", "")[:10]
    print(f"  Target date: {target_date}")

    # Get NWS forecast
    print("\nFetching NWS forecast...")
    nws_high = get_nws_forecast_high(target_date)
    print(f"  NWS predicted high: {nws_high}°F")

    # Collect prices
    print("\nCollecting bucket prices...")
    rows = []
    for m in buckets:
        price = get_current_price(m)
        floor = float(m["floor_strike"])
        cap = float(m["cap_strike"])
        ticker = m["ticker"]

        rows.append({
            "collected_at": now.strftime("%Y-%m-%d %H:%M UTC"),
            "target_date": target_date,
            "ticker": ticker,
            "floor": floor,
            "cap": cap,
            "price_mid": price,
            "nws_forecast_high": nws_high,
            "actual_high": "",
            "result": ""
        })

        print(f"  {floor:.0f}-{cap:.0f}°  price={price}  {'← NWS target' if nws_high and floor <= nws_high <= cap else ''}")

    # Save
    print("\nSaving...")
    append_to_csv(rows)
    print("\nDone!")

run_collection()