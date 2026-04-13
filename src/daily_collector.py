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

def get_todays_buckets():
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

    # Pick soonest closing event
    next_event_ticker = min(events.keys(),
        key=lambda e: events[e][0].get("close_time", "9999"))
    buckets = events[next_event_ticker]
    target_date = buckets[0].get("close_time", "")[:10]
    return buckets, target_date

def get_current_price(market):
    bid = market.get("yes_bid_dollars")
    ask = market.get("yes_ask_dollars")
    if bid and ask:
        return round((float(bid) + float(ask)) / 2, 4)
    return None

def get_nws_forecast_high(target_date_str):
    try:
        r = requests.get(
            "https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly",
            headers={"User-Agent": "kalshi-research"},
            timeout=10
        )
        periods = r.json()["properties"]["periods"]
        temps = [p["temperature"] for p in periods
                 if target_date_str in p["startTime"]]
        return max(temps) if temps else None
    except Exception as e:
        print(f"  NWS error: {e}")
        return None

def append_to_csv(rows):
    existing = set()
    file_exists = DATA_FILE.exists()

    if file_exists:
        with open(DATA_FILE, "r") as f:
            for row in csv.DictReader(f):
                existing.add((row["target_date"], row["ticker"]))

    new_rows = [r for r in rows
                if (r["target_date"], r["ticker"]) not in existing]

    if not new_rows:
        print("  No new rows to write (already collected today)")
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

def backfill_results():
    if not DATA_FILE.exists():
        return

    with open(DATA_FILE, "r") as f:
        rows = list(csv.DictReader(f))

    updated = 0
    for row in rows:
        # Skip if already complete
        if row.get("result") and row.get("actual_high"):
            continue

        ticker = row["ticker"]
        target_date = row["target_date"]

        # Get market status from Kalshi
        try:
            r = requests.get(f"{BASE_URL}/markets/{ticker}",
                           headers=headers, timeout=10)
            m = r.json().get("market", {})
            if m.get("status") in ("settled", "finalized") and not row.get("result"):
                row["result"] = m.get("result", "")
                updated += 1
        except:
            pass

        # Get actual high from NOAA
        if not row.get("actual_high") and target_date:
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
                    }, timeout=10
                )
                if nr.text.strip():
                    results = nr.json().get("results", [])
                    if results:
                        row["actual_high"] = results[0]["value"]
                        updated += 1
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
        print(f"  Backfilled {updated} fields")
    else:
        print("  Nothing to backfill yet")

def run_collection():
    now = datetime.now(timezone.utc)
    print(f"\n=== Daily Collection Run ===")
    print(f"Time: {now.strftime('%Y-%m-%d %H:%M UTC')}\n")

    print("Backfilling past results...")
    backfill_results()

    print("\nFetching open buckets...")
    buckets, target_date = get_todays_buckets()
    print(f"  Found {len(buckets)} range buckets")

    if not buckets:
        print("  No buckets found")
        return

    print(f"  Target date: {target_date}")

    print("\nFetching NWS forecast...")
    nws_high = get_nws_forecast_high(target_date)
    print(f"  NWS predicted high: {nws_high}°F")

    print("\nCollecting bucket prices...")
    rows = []
    for m in buckets:
        price = get_current_price(m)
        floor = float(m["floor_strike"])
        cap = float(m["cap_strike"])
        ticker = m["ticker"]
        nws_tag = "← NWS target" if nws_high and floor <= nws_high <= cap else ""
        print(f"  {floor:.0f}-{cap:.0f}°  price={price}  {nws_tag}")
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

    print("\nSaving...")
    append_to_csv(rows)
    print("\nDone!")

run_collection()