import requests
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import db

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY    = os.getenv('KALSHI_API_KEY')
NOAA_TOKEN = os.getenv('NOAA_TOKEN')
BASE_URL   = 'https://api.elections.kalshi.com/trade-api/v2'
headers    = {'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json'}


def get_todays_buckets():
    r = requests.get(f'{BASE_URL}/markets', headers=headers,
                     params={'series_ticker': 'KXHIGHNY', 'status': 'open', 'limit': 50})
    markets = r.json().get('markets', [])
    events: dict = {}
    for m in markets:
        if not m.get('floor_strike') or not m.get('cap_strike'):
            continue
        events.setdefault(m.get('event_ticker', ''), []).append(m)
    if not events:
        return [], None
    next_event = min(events, key=lambda e: events[e][0].get('close_time', '9999'))
    buckets    = events[next_event]
    return buckets, buckets[0].get('close_time', '')[:10]


def get_current_price(market):
    bid, ask = market.get('yes_bid_dollars'), market.get('yes_ask_dollars')
    if bid and ask:
        return round((float(bid) + float(ask)) / 2, 4)
    return None


def get_nws_forecast_high(target_date):
    try:
        r = requests.get(
            'https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly',
            headers={'User-Agent': 'kalshi-research'}, timeout=10)
        periods = r.json()['properties']['periods']
        temps = [p['temperature'] for p in periods if target_date in p['startTime']]
        return max(temps) if temps else None
    except Exception as e:
        print(f"  NWS error: {e}")
        return None


def backfill_results():
    """Check Kalshi + NOAA for any unresolved past observations."""
    obs_all = db.get_all_observations()
    pending = [o for o in obs_all if not o.get('result') or not o.get('actual_high')]

    if not pending:
        print("  Nothing to backfill")
        return

    for obs in pending:
        ticker      = obs['ticker']
        target_date = obs['target_date']

        try:
            r = requests.get(f'{BASE_URL}/markets/{ticker}',
                             headers=headers, timeout=10)
            m = r.json().get('market', {})
            if m.get('status') in ('settled', 'finalized') and not obs.get('result'):
                result = m.get('result', '')
                actual = None

                if NOAA_TOKEN:
                    nr = requests.get(
                        'https://www.ncdc.noaa.gov/cdo-web/api/v2/data',
                        headers={'token': NOAA_TOKEN},
                        params={
                            'datasetid': 'GHCND',
                            'stationid': 'GHCND:USW00094728',
                            'datatypeid': 'TMAX',
                            'startdate': target_date,
                            'enddate':   target_date,
                            'units': 'standard', 'limit': 10,
                        }, timeout=10)
                    if nr.text.strip():
                        noaa_rows = nr.json().get('results', [])
                        if noaa_rows:
                            actual = float(noaa_rows[0]['value'])

                if result:
                    db.update_observation_result(ticker, actual, result)
                    print(f"  Backfilled {ticker}: result={result} actual={actual}")
        except Exception as e:
            print(f"  Error backfilling {ticker}: {e}")


def run_collection():
    now = datetime.now(timezone.utc)
    print(f"\n=== Collection Run — {now.strftime('%Y-%m-%d %H:%M UTC')} ===\n")

    print("Backfilling past results...")
    backfill_results()

    print("\nFetching open buckets...")
    buckets, target_date = get_todays_buckets()
    if not buckets:
        print("  No open buckets found")
        return
    print(f"  Found {len(buckets)} buckets for {target_date}")

    print("\nFetching NWS forecast...")
    nws_high = get_nws_forecast_high(target_date)
    print(f"  NWS high: {nws_high}°F")

    print("\nSaving observations...")
    saved = 0
    for m in buckets:
        price = get_current_price(m)
        floor = float(m['floor_strike'])
        cap   = float(m['cap_strike'])
        nws_tag = '← NWS' if nws_high and floor <= nws_high <= cap else ''
        print(f"  {floor:.0f}-{cap:.0f}°  {price}  {nws_tag}")
        db.insert_observation({
            'collected_at':      now.strftime('%Y-%m-%d %H:%M UTC'),
            'target_date':       target_date,
            'ticker':            m['ticker'],
            'floor':             floor,
            'cap':               cap,
            'price_mid':         price,
            'nws_forecast_high': nws_high,
            'actual_high':       None,
            'result':            '',
        })
        saved += 1

    print(f"\n  {saved} buckets processed (duplicates skipped by DB)")
    print("Done!")


run_collection()
