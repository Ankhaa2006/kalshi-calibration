"""
One-time migration: CSV files → SQLite.
Safe to re-run (INSERT OR IGNORE skips duplicates).
"""
import csv
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))
import db

BASE = Path('/Users/ankhbayarbatkhurel/kalshi-calibration/data')
OBS_CSV   = BASE / 'daily_observations.csv'
TRADE_CSV = BASE / 'trade_log.csv'


def migrate_observations():
    if not OBS_CSV.exists():
        print("  observations CSV not found — skipping")
        return
    with open(OBS_CSV, newline='') as f:
        rows = list(csv.DictReader(f))
    count = 0
    for r in rows:
        db.insert_observation({
            'collected_at':      r.get('collected_at', ''),
            'target_date':       r.get('target_date', ''),
            'ticker':            r.get('ticker', ''),
            'floor':             float(r['floor'])  if r.get('floor')  else None,
            'cap':               float(r['cap'])    if r.get('cap')    else None,
            'price_mid':         float(r['price_mid']) if r.get('price_mid') else None,
            'nws_forecast_high': float(r['nws_forecast_high']) if r.get('nws_forecast_high') else None,
            'actual_high':       float(r['actual_high']) if r.get('actual_high') else None,
            'result':            r.get('result', ''),
        })
        count += 1
    print(f"  Observations: {count} rows processed → {db.DB_PATH}")


def migrate_trades():
    if not TRADE_CSV.exists():
        print("  trade_log CSV not found — skipping")
        return
    with open(TRADE_CSV, newline='') as f:
        rows = list(csv.DictReader(f))
    count = 0
    for r in rows:
        db.insert_trade({
            'timestamp':        r.get('timestamp', ''),
            'ticker':           r.get('ticker', ''),
            'side':             r.get('side', ''),
            'action':           r.get('action', ''),
            'count':            int(r['count']) if r.get('count') else None,
            'price_cents':      int(r['price_cents']) if r.get('price_cents') else None,
            'cost':             float(r['cost']) if r.get('cost') else None,
            'order_id':         r.get('order_id', ''),
            'client_order_id':  r.get('client_order_id', ''),
            'dry_run':          1 if r.get('dry_run') == 'True' else 0,
            'pnl':              float(r['pnl']) if r.get('pnl') else None,
            'result':           r.get('result', ''),
        })
        count += 1
    print(f"  Trades: {count} rows processed → {db.DB_PATH}")


if __name__ == '__main__':
    print("Migrating CSVs → SQLite...")
    migrate_observations()
    migrate_trades()
    obs   = db.get_all_observations()
    trdas = db.get_all_trades()
    print(f"\nDB now has {len(obs)} observations, {len(trdas)} trades.")
    print("Migration complete.")
