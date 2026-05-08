import csv
import subprocess
import sys
import time
import base64
import os
import datetime
from pathlib import Path

import requests
from flask import Flask, jsonify, render_template
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))

BASE_DIR = Path('/Users/ankhbayarbatkhurel/kalshi-calibration')
DATA_FILE = BASE_DIR / 'data/daily_observations.csv'
TRADE_LOG  = BASE_DIR / 'data/trade_log.csv'
KEY_FILE   = BASE_DIR / 'kalshi_trading.key'

API_KEY    = os.getenv('KALSHI_API_KEY')
API_KEY_ID = os.getenv('KALSHI_API_KEY_ID')
BASE_URL   = 'https://api.elections.kalshi.com/trade-api/v2'

app = Flask(__name__)

# ── RSA auth ──────────────────────────────────────────────────────────────

def _load_private_key():
    try:
        from cryptography.hazmat.primitives import serialization, hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend
        with open(KEY_FILE, 'rb') as f:
            return serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend())
    except Exception:
        return None

_private_key = _load_private_key()


def _make_headers(method, path):
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding as apad
    ts  = str(int(datetime.datetime.now().timestamp() * 1000))
    msg = f'{ts}{method}{path}'.encode()
    sig = _private_key.sign(
        msg,
        apad.PSS(mgf=apad.MGF1(hashes.SHA256()), salt_length=apad.PSS.DIGEST_LENGTH),
        hashes.SHA256())
    return {
        'KALSHI-ACCESS-KEY': API_KEY_ID,
        'KALSHI-ACCESS-SIGNATURE': base64.b64encode(sig).decode(),
        'KALSHI-ACCESS-TIMESTAMP': ts,
        'Content-Type': 'application/json',
    }


def _auth_get(path):
    try:
        return requests.get(
            BASE_URL + path,
            headers=_make_headers('GET', f'/trade-api/v2{path}'),
            timeout=10).json()
    except Exception as e:
        return {'error': str(e)}

# ── Signal cache ──────────────────────────────────────────────────────────

_signal_cache: dict = {'output': None, 'ts': 0}
SIGNAL_TTL = 300  # seconds


def get_signal_output():
    now = time.time()
    if _signal_cache['output'] and now - _signal_cache['ts'] < SIGNAL_TTL:
        return _signal_cache['output']
    try:
        result = subprocess.run(
            [sys.executable, str(BASE_DIR / 'src/fair_value_v2.py')],
            capture_output=True, text=True, timeout=45,
            cwd=str(BASE_DIR))
        out = result.stdout or ''
        if result.stderr:
            out += '\nSTDERR:\n' + result.stderr
    except subprocess.TimeoutExpired:
        out = 'Signal generation timed out.'
    except Exception as e:
        out = f'Error running signal: {e}'
    _signal_cache['output'] = out
    _signal_cache['ts'] = now
    return out

# ── Data helpers ──────────────────────────────────────────────────────────

def read_csv(path):
    if not path.exists():
        return []
    with open(path, newline='') as f:
        return list(csv.DictReader(f))


def compute_accuracy(obs):
    by_date: dict = {}
    for row in obs:
        if not row.get('result') or not row.get('actual_high'):
            continue
        d = row['target_date']
        by_date.setdefault(d, []).append(row)

    market_wins = market_days = 0
    nws_wins = nws_days = 0
    details = []

    for date in sorted(by_date):
        rows = by_date[date]
        valid = [r for r in rows if r.get('price_mid') and r.get('result') in ('yes', 'no')]
        if not valid:
            continue

        actual_high = rows[0].get('actual_high', '')
        nws_high    = rows[0].get('nws_forecast_high', '')

        # Market: highest-priced bucket is market's best guess
        market_days += 1
        best = max(valid, key=lambda r: float(r['price_mid']))
        market_won = best['result'] == 'yes'
        if market_won:
            market_wins += 1

        # NWS: find bucket spanning nws_forecast_high (inclusive cap for boundary)
        nws_won = None
        try:
            nws = float(nws_high)
            nws_bucket = next(
                (r for r in valid
                 if r.get('floor') and r.get('cap')
                 and float(r['floor']) <= nws <= float(r['cap'])),
                None)
            if nws_bucket:
                nws_days += 1
                nws_won = nws_bucket['result'] == 'yes'
                if nws_won:
                    nws_wins += 1
        except (ValueError, TypeError):
            pass

        details.append({
            'date':         date,
            'actual_high':  actual_high,
            'nws_high':     nws_high,
            'market_pick':  f"{best['floor']}–{best['cap']}°F",
            'market_won':   market_won,
            'nws_won':      nws_won,
        })

    return {
        'market_wins':  market_wins,
        'market_days':  market_days,
        'nws_wins':     nws_wins,
        'nws_days':     nws_days,
        'details':      list(reversed(details)),  # newest first
    }


def get_positions_pnl(trades):
    """Summarise live (non dry-run) trades."""
    live = [t for t in trades if t.get('dry_run') not in ('True', True)]
    total_cost   = sum(float(t['cost']) for t in live)
    settled      = [t for t in live if t.get('pnl') and t['pnl'] != '']
    pending      = [t for t in live if not t.get('pnl') or t['pnl'] == '']
    realized_pnl = sum(float(t['pnl']) for t in settled)
    wins         = sum(1 for t in settled if float(t['pnl']) > 0)
    return {
        'live_trades':   live,
        'total_cost':    round(total_cost, 2),
        'realized_pnl':  round(realized_pnl, 2),
        'settled_count': len(settled),
        'pending_count': len(pending),
        'win_count':     wins,
    }

# ── Routes ────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/data')
def api_data():
    obs    = read_csv(DATA_FILE)
    trades = read_csv(TRADE_LOG)

    accuracy = compute_accuracy(obs)
    pnl      = get_positions_pnl(trades)
    signal   = get_signal_output()

    # Live Kalshi balance
    balance_data = _auth_get('/portfolio/balance')
    balance_cents = balance_data.get('balance', 0)
    balance = round(balance_cents / 100, 2) if isinstance(balance_cents, (int, float)) else None

    # Observations: most recent 60, newest first
    obs_display = list(reversed(obs[-60:]))

    return jsonify({
        'balance':    balance,
        'pnl':        pnl,
        'accuracy':   accuracy,
        'signal':     signal,
        'signal_cached': _signal_cache['ts'] > 0,
        'signal_age': int(time.time() - _signal_cache['ts']) if _signal_cache['ts'] else None,
        'observations': obs_display,
        'updated_at': datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
    })


@app.route('/api/signal/refresh')
def api_signal_refresh():
    """Force-expire the signal cache and regenerate."""
    _signal_cache['ts'] = 0
    output = get_signal_output()
    return jsonify({'signal': output, 'ok': True})


if __name__ == '__main__':
    app.run(debug=True, port=5050, host='0.0.0.0')
