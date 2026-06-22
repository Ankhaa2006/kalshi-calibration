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

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
import db

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))

BASE_DIR   = Path('/Users/ankhbayarbatkhurel/kalshi-calibration')
KEY_FILE   = BASE_DIR / 'kalshi_trading.key'
API_KEY    = os.getenv('KALSHI_API_KEY')
API_KEY_ID = os.getenv('KALSHI_API_KEY_ID')
BASE_URL   = 'https://api.elections.kalshi.com/trade-api/v2'

app = Flask(__name__)

# ── RSA auth ──────────────────────────────────────────────────────────────

def _load_private_key():
    try:
        from cryptography.hazmat.primitives import serialization
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
        'KALSHI-ACCESS-KEY':       API_KEY_ID,
        'KALSHI-ACCESS-SIGNATURE': base64.b64encode(sig).decode(),
        'KALSHI-ACCESS-TIMESTAMP': ts,
        'Content-Type':            'application/json',
    }


def _auth_get(path):
    try:
        return requests.get(BASE_URL + path,
                            headers=_make_headers('GET', f'/trade-api/v2{path}'),
                            timeout=10).json()
    except Exception as e:
        return {'error': str(e)}

# ── Signal cache ──────────────────────────────────────────────────────────

_signal_cache: dict = {'output': None, 'ts': 0}
SIGNAL_TTL = 300


def get_signal_output():
    now = time.time()
    if _signal_cache['output'] and now - _signal_cache['ts'] < SIGNAL_TTL:
        return _signal_cache['output']
    try:
        result = subprocess.run(
            [sys.executable, str(BASE_DIR / 'src/model.py')],
            capture_output=True, text=True, timeout=45, cwd=str(BASE_DIR))
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

# ── Analytics ─────────────────────────────────────────────────────────────

def compute_accuracy(obs: list[dict]) -> dict:
    by_date: dict = {}
    for row in obs:
        if not row.get('result') or not row.get('actual_high'):
            continue
        by_date.setdefault(row['target_date'], []).append(row)

    market_wins = market_days = nws_wins = nws_days = 0
    details = []

    for date in sorted(by_date):
        rows  = by_date[date]
        valid = [r for r in rows
                 if r.get('price_mid') is not None and r.get('result') in ('yes', 'no')]
        if not valid:
            continue

        actual_high = rows[0].get('actual_high', '')
        nws_high    = rows[0].get('nws_forecast_high', '')

        market_days += 1
        best       = max(valid, key=lambda r: r['price_mid'] or 0)
        market_won = best['result'] == 'yes'
        if market_won:
            market_wins += 1

        nws_won = None
        try:
            nws = float(nws_high)
            nws_bucket = next(
                (r for r in valid
                 if r.get('floor') is not None and r.get('cap') is not None
                 and float(r['floor']) <= nws <= float(r['cap'])),
                None)
            if nws_bucket:
                nws_days += 1
                nws_won   = nws_bucket['result'] == 'yes'
                if nws_won:
                    nws_wins += 1
        except (ValueError, TypeError):
            pass

        details.append({
            'date':        date,
            'actual_high': actual_high,
            'nws_high':    nws_high,
            'market_pick': f"{best['floor']:.0f}–{best['cap']:.0f}°F"
                           if best.get('floor') else '—',
            'market_won':  market_won,
            'nws_won':     nws_won,
        })

    return {
        'market_wins':  market_wins,
        'market_days':  market_days,
        'nws_wins':     nws_wins,
        'nws_days':     nws_days,
        'details':      list(reversed(details)),
    }


def compute_calibration(obs: list[dict]) -> list[dict]:
    """
    Group settled observations into 10-point price buckets.
    Returns: [{bucket, label, total, wins, win_rate}]
    """
    buckets_raw: dict = {}
    for row in obs:
        if row.get('result') not in ('yes', 'no') or row.get('price_mid') is None:
            continue
        p   = float(row['price_mid'])
        key = int(p * 10) / 10          # floor to nearest 0.1
        key = min(key, 0.9)             # cap top at 0.9 bucket
        buckets_raw.setdefault(key, []).append(row['result'] == 'yes')

    result = []
    for lo in [i/10 for i in range(10)]:
        rows = buckets_raw.get(lo, [])
        wins = sum(rows)
        total = len(rows)
        result.append({
            'bucket':   lo,
            'label':    f"{int(lo*100)}–{int(lo*100)+10}¢",
            'total':    total,
            'wins':     wins,
            'win_rate': round(wins / total, 3) if total else None,
        })
    return result


def get_positions_pnl(trades: list[dict]) -> dict:
    live     = [t for t in trades if not t.get('dry_run')]
    total_cost   = sum(t['cost'] or 0 for t in live)
    settled      = [t for t in live if t.get('pnl') is not None]
    pending      = [t for t in live if t.get('pnl') is None]
    realized_pnl = sum(t['pnl'] for t in settled)
    wins         = sum(1 for t in settled if t['pnl'] > 0)
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
    obs    = db.get_all_observations()
    trades = db.get_all_trades()

    accuracy    = compute_accuracy(obs)
    calibration = compute_calibration(obs)
    pnl         = get_positions_pnl(trades)
    signal      = get_signal_output()

    balance_data  = _auth_get('/portfolio/balance')
    balance_cents = balance_data.get('balance', 0)
    balance = round(balance_cents / 100, 2) if isinstance(balance_cents, (int, float)) else None

    # Serialize for JSON (convert None-ish floats, booleans, etc.)
    def clean_trade(t):
        return {
            'timestamp':   t.get('timestamp', ''),
            'ticker':      t.get('ticker', ''),
            'side':        t.get('side', ''),
            'count':       t.get('count'),
            'price_cents': t.get('price_cents'),
            'cost':        t.get('cost'),
            'pnl':         t.get('pnl'),
            'result':      t.get('result', ''),
            'dry_run':     bool(t.get('dry_run')),
        }

    obs_display = list(reversed(obs[-60:]))

    return jsonify({
        'balance':      balance,
        'pnl': {
            **pnl,
            'live_trades': [clean_trade(t) for t in pnl['live_trades']],
        },
        'accuracy':     accuracy,
        'calibration':  calibration,
        'signal':       signal,
        'signal_age':   int(time.time() - _signal_cache['ts']) if _signal_cache['ts'] else None,
        'observations': [dict(o) for o in obs_display],
        'updated_at':   datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
    })


@app.route('/api/signal/refresh')
def api_signal_refresh():
    _signal_cache['ts'] = 0
    output = get_signal_output()
    return jsonify({'signal': output, 'ok': True})


if __name__ == '__main__':
    app.run(debug=True, port=5050, host='0.0.0.0')
