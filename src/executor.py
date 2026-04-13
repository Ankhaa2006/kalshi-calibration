import requests
import datetime
import base64
import os
import uuid
import json
import csv
from pathlib import Path
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY_ID = os.getenv('KALSHI_API_KEY_ID')
BASE_URL = 'https://api.elections.kalshi.com/trade-api/v2'
TRADE_LOG = Path('/Users/ankhbayarbatkhurel/kalshi-calibration/data/trade_log.csv')

# ── Auth ──────────────────────────────────────────────────────────────────

with open('/Users/ankhbayarbatkhurel/kalshi-calibration/kalshi_trading.key', 'rb') as f:
    private_key = serialization.load_pem_private_key(
        f.read(), password=None, backend=default_backend())

def make_headers(method, path):
    ts = str(int(datetime.datetime.now().timestamp() * 1000))
    msg = f'{ts}{method}{path}'.encode()
    sig = private_key.sign(msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256())
    return {
        'KALSHI-ACCESS-KEY': API_KEY_ID,
        'KALSHI-ACCESS-SIGNATURE': base64.b64encode(sig).decode(),
        'KALSHI-ACCESS-TIMESTAMP': ts,
        'Content-Type': 'application/json'
    }

def get(path):
    return requests.get(BASE_URL + path,
                       headers=make_headers('GET', f'/trade-api/v2{path}'),
                       timeout=10)

def post(path, data):
    return requests.post(BASE_URL + path,
                        headers=make_headers('POST', f'/trade-api/v2{path}'),
                        json=data, timeout=10)

# ── Portfolio ─────────────────────────────────────────────────────────────

def get_balance():
    r = get('/portfolio/balance')
    return r.json().get('balance', 0) / 100  # convert cents to dollars

def get_positions():
    r = get('/portfolio/positions')
    return r.json().get('market_positions', [])

# ── Order placement ───────────────────────────────────────────────────────

def place_order(ticker, side, action, count, price_cents,
                dry_run=True):
    """
    ticker:      e.g. KXHIGHNY-26APR13-T81
    side:        'yes' or 'no'
    action:      'buy'
    count:       number of contracts
    price_cents: integer 1-99
    dry_run:     if True, simulate without placing real order
    """
    client_order_id = str(uuid.uuid4())

    order_data = {
        "ticker": ticker,
        "action": action,
        "side": side,
        "count": count,
        "type": "limit",
        "yes_price": price_cents if side == "yes" else 100 - price_cents,
        "client_order_id": client_order_id
    }

    cost = count * (price_cents / 100)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Order:")
    print(f"  Ticker:  {ticker}")
    print(f"  Side:    {side.upper()} {action.upper()}")
    print(f"  Count:   {count} contracts")
    print(f"  Price:   {price_cents}¢")
    print(f"  Cost:    ${cost:.2f}")

    if dry_run:
        print("  Status:  SIMULATED (dry_run=True)")
        log_trade(ticker, side, action, count, price_cents,
                 cost, "simulated", client_order_id, dry_run=True)
        return {"simulated": True, "client_order_id": client_order_id}

    # Real order
    r = post('/portfolio/orders', order_data)

    if r.status_code == 201:
        order = r.json().get('order', {})
        print(f"  Status:  FILLED ✅  order_id={order.get('order_id')}")
        log_trade(ticker, side, action, count, price_cents,
                 cost, order.get('order_id'), client_order_id, dry_run=False)
        return order
    else:
        print(f"  Status:  FAILED ❌  {r.status_code} {r.text[:100]}")
        return None

# ── Trade logging ─────────────────────────────────────────────────────────

def log_trade(ticker, side, action, count, price_cents,
              cost, order_id, client_order_id, dry_run):
    file_exists = TRADE_LOG.exists()
    with open(TRADE_LOG, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'timestamp', 'ticker', 'side', 'action', 'count',
            'price_cents', 'cost', 'order_id', 'client_order_id',
            'dry_run', 'pnl', 'result'
        ])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M UTC'),
            'ticker': ticker,
            'side': side,
            'action': action,
            'count': count,
            'price_cents': price_cents,
            'cost': cost,
            'order_id': order_id,
            'client_order_id': client_order_id,
            'dry_run': dry_run,
            'pnl': '',
            'result': ''
        })

# ── Main: test with dry run first ─────────────────────────────────────────

if __name__ == "__main__":
    print(f"Balance: ${get_balance():.2f}\n")

    # Paper trade today's signals
    signals = [
        ("KXHIGHNY-26APR13-T81", "yes", "buy", 10, 6),   # BUY YES >81°
        ("KXHIGHNY-26APR13-B78.5", "no", "buy", 1, 57),  # BUY NO 78-79°
    ]

    for ticker, side, action, count, price in signals:
        place_order(ticker, side, action, count, price, dry_run=True)

    print(f"\nTrade log: {TRADE_LOG}")
    print("Set dry_run=False to execute real orders")