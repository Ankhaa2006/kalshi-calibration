import requests
import datetime
import base64
import os
import uuid
import sys
from pathlib import Path
from scipy import stats
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
import db

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY    = os.getenv('KALSHI_API_KEY')
API_KEY_ID = os.getenv('KALSHI_API_KEY_ID')
BASE_URL   = 'https://api.elections.kalshi.com/trade-api/v2'
read_headers = {'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json'}

# ── RSA Auth ──────────────────────────────────────────────────────────────

with open('/Users/ankhbayarbatkhurel/kalshi-calibration/kalshi_trading.key', 'rb') as f:
    _private_key = serialization.load_pem_private_key(
        f.read(), password=None, backend=default_backend())


def make_headers(method, path):
    ts  = str(int(datetime.datetime.now().timestamp() * 1000))
    msg = f'{ts}{method}{path}'.encode()
    sig = _private_key.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256())
    return {
        'KALSHI-ACCESS-KEY':       API_KEY_ID,
        'KALSHI-ACCESS-SIGNATURE': base64.b64encode(sig).decode(),
        'KALSHI-ACCESS-TIMESTAMP': ts,
        'Content-Type':            'application/json',
    }


def auth_get(path):
    return requests.get(BASE_URL + path,
                        headers=make_headers('GET', f'/trade-api/v2{path}'),
                        timeout=10)


def auth_post(path, data):
    return requests.post(BASE_URL + path,
                         headers=make_headers('POST', f'/trade-api/v2{path}'),
                         json=data, timeout=10)

# ── Market data ───────────────────────────────────────────────────────────

def get_open_buckets():
    r = requests.get(f'{BASE_URL}/markets', headers=read_headers,
                     params={'series_ticker': 'KXHIGHNY', 'status': 'open', 'limit': 50})
    markets = r.json().get('markets', [])
    events: dict = {}
    for m in markets:
        event = m.get('event_ticker', '')
        events.setdefault(event, []).append(m)
    if not events:
        return [], None, None
    next_event = min(events, key=lambda e: events[e][0].get('close_time', '9999'))
    buckets = events[next_event]
    target_date = buckets[0].get('close_time', '')[:10]
    return buckets, target_date, next_event


def get_mid(m):
    bid, ask = m.get('yes_bid_dollars'), m.get('yes_ask_dollars')
    if bid and ask:
        return (float(bid) + float(ask)) / 2
    return None


def extract_distribution(buckets):
    range_buckets, thresh_above, thresh_below = [], None, None
    for m in buckets:
        floor, cap, mid = m.get('floor_strike'), m.get('cap_strike'), get_mid(m)
        if mid is None:
            continue
        if floor and cap:
            range_buckets.append({'floor': float(floor), 'cap': float(cap),
                                  'mid_temp': (float(floor) + float(cap)) / 2,
                                  'raw_prob': mid, 'ticker': m['ticker']})
        elif floor and not cap:
            thresh_above = {'floor': float(floor), 'raw_prob': mid, 'ticker': m['ticker']}
        elif cap and not floor:
            thresh_below = {'cap': float(cap), 'raw_prob': mid, 'ticker': m['ticker']}

    total = sum(b['raw_prob'] for b in range_buckets)
    if thresh_above: total += thresh_above['raw_prob']
    if thresh_below: total += thresh_below['raw_prob']
    for b in range_buckets:
        b['prob'] = b['raw_prob'] / total
    if thresh_above: thresh_above['prob'] = thresh_above['raw_prob'] / total
    if thresh_below: thresh_below['prob'] = thresh_below['raw_prob'] / total
    return range_buckets, thresh_above, thresh_below


def market_implied_stats(range_buckets, thresh_above=None, thresh_below=None):
    pts = [(b['mid_temp'], b['prob']) for b in range_buckets]
    if thresh_above: pts.append((thresh_above['floor'] + 2, thresh_above['prob']))
    if thresh_below: pts.append((thresh_below['cap']  - 2, thresh_below['prob']))
    total = sum(p for _, p in pts)
    mean  = sum(t * p for t, p in pts) / total
    var   = sum(p * (t - mean)**2 for t, p in pts) / total
    return mean, var ** 0.5


def get_nws_forecast(target_date):
    try:
        r = requests.get(
            'https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly',
            headers={'User-Agent': 'kalshi-research'}, timeout=10)
        periods = r.json()['properties']['periods']
        temps = [p['temperature'] for p in periods if target_date in p['startTime']]
        if not temps:
            return None, None
        return max(temps), (3.5 if max(temps) - min(temps) > 15 else 4.5)
    except Exception:
        return None, None

# ── Probability helpers ───────────────────────────────────────────────────

def bucket_prob(floor, cap, mu, sigma):
    return stats.norm(mu, sigma).cdf(cap) - stats.norm(mu, sigma).cdf(floor)


def above_prob(threshold, mu, sigma):
    return 1 - stats.norm(mu, sigma).cdf(threshold)


def below_prob(threshold, mu, sigma):
    return stats.norm(mu, sigma).cdf(threshold)


def breakeven_yes(price, fee=0.07):
    return price / ((1 - price) * (1 - fee) + price)


def breakeven_no(yes_price, fee=0.07):
    return (1 - yes_price) / (yes_price * (1 - fee) + (1 - yes_price))

# ── Kelly position sizing ─────────────────────────────────────────────────

def kelly_contracts(direction, post_prob, mkt_price, balance, fee=0.07,
                    kelly_fraction=0.5, max_pct=0.15, min_contracts=1, max_contracts=10):
    """
    Half-Kelly sizing capped at max_pct of balance.
    Returns number of contracts.
    """
    if direction == 'YES':
        b   = (1 - mkt_price) * (1 - fee) / mkt_price   # net odds per $ risked
        p   = post_prob
    else:
        no_price = 1 - mkt_price
        b   = mkt_price * (1 - fee) / no_price
        p   = 1 - post_prob

    q = 1 - p
    raw_kelly = (p * b - q) / b if b > 0 else 0
    fraction  = max(0.0, raw_kelly * kelly_fraction)
    fraction  = min(fraction, max_pct)

    cost_per   = mkt_price if direction == 'YES' else (1 - mkt_price)
    max_risk   = balance * fraction
    contracts  = int(max_risk / cost_per) if cost_per > 0 else 0
    return max(min_contracts, min(max_contracts, contracts))

# ── Signal generation ─────────────────────────────────────────────────────

def generate_signals(range_buckets, thresh_above, thresh_below,
                     mu, sigma, edge_buffer=0.03):
    signals = []

    for b in sorted(range_buckets, key=lambda x: x['floor']):
        mkt = b['raw_prob']
        pp  = bucket_prob(b['floor'], b['cap'], mu, sigma)
        be_yes, be_no = breakeven_yes(mkt), breakeven_no(mkt)
        label = f"{b['floor']:.0f}-{b['cap']:.0f}°"
        if pp > be_yes + edge_buffer:
            signals.append(('YES', label, b['ticker'], mkt, pp, pp - be_yes))
        elif (1 - pp) > be_no + edge_buffer:
            signals.append(('NO', label, b['ticker'], mkt, pp, (1-pp) - be_no))

    for thresh, is_above in [(thresh_above, True), (thresh_below, False)]:
        if not thresh:
            continue
        mkt = thresh['raw_prob']
        pp  = above_prob(thresh['floor'], mu, sigma) if is_above \
              else below_prob(thresh['cap'], mu, sigma)
        label   = f">{thresh['floor']:.0f}°" if is_above else f"<{thresh['cap']:.0f}°"
        be_yes  = breakeven_yes(mkt)
        be_no   = breakeven_no(mkt)
        if pp > be_yes + edge_buffer:
            signals.append(('YES', label, thresh['ticker'], mkt, pp, pp - be_yes))
        elif (1 - pp) > be_no + edge_buffer:
            signals.append(('NO', label, thresh['ticker'], mkt, pp, (1-pp) - be_no))

    return signals

# ── P&L update ────────────────────────────────────────────────────────────

def update_pnl():
    pending = db.get_unsettled_live_trades()
    for trade in pending:
        ticker = trade['ticker']
        try:
            r = requests.get(f'{BASE_URL}/markets/{ticker}',
                             headers=read_headers, timeout=10)
            m = r.json().get('market', {})
            if m.get('status') not in ('settled', 'finalized'):
                continue
            result = m.get('result', '')
            side, count, price_cents = trade['side'], trade['count'], trade['price_cents']
            cost = trade['cost']
            won  = (side == 'yes' and result == 'yes') or \
                   (side == 'no'  and result == 'no')
            if won:
                gross = count * (1 - price_cents/100) if side == 'yes' \
                        else count * (price_cents/100)
                pnl = gross * 0.93 - cost
            else:
                pnl = -cost
            db.update_trade_pnl(trade['client_order_id'], round(pnl, 4), result)
            print(f"  Settled {ticker}: {'WIN' if won else 'LOSS'} ${pnl:+.2f}")
        except Exception as e:
            print(f"  Error checking {ticker}: {e}")


def print_pnl_summary():
    live = db.get_live_trades()
    if not live:
        print("  No live trades")
        return
    settled = [t for t in live if t['pnl'] is not None]
    pending = [t for t in live if t['pnl'] is None]
    total_cost   = sum(t['cost'] for t in live)
    realized_pnl = sum(t['pnl'] for t in settled)
    wins         = sum(1 for t in settled if t['pnl'] > 0)
    print(f"\n  {'Timestamp':<22} {'Ticker':<30} {'Side':<4} {'Cost':>6} {'P&L':>8}")
    print(f"  {'-'*75}")
    for t in live:
        pnl_s = f"${t['pnl']:+.2f}" if t['pnl'] is not None else 'pending'
        print(f"  {t['timestamp']:<22} {t['ticker']:<30} {t['side']:<4} "
              f"${t['cost']:>5.2f} {pnl_s:>8}")
    print(f"\n  Invested: ${total_cost:.2f}  |  "
          f"Settled: {len(settled)}  |  Pending: {len(pending)}")
    if settled:
        print(f"  Win rate: {wins}/{len(settled)}  |  "
              f"Realized P&L: ${realized_pnl:+.2f}  |  "
              f"ROI: {100*realized_pnl/total_cost:+.1f}%")

# ── Data collection ───────────────────────────────────────────────────────

def collect_data(buckets, target_date, nws_high):
    now = datetime.datetime.now(datetime.timezone.utc)
    saved = 0
    for m in buckets:
        floor, cap = m.get('floor_strike'), m.get('cap_strike')
        if not floor or not cap:
            continue
        db.insert_observation({
            'collected_at':      now.strftime('%Y-%m-%d %H:%M UTC'),
            'target_date':       target_date,
            'ticker':            m['ticker'],
            'floor':             float(floor),
            'cap':               float(cap),
            'price_mid':         get_mid(m),
            'nws_forecast_high': nws_high,
            'actual_high':       None,
            'result':            '',
        })
        saved += 1
    print(f"  Saved {saved} observations (duplicates skipped)")

# ── Order execution ───────────────────────────────────────────────────────

def place_order(ticker, side, count, price_cents, dry_run=True):
    client_order_id = str(uuid.uuid4())
    cost = count * (price_cents / 100)
    prefix = '[DRY RUN] ' if dry_run else ''
    print(f"\n  {prefix}Order: {side.upper()} {ticker}")
    print(f"    {count} contracts @ {price_cents}¢  →  ${cost:.2f}")

    if dry_run:
        print(f"    Status: SIMULATED")
        db.insert_trade({
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M UTC'),
            'ticker': ticker, 'side': side, 'action': 'buy',
            'count': count, 'price_cents': price_cents, 'cost': cost,
            'order_id': 'simulated', 'client_order_id': client_order_id,
            'dry_run': 1, 'pnl': None, 'result': '',
        })
        return True

    order_data = {
        'ticker': ticker, 'action': 'buy', 'side': side, 'count': count,
        'type': 'limit',
        'yes_price': price_cents if side == 'yes' else 100 - price_cents,
        'client_order_id': client_order_id,
    }
    r = auth_post('/portfolio/orders', order_data)
    if r.status_code == 201:
        order_id = r.json().get('order', {}).get('order_id', '')
        print(f"    Status: FILLED ✅  order_id={order_id}")
        db.insert_trade({
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M UTC'),
            'ticker': ticker, 'side': side, 'action': 'buy',
            'count': count, 'price_cents': price_cents, 'cost': cost,
            'order_id': order_id, 'client_order_id': client_order_id,
            'dry_run': 0, 'pnl': None, 'result': '',
        })
        return True
    else:
        print(f"    Status: FAILED ❌  {r.status_code} {r.text[:120]}")
        return False

# ── MAIN ──────────────────────────────────────────────────────────────────

def main():
    now = datetime.datetime.now(datetime.timezone.utc)
    print("=" * 60)
    print("KALSHI DAILY RUNNER")
    print(f"Time: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Step 1: P&L update
    print("\n📊 Step 1: Updating P&L...")
    update_pnl()
    print_pnl_summary()

    # Step 2: Balance
    balance = auth_get('/portfolio/balance').json().get('balance', 0) / 100
    print(f"\n💰 Balance: ${balance:.2f}")

    # Step 3: Market data
    print("\n📥 Step 2: Collecting market data...")
    buckets, target_date, event_ticker = get_open_buckets()
    if not buckets:
        print("  No open markets found")
        return
    print(f"  Target date: {target_date}")

    nws_high, nws_sigma = get_nws_forecast(target_date)
    print(f"  NWS forecast: {nws_high}°F  (informational only — NWS disabled)")
    collect_data(buckets, target_date, nws_high)

    # Step 4: Fair value model (market-only posterior)
    print("\n🧮 Step 3: Fair value model (market distribution only)...")
    range_buckets, thresh_above, thresh_below = extract_distribution(buckets)

    max_price = max((b['raw_prob'] for b in range_buckets), default=0)
    if max_price < 0.05 or max_price > 0.90:
        print("  ⚠️  Market near resolution — no signals today")
        return

    mu, sigma = market_implied_stats(range_buckets, thresh_above, thresh_below)
    print(f"  Market posterior: {mu:.1f}°F ± {sigma:.1f}°F")
    print(f"  NWS (unused):     {nws_high}°F  "
          f"| divergence: {(nws_high - mu):+.1f}°F" if nws_high else "")

    # Step 5: Signals
    print("\n⚡ Step 4: Signal detection...")
    signals = generate_signals(range_buckets, thresh_above, thresh_below, mu, sigma)

    if not signals:
        print("  No trades recommended today")
        return

    print(f"\n  {'Dir':<5} {'Bucket':<10} {'Mkt%':>7} {'FV%':>7} {'Edge':>7}")
    print(f"  {'-'*42}")
    for direction, label, ticker, mkt, fv, edge in signals:
        print(f"  {direction:<5} {label:<10} {mkt*100:>6.1f}% {fv*100:>6.1f}% {edge*100:>+6.1f}%")

    # Step 6: Execute best signal (with duplicate check)
    print(f"\n🚀 Step 5: Trade execution...")
    signals_sorted = sorted(signals, key=lambda x: x[5], reverse=True)

    best = None
    for sig in signals_sorted:
        if db.has_open_position(sig[2]):
            print(f"  Skipping {sig[2]} — already have an open position")
            continue
        best = sig
        break

    if best is None:
        print("  All signals have existing open positions — no new trade")
        return

    direction, label, ticker, mkt_price, fv, edge = best
    price_cents = int(mkt_price * 100) if direction == 'YES' else int((1 - mkt_price) * 100)
    contracts   = kelly_contracts(direction, fv, mkt_price, balance)
    cost        = contracts * (mkt_price if direction == 'YES' else (1 - mkt_price))

    print(f"  Best signal: {direction} on {label} ({ticker})")
    print(f"  Edge: {edge*100:.1f}%  |  Kelly contracts: {contracts}  |  Cost: ${cost:.2f}")

    if edge > 0.15:
        place_order(ticker, direction.lower(), contracts, price_cents, dry_run=False)
    elif edge > 0.05:
        print(f"  Edge {edge*100:.1f}% below 15% threshold — paper trading only")
        place_order(ticker, direction.lower(), contracts, price_cents, dry_run=True)
    else:
        print("  Insufficient edge — no trade")

    print(f"\n{'='*60}")
    print("Done.")
    print(f"{'='*60}")


main()
