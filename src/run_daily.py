import sys
import requests
import datetime
import base64
import os
import csv
import time
from pathlib import Path
from scipy import stats
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
import db

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY    = os.getenv("KALSHI_API_KEY")
API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
NOAA_TOKEN = os.getenv("NOAA_TOKEN")
BASE_URL   = "https://api.elections.kalshi.com/trade-api/v2"
read_headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
DATA_FILE  = Path('/Users/ankhbayarbatkhurel/kalshi-calibration/data/daily_observations.csv')
TRADE_LOG  = Path('/Users/ankhbayarbatkhurel/kalshi-calibration/data/trade_log.csv')

# ── RSA Auth ──────────────────────────────────────────────────────────────

with open('/Users/ankhbayarbatkhurel/kalshi-calibration/kalshi_trading.key', 'rb') as f:
    private_key = serialization.load_pem_private_key(
        f.read(), password=None, backend=default_backend())

def make_headers(method, path):
    ts  = str(int(datetime.datetime.now().timestamp() * 1000))
    msg = f'{ts}{method}{path}'.encode()
    sig = private_key.sign(msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256())
    return {
        'KALSHI-ACCESS-KEY':       API_KEY_ID,
        'KALSHI-ACCESS-SIGNATURE': base64.b64encode(sig).decode(),
        'KALSHI-ACCESS-TIMESTAMP': ts,
        'Content-Type':            'application/json'
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
    r = requests.get(f"{BASE_URL}/markets", headers=read_headers,
        params={"series_ticker": "KXHIGHNY", "status": "open", "limit": 50})
    markets = r.json().get("markets", [])
    events = {}
    for m in markets:
        event = m.get("event_ticker", "")
        if event not in events:
            events[event] = []
        events[event].append(m)
    if not events:
        return [], None, None
    next_event = min(events.keys(),
        key=lambda e: events[e][0].get("close_time", "9999"))
    buckets     = events[next_event]
    target_date = buckets[0].get("close_time", "")[:10]
    return buckets, target_date, next_event

def get_mid(m):
    bid = m.get("yes_bid_dollars")
    ask = m.get("yes_ask_dollars")
    if bid and ask:
        return (float(bid) + float(ask)) / 2
    return None

def extract_distribution(buckets):
    range_buckets, thresh_above, thresh_below = [], None, None
    for m in buckets:
        floor = m.get("floor_strike")
        cap   = m.get("cap_strike")
        mid   = get_mid(m)
        if mid is None:
            continue
        if floor and cap:
            range_buckets.append({
                "floor": float(floor), "cap": float(cap),
                "mid_temp": (float(floor) + float(cap)) / 2,
                "raw_prob": mid, "ticker": m["ticker"]
            })
        elif floor and not cap:
            thresh_above = {"floor": float(floor), "raw_prob": mid, "ticker": m["ticker"]}
        elif cap and not floor:
            thresh_below = {"cap": float(cap), "raw_prob": mid, "ticker": m["ticker"]}

    total = sum(b["raw_prob"] for b in range_buckets)
    if thresh_above: total += thresh_above["raw_prob"]
    if thresh_below: total += thresh_below["raw_prob"]
    for b in range_buckets:
        b["prob"] = b["raw_prob"] / total
    if thresh_above: thresh_above["prob"] = thresh_above["raw_prob"] / total
    if thresh_below: thresh_below["prob"] = thresh_below["raw_prob"] / total
    return range_buckets, thresh_above, thresh_below

def market_implied_stats(range_buckets, thresh_above=None, thresh_below=None):
    all_points = [(b["mid_temp"], b["prob"]) for b in range_buckets]
    if thresh_above: all_points.append((thresh_above["floor"] + 2, thresh_above["prob"]))
    if thresh_below: all_points.append((thresh_below["cap"] - 2, thresh_below["prob"]))
    total = sum(p for _, p in all_points)
    mean  = sum(t * p for t, p in all_points) / total
    var   = sum(p * (t - mean)**2 for t, p in all_points) / total
    return mean, var ** 0.5

def get_nws_forecast(target_date):
    try:
        r = requests.get(
            "https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly",
            headers={"User-Agent": "kalshi-research"}, timeout=10)
        periods = r.json()["properties"]["periods"]
        temps   = [p["temperature"] for p in periods if target_date in p["startTime"]]
        if not temps: return None, None
        nws_high  = max(temps)
        nws_sigma = 3.5 if (max(temps) - min(temps)) > 15 else 4.5
        return nws_high, nws_sigma
    except Exception:
        return None, None

def bayesian_update(market_mean, market_std, nws_mean, nws_std,
                    market_weight=0.85, nws_weight=0.15):
    market_var = (market_std ** 2) / market_weight
    nws_var    = (nws_std   ** 2) / nws_weight
    post_var   = 1 / (1/market_var + 1/nws_var)
    post_mean  = post_var * (market_mean/market_var + nws_mean/nws_var)
    return post_mean, post_var ** 0.5

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

# ── Network check ─────────────────────────────────────────────────────────

def wait_for_network(host='api.elections.kalshi.com', max_tries=12, interval=5):
    """Block up to 60 s waiting for DNS to resolve. Returns True when ready."""
    import socket
    for i in range(max_tries):
        try:
            socket.getaddrinfo(host, 443)
            return True
        except socket.gaierror:
            if i < max_tries - 1:
                print(f"  DNS not ready yet (attempt {i+1}/{max_tries}) — retrying in {interval}s")
                time.sleep(interval)
    return False

# ── CSV → SQLite backfill ─────────────────────────────────────────────────

def sync_csv_to_db():
    """Push all CSV trades into SQLite (INSERT OR IGNORE, so it's idempotent)."""
    if not TRADE_LOG.exists():
        return 0
    with open(TRADE_LOG, 'r') as f:
        trades = list(csv.DictReader(f))
    synced = 0
    for t in trades:
        try:
            db.insert_trade({
                'timestamp':       t['timestamp'],
                'ticker':          t['ticker'],
                'side':            t['side'],
                'action':          t.get('action', 'buy'),
                'count':           int(t['count']),
                'price_cents':     int(t['price_cents']),
                'cost':            float(t['cost']),
                'order_id':        t['order_id'],
                'client_order_id': t['client_order_id'],
                'dry_run':         1 if t.get('dry_run') == 'True' else 0,
                'pnl':             float(t['pnl']) if t.get('pnl') else None,
                'result':          t.get('result', ''),
            })
            synced += 1
        except Exception:
            pass
    # Push any settled P&L that's in CSV but not yet reflected in SQLite
    for t in trades:
        if t.get('pnl') and t.get('dry_run') == 'False':
            try:
                db.update_trade_pnl(
                    t['client_order_id'], float(t['pnl']), t.get('result', ''))
            except Exception:
                pass
    return synced

# ── P&L Tracker ───────────────────────────────────────────────────────────

def update_pnl():
    """Check all trades in log and update P&L for settled ones."""
    if not TRADE_LOG.exists():
        return []

    with open(TRADE_LOG, 'r') as f:
        trades = list(csv.DictReader(f))

    updated = False
    summary = []

    for trade in trades:
        if trade.get('dry_run') == 'True':
            continue
        if trade.get('pnl'):
            summary.append(trade)
            continue

        ticker = trade['ticker']
        try:
            r = requests.get(f"{BASE_URL}/markets/{ticker}",
                             headers=read_headers, timeout=10)
            m = r.json().get('market', {})

            if m.get('status') in ('settled', 'finalized'):
                result      = m.get('result', '')
                side        = trade['side']
                count       = int(trade['count'])
                price_cents = int(trade['price_cents'])
                cost        = float(trade['cost'])

                won = (side == 'yes' and result == 'yes') or \
                      (side == 'no'  and result == 'no')

                if won:
                    gross_profit = (count * (1 - price_cents/100) if side == 'yes'
                                    else count * (price_cents/100))
                    pnl = gross_profit * 0.93 - cost
                else:
                    pnl = -cost

                trade['pnl']    = round(pnl, 4)
                trade['result'] = result
                updated         = True
                # Sync to SQLite immediately
                try:
                    db.update_trade_pnl(trade['client_order_id'], round(pnl, 4), result)
                except Exception:
                    pass

        except Exception:
            pass

        summary.append(trade)

    if updated:
        with open(TRADE_LOG, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(trades[0].keys()))
            writer.writeheader()
            writer.writerows(trades)

    return [t for t in summary if t.get('dry_run') != 'True']

def print_pnl_summary(live_trades):
    print(f"\n{'='*55}")
    print("P&L SUMMARY — LIVE TRADES")
    print(f"{'='*55}")

    if not live_trades:
        print("  No live trades yet")
        return

    total_cost  = sum(float(t['cost']) for t in live_trades)
    settled     = [t for t in live_trades if t.get('pnl')]
    pending     = [t for t in live_trades if not t.get('pnl')]
    settled_pnl = sum(float(t['pnl']) for t in settled)
    wins        = sum(1 for t in settled if float(t.get('pnl', 0)) > 0)

    print(f"\n  {'Timestamp':<22} {'Ticker':<30} {'Side':<5} {'Cost':>6} {'P&L':>8} {'Status'}")
    print(f"  {'-'*90}")

    for t in live_trades:
        pnl_str = f"${float(t['pnl']):+.2f}" if t.get('pnl') else "pending"
        status  = ("✅ WIN"  if t.get('pnl') and float(t['pnl']) > 0 else
                   "❌ LOSS" if t.get('pnl') and float(t['pnl']) <= 0 else "⏳")
        print(f"  {t['timestamp']:<22} {t['ticker']:<30} {t['side']:<5} "
              f"${float(t['cost']):>5.2f} {pnl_str:>8}  {status}")

    print(f"\n  Total invested:  ${total_cost:.2f}")
    print(f"  Settled trades:  {len(settled)}")
    print(f"  Pending trades:  {len(pending)}")
    if settled:
        print(f"  Win rate:        {wins}/{len(settled)} ({100*wins/len(settled):.0f}%)")
        print(f"  Realized P&L:    ${settled_pnl:+.2f}")
        roi = 100 * settled_pnl / total_cost if total_cost else 0
        print(f"  ROI:             {roi:+.1f}%")

# ── Data collection ───────────────────────────────────────────────────────

def collect_data(buckets, target_date, nws_high):
    existing = set()
    if DATA_FILE.exists():
        with open(DATA_FILE, 'r') as f:
            for row in csv.DictReader(f):
                existing.add((row['target_date'], row['ticker']))

    now  = datetime.datetime.now(datetime.timezone.utc)
    rows = []
    for m in buckets:
        floor = m.get("floor_strike")
        cap   = m.get("cap_strike")
        if not floor or not cap:
            continue
        ticker = m["ticker"]
        if (target_date, ticker) in existing:
            continue
        price = get_mid(m)
        rows.append({
            "collected_at":      now.strftime("%Y-%m-%d %H:%M UTC"),
            "target_date":       target_date,
            "ticker":            ticker,
            "floor":             float(floor),
            "cap":               float(cap),
            "price_mid":         price,
            "nws_forecast_high": nws_high,
            "actual_high":       "",
            "result":            ""
        })

    if rows:
        file_exists = DATA_FILE.exists()
        with open(DATA_FILE, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)
        print(f"  Saved {len(rows)} new observations")
    else:
        print("  Already collected today")

# ── Signal generation ─────────────────────────────────────────────────────

def generate_signals(range_buckets, thresh_above, thresh_below,
                     post_mean, post_std, edge_buffer=0.03):
    signals = []

    for b in sorted(range_buckets, key=lambda x: x["floor"]):
        mkt_prob  = b["raw_prob"]
        post_prob = bucket_prob(b["floor"], b["cap"], post_mean, post_std)
        be_yes    = breakeven_yes(mkt_prob)
        be_no     = breakeven_no(mkt_prob)
        label     = f"{b['floor']:.0f}-{b['cap']:.0f}°"

        if post_prob > be_yes + edge_buffer:
            signals.append(("YES", label, b["ticker"], mkt_prob, post_prob,
                            post_prob - be_yes))
        elif (1 - post_prob) > be_no + edge_buffer:
            signals.append(("NO", label, b["ticker"], mkt_prob, post_prob,
                            (1-post_prob) - be_no))

    for thresh, is_above in [(thresh_above, True), (thresh_below, False)]:
        if not thresh:
            continue
        mkt_prob = thresh["raw_prob"]
        if is_above:
            post_prob = above_prob(thresh["floor"], post_mean, post_std)
            label     = f">{thresh['floor']:.0f}°"
        else:
            post_prob = below_prob(thresh["cap"], post_mean, post_std)
            label     = f"<{thresh['cap']:.0f}°"

        be_yes = breakeven_yes(mkt_prob)
        be_no  = breakeven_no(mkt_prob)

        if post_prob > be_yes + edge_buffer:
            signals.append(("YES", label, thresh["ticker"], mkt_prob, post_prob,
                            post_prob - be_yes))
        elif (1 - post_prob) > be_no + edge_buffer:
            signals.append(("NO", label, thresh["ticker"], mkt_prob, post_prob,
                            (1-post_prob) - be_no))

    return signals

# ── Order execution ───────────────────────────────────────────────────────

def place_order(ticker, side, count, price_cents, dry_run=True):
    import uuid
    client_order_id = str(uuid.uuid4())
    cost = count * (price_cents / 100)

    print(f"\n  {'[DRY RUN] ' if dry_run else ''}Order:")
    print(f"    Ticker: {ticker}")
    print(f"    Side:   {side.upper()} BUY")
    print(f"    Count:  {count} contracts @ {price_cents}¢")
    print(f"    Cost:   ${cost:.2f}")

    if dry_run:
        print(f"    Status: SIMULATED")
        log_trade(ticker, side, count, price_cents, cost,
                  "simulated", client_order_id, dry_run=True)
        return True

    ORDER_URL  = "https://external-api.kalshi.com/trade-api/v2/portfolio/events/orders"
    ORDER_PATH = "/trade-api/v2/portfolio/events/orders"
    api_side   = "bid" if side == "yes" else "ask"
    price_str  = f"{price_cents / 100:.4f}"

    order_data = {
        "ticker":                     ticker,
        "side":                       api_side,
        "count":                      count,
        "price":                      price_str,
        "time_in_force":              "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
        "post_only":                  False,
        "cancel_order_on_pause":      False,
        "reduce_only":                False,
        "client_order_id":            client_order_id
    }
    r = requests.post(ORDER_URL,
                      headers=make_headers('POST', ORDER_PATH),
                      json=order_data, timeout=10)

    if r.status_code == 201:
        order    = r.json().get('order', {})
        order_id = order.get('order_id', '')
        print(f"    Status: FILLED ✅  order_id={order_id}")
        log_trade(ticker, side, count, price_cents, cost,
                  order_id, client_order_id, dry_run=False)
        return True
    else:
        print(f"    Status: FAILED ❌  {r.status_code} {r.text[:100]}")
        print(f"    Full response: {r.text}")
        return False

def log_trade(ticker, side, count, price_cents, cost,
              order_id, client_order_id, dry_run):
    ts         = datetime.datetime.now().strftime('%Y-%m-%d %H:%M UTC')
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
            'timestamp':       ts,
            'ticker':          ticker,
            'side':            side,
            'action':          'buy',
            'count':           count,
            'price_cents':     price_cents,
            'cost':            cost,
            'order_id':        order_id,
            'client_order_id': client_order_id,
            'dry_run':         dry_run,
            'pnl':             '',
            'result':          ''
        })
    # Mirror every trade to SQLite so the dashboard stays in sync
    try:
        db.insert_trade({
            'timestamp':       ts,
            'ticker':          ticker,
            'side':            side,
            'action':          'buy',
            'count':           count,
            'price_cents':     price_cents,
            'cost':            cost,
            'order_id':        order_id,
            'client_order_id': client_order_id,
            'dry_run':         1 if dry_run else 0,
            'pnl':             None,
            'result':          '',
        })
    except Exception:
        pass

# ── MAIN ──────────────────────────────────────────────────────────────────

def main():
    now = datetime.datetime.now(datetime.timezone.utc)
    print("=" * 60)
    print("KALSHI DAILY RUNNER")
    print(f"Time: {now.strftime('%Y-%m-%d %H:%M UTC')} "
          f"({now.strftime('%I:%M %p')} UTC)")
    print("=" * 60)

    # ── Step 0: Network check ─────────────────────────────────────────────
    print("\n🌐 Step 0: Checking network...")
    if not wait_for_network():
        print("  ⚠️  DNS unavailable after 60 s — aborting run")
        return
    print("  Network ready")

    # ── Step 0b: Sync CSV → SQLite ────────────────────────────────────────
    print("\n🔄 Syncing CSV trades to SQLite...")
    synced = sync_csv_to_db()
    print(f"  {synced} rows processed (new inserts use INSERT OR IGNORE)")

    # ── Step 1: P&L Update ────────────────────────────────────────────────
    print("\n📊 Step 1: Updating P&L...")
    live_trades = update_pnl()
    print_pnl_summary(live_trades)

    # ── Step 2: Get balance ───────────────────────────────────────────────
    balance = None
    for attempt in range(3):
        try:
            balance = auth_get('/portfolio/balance').json().get('balance', 0) / 100
            break
        except Exception as e:
            print(f"  Balance fetch failed (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(10)
    if balance is None:
        print("  ⚠️  Could not reach Kalshi API after 3 attempts — aborting")
        return
    print(f"\n💰 Current balance: ${balance:.2f}")

    # ── Step 3: Collect today's data ──────────────────────────────────────
    print("\n📥 Step 2: Collecting market data...")
    buckets, target_date, event_ticker = get_open_buckets()

    if not buckets:
        print("  No open markets found")
        return

    print(f"  Target date: {target_date}")
    nws_high, nws_sigma = get_nws_forecast(target_date)
    print(f"  NWS forecast: {nws_high}°F")
    collect_data(buckets, target_date, nws_high)

    # ── Step 4: Fair value model ──────────────────────────────────────────
    print("\n🧮 Step 3: Running fair value model...")
    range_buckets, thresh_above, thresh_below = extract_distribution(buckets)

    max_price = max(b["raw_prob"] for b in range_buckets) if range_buckets else 0
    if max_price < 0.05 or max_price > 0.90:
        print("  ⚠️  Market near resolution — no signals today")
        print("  Run before noon ET for actionable signals")
        return

    market_mean, market_std = market_implied_stats(
        range_buckets, thresh_above, thresh_below)

    # Guard against NWS outage — fall back to market-only distribution
    if nws_high is None:
        post_mean, post_std = market_mean, market_std
        divergence = 0.0
        print("  NWS unavailable — using market distribution only")
    else:
        post_mean, post_std = bayesian_update(
            market_mean, market_std, nws_high, nws_sigma)
        divergence = nws_high - market_mean

    print(f"  Market implied: {market_mean:.1f}°F ± {market_std:.1f}°F")
    if nws_high is not None:
        print(f"  NWS forecast:   {nws_high}°F")
    print(f"  Posterior:      {post_mean:.1f}°F ± {post_std:.1f}°F")
    print(f"  Divergence:     {divergence:+.1f}°F")

    # ── Step 5: Generate signals ──────────────────────────────────────────
    print("\n⚡ Step 4: Signal detection...")
    signals = generate_signals(range_buckets, thresh_above, thresh_below,
                               post_mean, post_std)

    if not signals:
        print("  No trades recommended today")
        return

    print(f"\n  {'Direction':<6} {'Bucket':<10} {'Mkt%':>7} {'FV%':>7} {'Edge':>7}")
    print(f"  {'-'*45}")
    for direction, label, ticker, mkt, fv, edge in signals:
        print(f"  {direction:<6} {label:<10} {mkt*100:>6.1f}% "
              f"{fv*100:>6.1f}% {edge*100:>+6.1f}%")

    # ── Step 6: Execute top signal ────────────────────────────────────────
    print(f"\n🚀 Step 5: Trade execution...")

    best = max(signals, key=lambda x: x[5])
    direction, label, ticker, mkt_price, fv, edge = best

    max_risk   = balance * 0.10
    cost_per   = mkt_price if direction == "YES" else (1 - mkt_price)
    contracts  = max(1, min(10, int(max_risk / cost_per)))
    price_cents = (int(mkt_price * 100) if direction == "YES"
                   else int((1 - mkt_price) * 100))

    print(f"  Best signal: {direction} on {label}")
    print(f"  Edge: {edge*100:.1f}%  |  Max risk: ${max_risk:.2f}")

    # Use SQLite as the authoritative duplicate check
    if db.has_open_position(ticker):
        print(f"  Already have an open position on {ticker} — skipping")
    elif edge > 0.15 and abs(divergence) > 3:
        place_order(ticker, direction.lower(), contracts,
                    price_cents, dry_run=False)
    elif edge > 0.05:
        print(f"  Edge {edge*100:.1f}% below 15% threshold — paper trading")
        place_order(ticker, direction.lower(), contracts,
                    price_cents, dry_run=True)
    else:
        print("  Insufficient edge — no trade")

    print(f"\n{'='*60}")
    print("Done! Check back tomorrow morning.")
    print(f"{'='*60}")

main()
