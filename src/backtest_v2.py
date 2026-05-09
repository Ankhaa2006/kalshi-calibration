"""
backtest_v2.py — Full backtesting engine with parameter sweep and walk-forward validation.

Usage:
  python3 src/backtest_v2.py            # fetch prices + full sweep (first run ~15 min)
  python3 src/backtest_v2.py --cached   # use cached prices, rerun sweep only
"""
import sys
import os
import time
import json
import argparse
import requests
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path
from scipy import stats
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
import db

load_dotenv(dotenv_path=Path('/Users/ankhbayarbatkhurel/kalshi-calibration/.env'))
API_KEY  = os.getenv('KALSHI_API_KEY')
BASE_URL = 'https://api.elections.kalshi.com/trade-api/v2'
HEADERS  = {'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json'}

# ── Config ────────────────────────────────────────────────────────────────

ENTRY_HOURS_UTC = [13, 14, 15]       # 8am, 9am, 10am EDT
EDGE_BUFFERS    = [0.02, 0.03, 0.05, 0.08, 0.10]
KELLY_FRACTIONS = [0.25, 0.50, 0.75, 1.0]
STARTING_BALANCE = 100.0
MAX_OFFSET_SEC   = 5400              # ±90 min window for candlestick match
SLEEP_BETWEEN_CALLS = 0.25

# ── API helpers ───────────────────────────────────────────────────────────

def api_get(path, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(BASE_URL + path, headers=HEADERS,
                             params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
        except Exception:
            time.sleep(2 ** attempt)
    return {}


def get_candle_price(series_ticker, market_ticker, trading_day: datetime, hour_utc: int):
    """
    Return mid price at `hour_utc` UTC on `trading_day`.
    Returns None if no candle is within MAX_OFFSET_SEC of target.
    """
    start_ts  = int(trading_day.timestamp())
    end_ts    = start_ts + 86400
    target_ts = start_ts + hour_utc * 3600

    data = api_get(
        f'/series/KXHIGHNY/markets/{market_ticker}/candlesticks',
        params={'start_ts': start_ts, 'end_ts': end_ts, 'period_interval': 60})
    candles = data.get('candlesticks', [])
    if not candles:
        return None

    best = min(candles, key=lambda c: abs(c['end_period_ts'] - target_ts))
    if abs(best['end_period_ts'] - target_ts) > MAX_OFFSET_SEC:
        return None

    bid = float(best['yes_bid']['close_dollars'])
    ask = float(best['yes_ask']['close_dollars'])

    if bid == 0 and ask == 0:
        return None
    if bid == 0:
        return ask    # one-sided quote
    if ask == 0:
        return bid
    return (bid + ask) / 2

# ── Phase 1: Fetch and cache all settled event prices ─────────────────────

def fetch_all_settled_events():
    """Return list of all settled KXHIGHNY events with their markets."""
    markets, cursor = [], None
    while True:
        params = {'series_ticker': 'KXHIGHNY', 'status': 'settled', 'limit': 100}
        if cursor:
            params['cursor'] = cursor
        data    = api_get('/markets', params=params)
        batch   = data.get('markets', [])
        markets.extend(batch)
        cursor  = data.get('cursor')
        if not cursor or not batch:
            break
        time.sleep(0.1)

    # Group by event
    events: dict = {}
    for m in markets:
        e = m.get('event_ticker', '')
        events.setdefault(e, []).append(m)
    return events


def fetch_and_cache_prices(events: dict):
    """Fetch intraday candlestick prices for all markets, store in SQLite."""
    already_cached = set()
    with db.get_conn() as conn:
        rows = conn.execute('SELECT market_ticker FROM backtest_prices').fetchall()
        already_cached = {r[0] for r in rows}

    total_events = len(events)
    fetched = 0

    for i, (event_ticker, markets) in enumerate(sorted(events.items()), 1):
        needs_fetch = [m for m in markets if m['ticker'] not in already_cached]
        if not needs_fetch:
            print(f'  [{i}/{total_events}] {event_ticker} — cached', flush=True)
            continue

        # Determine trading day (close_date - 1 day)
        close_date_str = markets[0].get('close_time', '')[:10]
        try:
            close_dt   = datetime.strptime(close_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            trading_dt = close_dt - timedelta(days=1)
            target_date = trading_dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

        print(f'  [{i}/{total_events}] {event_ticker} → trading day {target_date} '
              f'({len(needs_fetch)} markets)', flush=True)

        for m in needs_fetch:
            ticker = m['ticker']
            floor  = m.get('floor_strike')
            cap    = m.get('cap_strike')
            result = m.get('result', '')

            is_threshold   = 0
            threshold_type = None
            if floor and not cap:
                is_threshold, threshold_type = 1, 'above'
                floor = float(floor)
                cap   = None
            elif cap and not floor:
                is_threshold, threshold_type = 1, 'below'
                cap   = float(cap)
                floor = None
            else:
                floor = float(floor) if floor else None
                cap   = float(cap)   if cap   else None

            prices = {}
            for hour in ENTRY_HOURS_UTC:
                p = get_candle_price('KXHIGHNY', ticker, trading_dt, hour)
                prices[hour] = p
                time.sleep(SLEEP_BETWEEN_CALLS)

            with db.get_conn() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO backtest_prices
                      (event_ticker, market_ticker, target_date, floor, cap,
                       is_threshold, threshold_type, result,
                       price_13utc, price_14utc, price_15utc, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (event_ticker, ticker, target_date,
                      floor, cap, is_threshold, threshold_type, result,
                      prices.get(13), prices.get(14), prices.get(15),
                      datetime.now(timezone.utc).isoformat()))
            fetched += 1

    print(f'\n  Done. {fetched} new markets cached.\n')

# ── Signal logic (mirrors fair_value_v2.py exactly) ──────────────────────

def extract_distribution(price_row_list):
    """
    price_row_list: list of dicts with floor, cap, is_threshold, threshold_type, price
    Returns: range_buckets, thresh_above, thresh_below
    """
    range_buckets, thresh_above, thresh_below = [], None, None
    for r in price_row_list:
        p = r['price']
        if p is None or p <= 0:
            continue
        if not r['is_threshold']:
            if r['floor'] is not None and r['cap'] is not None:
                range_buckets.append({
                    'floor':    r['floor'],
                    'cap':      r['cap'],
                    'mid_temp': (r['floor'] + r['cap']) / 2,
                    'raw_prob': p,
                    'ticker':   r['market_ticker'],
                    'result':   r['result'],
                })
        elif r['threshold_type'] == 'above':
            thresh_above = {'floor': r['floor'], 'raw_prob': p,
                            'ticker': r['market_ticker'], 'result': r['result']}
        elif r['threshold_type'] == 'below':
            thresh_below = {'cap':  r['cap'],  'raw_prob': p,
                            'ticker': r['market_ticker'], 'result': r['result']}

    total = sum(b['raw_prob'] for b in range_buckets)
    if thresh_above: total += thresh_above['raw_prob']
    if thresh_below: total += thresh_below['raw_prob']
    if total == 0:
        return [], None, None

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
    if total == 0:
        return None, None
    mean = sum(t * p for t, p in pts) / total
    var  = sum(p * (t - mean) ** 2 for t, p in pts) / total
    return mean, var ** 0.5


def bucket_prob(floor, cap, mu, sigma):
    return stats.norm(mu, sigma).cdf(cap) - stats.norm(mu, sigma).cdf(floor)


def above_prob(threshold, mu, sigma):
    return 1 - stats.norm(mu, sigma).cdf(threshold)


def below_prob(threshold, mu, sigma):
    return stats.norm(mu, sigma).cdf(threshold)


def build_empirical_prior(past_rows: list) -> dict:
    """
    Historical win rate per (floor, cap) bucket from past settled range buckets.
    Walk-forward safe: caller must pass only prior events' rows.
    Requires ≥5 observations per bucket to include.
    """
    counts: dict = {}
    wins:   dict = {}
    for r in past_rows:
        if r.get('is_threshold') or r.get('result') not in ('yes', 'no'):
            continue
        key = (r['floor'], r['cap'])
        counts[key] = counts.get(key, 0) + 1
        if r['result'] == 'yes':
            wins[key] = wins.get(key, 0) + 1
    return {k: wins.get(k, 0) / counts[k] for k in counts if counts[k] >= 5}


def get_momentum(event_rows: list, ticker: str, entry_hour: int) -> float | None:
    """
    Price drift from earliest available prior hour to entry_hour for `ticker`.
    Positive = price rising (market getting more bullish on YES).
    Returns None when no earlier price column exists (entry at 13UTC).
    """
    row = next((r for r in event_rows if r['market_ticker'] == ticker), None)
    if row is None:
        return None
    prices = {13: row.get('price_13utc'), 14: row.get('price_14utc'),
              15: row.get('price_15utc')}
    p_now = prices.get(entry_hour)
    if p_now is None:
        return None
    for h in sorted(prices):
        if h < entry_hour and prices[h] is not None:
            return p_now - prices[h]
    return None


def breakeven_yes(price, fee=0.07):
    return price / ((1 - price) * (1 - fee) + price)


def breakeven_no(price, fee=0.07):
    return (1 - price) / (price * (1 - fee) + (1 - price))


def kelly_size(direction, post_prob, mkt_price, balance, kelly_frac,
               fee=0.07, max_pct=0.15):
    if direction == 'YES':
        b = (1 - mkt_price) * (1 - fee) / mkt_price
        p = post_prob
    else:
        no_price = 1 - mkt_price
        b = mkt_price * (1 - fee) / no_price
        p = 1 - post_prob
    q         = 1 - p
    raw_kelly = (p * b - q) / b if b > 0 else 0
    fraction  = max(0.0, raw_kelly * kelly_frac)
    fraction  = min(fraction, max_pct)
    cost_per  = mkt_price if direction == 'YES' else (1 - mkt_price)
    max_risk  = balance * fraction
    contracts = max(1, int(max_risk / cost_per)) if cost_per > 0 else 1
    return contracts


def generate_signals(range_buckets, thresh_above, thresh_below, mu, sigma, edge_buffer,
                     emp_prior=None, entry_hour=None, event_rows=None):
    """
    Three-layer signal:
      1. Gaussian posterior (market-implied distribution smoothed)
      2. Empirical prior   (historical bucket win rates — walk-forward safe)
      3. Momentum filter   (price drift must not contradict direction)

    Composite fair value blends gaussian (60%) + empirical (40%) when prior exists.
    Empirical and momentum must not contradict the direction to fire a signal.
    """
    signals = []
    for b in sorted(range_buckets, key=lambda x: x['floor']):
        mkt      = b['raw_prob']
        pp_gauss = bucket_prob(b['floor'], b['cap'], mu, sigma)
        emp_rate = emp_prior.get((b['floor'], b['cap'])) if emp_prior else None
        momentum = get_momentum(event_rows, b['ticker'], entry_hour) \
                   if event_rows and entry_hour else None

        # Composite fair value: empirical shifts gaussian by at most ±0.15
        # (prevents late-day blowup when market has converged but base rate is low)
        if emp_rate is not None:
            adj = max(-0.15, min(0.15, emp_rate - pp_gauss))
            pp  = pp_gauss + 0.40 * adj
        else:
            pp = pp_gauss

        bey = breakeven_yes(mkt)
        ben = breakeven_no(mkt)

        if pp > bey + edge_buffer:
            # Empirical must agree (or be unavailable); momentum must not contradict
            emp_ok = emp_rate is None or emp_rate >= mkt
            mom_ok = momentum is None or momentum > -0.02
            if emp_ok and mom_ok:
                signals.append(('YES', b['ticker'], mkt, pp, pp - bey, b['result']))
        elif (1 - pp) > ben + edge_buffer:
            emp_ok = emp_rate is None or emp_rate <= mkt
            mom_ok = momentum is None or momentum < 0.02
            if emp_ok and mom_ok:
                signals.append(('NO',  b['ticker'], mkt, pp, (1-pp) - ben, b['result']))

    for thresh, is_above in [(thresh_above, True), (thresh_below, False)]:
        if not thresh:
            continue
        mkt = thresh['raw_prob']
        pp  = above_prob(thresh['floor'], mu, sigma) if is_above \
              else below_prob(thresh['cap'], mu, sigma)
        # Threshold buckets: gaussian only (too few historical observations for prior)
        bey = breakeven_yes(mkt)
        ben = breakeven_no(mkt)
        if pp > bey + edge_buffer:
            signals.append(('YES', thresh['ticker'], mkt, pp, pp - bey, thresh['result']))
        elif (1 - pp) > ben + edge_buffer:
            signals.append(('NO',  thresh['ticker'], mkt, pp, (1-pp) - ben, thresh['result']))

    return sorted(signals, key=lambda s: s[4], reverse=True)

# ── Phase 2: Simulation ───────────────────────────────────────────────────

def simulate_event(event_rows, entry_hour, edge_buffer, kelly_frac, balance,
                   emp_prior=None, fee=0.07):
    """
    Simulate one event. Returns a list of trade dicts (may be empty).
    `event_rows` = list of backtest_prices rows for one event.
    `emp_prior`  = {(floor, cap): historical_win_rate} from prior events only.
    """
    price_col = {13: 'price_13utc', 14: 'price_14utc', 15: 'price_15utc'}[entry_hour]

    # Build price_row_list with the chosen entry price
    price_rows = []
    for r in event_rows:
        p = r.get(price_col)
        price_rows.append({**r, 'price': p})

    range_buckets, thresh_above, thresh_below = extract_distribution(price_rows)
    if len(range_buckets) < 2:
        return []

    mu, sigma = market_implied_stats(range_buckets, thresh_above, thresh_below)
    if mu is None:
        return []

    # Stale market guard: skip if market has basically resolved
    max_raw = max(b['raw_prob'] for b in range_buckets)
    if max_raw < 0.05 or max_raw > 0.90:
        return []

    signals = generate_signals(range_buckets, thresh_above, thresh_below,
                               mu, sigma, edge_buffer,
                               emp_prior=emp_prior,
                               entry_hour=entry_hour,
                               event_rows=event_rows)
    if not signals:
        return []

    trades = []
    # Take up to 2 best signals (by edge), dedup by ticker
    seen_tickers = set()
    for direction, ticker, mkt_price, post_prob, edge, result in signals[:2]:
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)

        contracts = kelly_size(direction, post_prob, mkt_price, balance, kelly_frac)
        cost_per  = mkt_price if direction == 'YES' else (1 - mkt_price)
        cost      = contracts * cost_per

        # Don't bet more than we have
        if cost > balance:
            contracts = max(1, int(balance / cost_per))
            cost      = contracts * cost_per

        # Outcome: pnl = net change in balance (cost already deducted on entry)
        # Win YES: receive $1, pay 7% fee on profit → net = (1-p)*(1-fee)
        # Win NO:  receive $1, pay 7% fee on profit → net = p*(1-fee)
        if direction == 'YES':
            won = result == 'yes'
            pnl = contracts * (1 - mkt_price) * (1 - fee) if won else -cost
        else:
            won = result == 'no'
            pnl = contracts * mkt_price * (1 - fee) if won else -cost

        trades.append({
            'event_ticker': event_rows[0]['event_ticker'],
            'target_date':  event_rows[0]['target_date'],
            'ticker':       ticker,
            'direction':    direction,
            'mkt_price':    round(mkt_price, 4),
            'post_prob':    round(post_prob, 4),
            'edge':         round(edge, 4),
            'contracts':    contracts,
            'cost':         round(cost, 4),
            'won':          won,
            'pnl':          round(pnl, 4),
            'result':       result,
        })

    return trades


def simulate_all(events_by_ticker, entry_hour, edge_buffer, kelly_frac):
    """
    Simulate across all events, tracking running balance.
    Builds a rolling empirical prior from past settled events (walk-forward safe).
    Returns list of trade dicts with cumulative balance appended.
    """
    balance         = STARTING_BALANCE
    all_trades      = []
    settled_history = []   # grows with each completed event; never includes current

    # Sort chronologically by target_date, not alphabetically by event_ticker
    sorted_events = sorted(events_by_ticker.keys(),
                           key=lambda e: events_by_ticker[e][0]['target_date'])
    for event_ticker in sorted_events:
        rows      = events_by_ticker[event_ticker]
        emp_prior = build_empirical_prior(settled_history)
        trades    = simulate_event(rows, entry_hour, edge_buffer, kelly_frac,
                                   balance, emp_prior)
        # Extend history AFTER simulation to prevent look-ahead bias
        settled_history.extend(rows)

        for t in trades:
            balance += t['pnl']
            all_trades.append({**t, 'balance_after': round(balance, 4)})

    return all_trades

# ── Phase 3: Metrics ──────────────────────────────────────────────────────

def compute_metrics(trades: list, all_dates: list) -> dict:
    """
    all_dates: sorted list of all event target_dates (for daily Sharpe with zeros).
    """
    if not trades:
        return {
            'n_trades': 0, 'win_rate': 0, 'total_pnl': 0,
            'sharpe_daily': 0, 'sharpe_per_trade': 0,
            'max_drawdown': 0, 'profit_factor': 0,
            'final_balance': STARTING_BALANCE,
        }

    # Per-day P&L (include zero days for Sharpe)
    pnl_by_date: dict = {}
    for t in trades:
        d = t['target_date']
        pnl_by_date[d] = pnl_by_date.get(d, 0) + t['pnl']

    # Daily returns (0 on non-trading days)
    daily_pnl = []
    running   = STARTING_BALANCE
    for d in all_dates:
        daily_pnl.append(pnl_by_date.get(d, 0.0))

    daily_returns = [p / STARTING_BALANCE for p in daily_pnl]

    # Sharpe (annualized from daily returns)
    arr = np.array(daily_returns)
    sharpe_daily = (arr.mean() / arr.std() * np.sqrt(252)) if arr.std() > 0 else 0

    # Per-trade Sharpe (cleaner for sparse strategies)
    trade_returns = np.array([t['pnl'] / STARTING_BALANCE for t in trades])
    n_per_year    = min(252, len(all_dates))
    sharpe_trade  = (trade_returns.mean() / trade_returns.std() * np.sqrt(n_per_year)) \
                    if trade_returns.std() > 0 else 0

    # Max drawdown (on balance, using cumulative trades)
    peak = STARTING_BALANCE
    max_dd = 0.0
    bal = STARTING_BALANCE
    for t in trades:
        bal += t['pnl']
        if bal > peak:
            peak = bal
        dd = (peak - bal) / peak
        if dd > max_dd:
            max_dd = dd

    # Profit factor
    gross_profit = sum(t['pnl'] for t in trades if t['pnl'] > 0)
    gross_loss   = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
    pf = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')

    wins     = sum(1 for t in trades if t['won'])
    win_rate = wins / len(trades)

    return {
        'n_trades':       len(trades),
        'win_rate':       round(win_rate, 4),
        'total_pnl':      round(sum(t['pnl'] for t in trades), 4),
        'sharpe_daily':   round(float(sharpe_daily), 3),
        'sharpe_per_trade': round(float(sharpe_trade), 3),
        'max_drawdown':   round(float(max_dd), 4),
        'profit_factor':  round(float(pf), 3) if pf != float('inf') else 999,
        'final_balance':  round(STARTING_BALANCE + sum(t['pnl'] for t in trades), 4),
    }


def build_equity_curve(trades: list, all_dates: list) -> list:
    """Returns [{date, balance}] for Chart.js."""
    pnl_by_date: dict = {}
    for t in trades:
        d = t['target_date']
        pnl_by_date[d] = pnl_by_date.get(d, 0) + t['pnl']

    curve  = []
    bal    = STARTING_BALANCE
    for d in all_dates:
        bal += pnl_by_date.get(d, 0)
        curve.append({'date': d, 'balance': round(bal, 4)})
    return curve

# ── Phase 4: Walk-forward ─────────────────────────────────────────────────

def walk_forward(events_by_ticker: dict, best_params: dict) -> dict:
    # Sort chronologically
    dates    = sorted(events_by_ticker.keys(),
                      key=lambda e: events_by_ticker[e][0]['target_date'])
    split    = int(len(dates) * 0.65)
    train_ev = {e: events_by_ticker[e] for e in dates[:split]}
    test_ev  = {e: events_by_ticker[e] for e in dates[split:]}

    # Use target_dates (not event_tickers) for Sharpe daily-return series
    train_dates = sorted(set(r['target_date'] for rows in train_ev.values() for r in rows))
    test_dates  = sorted(set(r['target_date'] for rows in test_ev.values() for r in rows))

    h, eb, kf = best_params['hour'], best_params['edge_buffer'], best_params['kelly_fraction']

    train_trades = simulate_all(train_ev, h, eb, kf)
    test_trades  = simulate_all(test_ev,  h, eb, kf)

    return {
        'train_events': len(train_ev),
        'test_events':  len(test_ev),
        'split_date':   dates[split],
        'train':        compute_metrics(train_trades, train_dates),
        'test':         compute_metrics(test_trades,  test_dates),
        'train_curve':  build_equity_curve(train_trades, train_dates),
        'test_curve':   build_equity_curve(test_trades,  test_dates),
    }

# ── Main ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cached', action='store_true',
                        help='Skip price fetch, use cached data only')
    args = parser.parse_args()

    print('=' * 68)
    print('BACKTEST v2 — KXHIGHNY Composite Signal (Gaussian + Empirical + Momentum)')
    print(f'Run: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}')
    print('=' * 68)

    # ── Phase 1: Data ─────────────────────────────────────────────────────
    print('\n📥 Phase 1: Loading historical prices...')

    if not args.cached:
        print('  Fetching settled events from Kalshi...')
        events = fetch_all_settled_events()
        range_events = {e: m for e, m in events.items()
                        if any(x.get('floor_strike') for x in m)}
        print(f'  Found {len(range_events)} events with range buckets')
        print('  Fetching & caching candlestick prices...')
        fetch_and_cache_prices(range_events)
    else:
        print('  Using cached prices (--cached flag)')

    # Load from DB
    with db.get_conn() as conn:
        rows = conn.execute(
            'SELECT * FROM backtest_prices ORDER BY target_date, event_ticker'
        ).fetchall()
    all_rows = [dict(r) for r in rows]

    if not all_rows:
        print('  No cached data found. Run without --cached first.')
        return

    # Group by event
    events_by_ticker: dict = {}
    for r in all_rows:
        events_by_ticker.setdefault(r['event_ticker'], []).append(r)

    # Build chronological target_date list for Sharpe daily-return series
    all_target_dates = sorted(set(
        r['target_date'] for rows in events_by_ticker.values() for r in rows))
    n_events   = len(events_by_ticker)
    date_range = f'{all_target_dates[0]} → {all_target_dates[-1]}'

    print(f'\n  Events: {n_events}  |  Range: {date_range}')

    # Skip events with no results yet
    events_by_ticker = {
        e: rows for e, rows in events_by_ticker.items()
        if any(r['result'] in ('yes', 'no') for r in rows)
    }
    all_target_dates = sorted(set(
        r['target_date'] for rows in events_by_ticker.values() for r in rows))
    print(f'  Settled (usable): {len(events_by_ticker)} events')

    if len(events_by_ticker) < 10:
        print('  Not enough settled events for meaningful backtest (need ≥10)')
        return

    # ── Phase 2: Parameter sweep ──────────────────────────────────────────
    n_combos = len(ENTRY_HOURS_UTC) * len(EDGE_BUFFERS) * len(KELLY_FRACTIONS)
    print(f'\n⚡ Phase 2: Parameter sweep ({n_combos} combinations)...')

    sweep_results = []
    combo_n = 0

    for hour in ENTRY_HOURS_UTC:
        for edge_buffer in EDGE_BUFFERS:
            for kelly_frac in KELLY_FRACTIONS:
                combo_n += 1
                trades  = simulate_all(events_by_ticker, hour, edge_buffer, kelly_frac)
                metrics = compute_metrics(trades, all_target_dates)
                equity  = build_equity_curve(trades, all_target_dates)
                sweep_results.append({
                    'hour':          hour,
                    'edge_buffer':   edge_buffer,
                    'kelly_fraction': kelly_frac,
                    'metrics':       metrics,
                    'trades':        trades,
                    'equity':        equity,
                })
                print(f'  [{combo_n:>2}/{n_combos}] '
                      f'h={hour}:00 eb={edge_buffer:.2f} kf={kelly_frac:.2f}  '
                      f'trades={metrics["n_trades"]:>3}  '
                      f'win={metrics["win_rate"]*100:.0f}%  '
                      f'pnl=${metrics["total_pnl"]:>+7.2f}  '
                      f'sharpe={metrics["sharpe_daily"]:>+5.2f}  '
                      f'dd={metrics["max_drawdown"]*100:.1f}%', flush=True)

    # Sort by daily Sharpe
    sweep_results.sort(key=lambda x: x['metrics']['sharpe_daily'], reverse=True)

    # ── Print results table ───────────────────────────────────────────────
    print(f'\n{"="*68}')
    print('RESULTS — Sorted by Daily Sharpe Ratio')
    print(f'{"="*68}')
    print(f'  {"Hour":>5} {"Edge":>6} {"Kelly":>6} {"Trades":>7} {"Win%":>6} '
          f'{"P&L":>8} {"Sharpe":>7} {"MaxDD":>7} {"PFactor":>8}')
    print(f'  {"-"*66}')
    for r in sweep_results[:15]:
        m = r['metrics']
        print(f'  {r["hour"]:02d}:00  {r["edge_buffer"]:>5.2f}  '
              f'{r["kelly_fraction"]:>5.2f}  {m["n_trades"]:>6}  '
              f'{m["win_rate"]*100:>5.1f}%  '
              f'${m["total_pnl"]:>+7.2f}  {m["sharpe_daily"]:>+6.2f}  '
              f'{m["max_drawdown"]*100:>6.1f}%  {m["profit_factor"]:>7.2f}')

    # ── Phase 3: Walk-forward on top param set ────────────────────────────
    best = sweep_results[0]
    best_params = {
        'hour':          best['hour'],
        'edge_buffer':   best['edge_buffer'],
        'kelly_fraction': best['kelly_fraction'],
    }

    print(f'\n{"="*68}')
    print('WALK-FORWARD VALIDATION')
    print(f'{"="*68}')
    print(f'  Best params: hour={best_params["hour"]}:00 UTC  '
          f'edge={best_params["edge_buffer"]}  kelly={best_params["kelly_fraction"]}')

    wf = walk_forward(events_by_ticker, best_params)
    ti, te = wf['train'], wf['test']

    print(f'\n  Split at event {wf["split_date"]}')
    print(f'  Train: {wf["train_events"]} events')
    print(f'  Test:  {wf["test_events"]} events')
    print()
    print(f'  {"Metric":<22} {"In-sample":>12} {"Out-of-sample":>14}')
    print(f'  {"-"*50}')
    for label, tk, tsk in [
        ('Sharpe (daily)',    'sharpe_daily',   'sharpe_daily'),
        ('Sharpe (per-trade)','sharpe_per_trade','sharpe_per_trade'),
        ('Win rate',         'win_rate',        'win_rate'),
        ('Total P&L',        'total_pnl',       'total_pnl'),
        ('Max drawdown',     'max_drawdown',    'max_drawdown'),
        ('Profit factor',    'profit_factor',   'profit_factor'),
        ('Trades',           'n_trades',        'n_trades'),
    ]:
        iv = ti.get(tk, 0)
        ov = te.get(tk, 0)
        if tk in ('win_rate', 'max_drawdown'):
            print(f'  {label:<22} {iv*100:>11.1f}%  {ov*100:>13.1f}%')
        elif tk == 'total_pnl':
            print(f'  {label:<22} ${iv:>10.2f}   ${ov:>12.2f}')
        elif tk == 'n_trades':
            print(f'  {label:<22} {iv:>12}   {ov:>13}')
        else:
            print(f'  {label:<22} {iv:>12.3f}   {ov:>13.3f}')

    # Sharpe decay
    if ti['sharpe_daily'] != 0:
        decay = (ti['sharpe_daily'] - te['sharpe_daily']) / abs(ti['sharpe_daily'])
        print(f'\n  Sharpe decay (IS→OOS): {decay*100:+.1f}%')
        if decay < 0.25:
            verdict = '✅ Strong generalization (decay <25%)'
        elif decay < 0.50:
            verdict = '⚡ Moderate generalization (decay 25-50%)'
        else:
            verdict = '⚠️  Possible overfitting (decay >50%)'
        print(f'  Verdict: {verdict}')

    # ── Save top-5 results to DB ──────────────────────────────────────────
    now_str = datetime.now(timezone.utc).isoformat()
    with db.get_conn() as conn:
        conn.execute('DELETE FROM backtest_runs')  # replace previous run
        for r in sweep_results[:5]:
            conn.execute("""
                INSERT INTO backtest_runs (run_at, n_events, params, metrics, equity)
                VALUES (?, ?, ?, ?, ?)
            """, (
                now_str,
                len(events_by_ticker),
                json.dumps({'hour': r['hour'], 'edge_buffer': r['edge_buffer'],
                            'kelly_fraction': r['kelly_fraction']}),
                json.dumps(r['metrics']),
                json.dumps(r['equity']),
            ))

    # Also save walk-forward result as special row
    with db.get_conn() as conn:
        conn.execute("""
            INSERT INTO backtest_runs (run_at, n_events, params, metrics, equity)
            VALUES (?, ?, ?, ?, ?)
        """, (
            now_str, len(events_by_ticker),
            json.dumps({**best_params, 'type': 'walk_forward'}),
            json.dumps({'train': ti, 'test': te,
                        'train_events': wf['train_events'],
                        'test_events':  wf['test_events'],
                        'split_date':   wf['split_date']}),
            json.dumps({'train': wf['train_curve'], 'test': wf['test_curve']}),
        ))

    print(f'\n  Results saved to SQLite ({len(sweep_results)} param sets).')
    print(f'\n{"="*68}')
    print('RECOMMENDATION')
    print(f'{"="*68}')
    bm = best['metrics']
    print(f'  Optimal:  hour={best_params["hour"]}:00 UTC, '
          f'edge={best_params["edge_buffer"]}, kelly={best_params["kelly_fraction"]}')
    print(f'  Sharpe:   {bm["sharpe_daily"]:+.2f} (daily)  '
          f'{bm["sharpe_per_trade"]:+.2f} (per-trade)')
    print(f'  Win rate: {bm["win_rate"]*100:.1f}%  |  '
          f'P&L: ${bm["total_pnl"]:+.2f} on ${STARTING_BALANCE:.0f}  |  '
          f'Max DD: {bm["max_drawdown"]*100:.1f}%')
    print()
    if bm['sharpe_daily'] > 0.5:
        print('  Signal appears real. Deploy these params in live system.')
    elif bm['sharpe_daily'] > 0:
        print('  Weak edge. Collect more data before deploying.')
    else:
        print('  No detectable edge with current model. Revisit signal logic.')
    print(f'{"="*68}\n')


main()
