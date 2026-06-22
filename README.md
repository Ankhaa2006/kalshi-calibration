# kalshi-calibration

Automated trading bot for Kalshi's `KXHIGHNY` market (daily NYC high temperature).
It compares the market-implied probability distribution against a Bayesian fair-value
model and places trades when the edge exceeds a fee-adjusted threshold.

## Project structure

```
src/
  run_daily.py        Main entry point. Runs the full daily cycle: P&L update,
                       balance check, data collection, signal generation, trade
                       execution. Invoked on a schedule via launchd.
  model.py             Fair value model. Computes the market-implied Gaussian,
                       blends in NWS forecast (only when it diverges >1σ) and an
                       empirical bucket prior, then prints trade recommendations.
                       Also invoked standalone by the dashboard for live signals.
  backtest_v2.py       Backtesting engine. Replays settled observations from
                       SQLite, simulates the market-only Gaussian signal with
                       half-Kelly sizing, and reports Sharpe/ROI/calibration plus
                       a 60/40 walk-forward split.
  daily_collector.py   Standalone collector: backfills settlement results for
                       past observations and saves today's open bucket prices.
  db.py                 Central SQLite helper — all reads/writes to data/kalshi.db
                       go through here (observations, trades, backtest tables).
  migrate_to_sqlite.py  One-time migration of the legacy CSV files into SQLite.
                       Safe to re-run.

dashboard/
  app.py               Flask app serving a live dashboard (balance, P&L,
                       calibration chart, accuracy tracking, current signal).
  templates/index.html  Dashboard UI.

data/                  CSVs, SQLite DB, logs, and cached results (gitignored
                       except for committed CSV snapshots).
kalshi_daily.plist     launchd job that runs run_daily.py at 10am/1pm/3pm ET.
```

## Running it

Set up environment variables in `.env` (Kalshi API key/key ID, NOAA token) and
place your Kalshi RSA private key at `kalshi_trading.key`.

**Daily collection + trading cycle:**
```
python3 src/run_daily.py
```
Checks network/DNS, syncs CSV trades into SQLite, updates P&L on settled
trades, fetches the current balance, collects today's market prices, runs the
fair value model, and executes the best signal (live or paper, depending on
edge size).

**Backtest:**
```
python3 src/backtest_v2.py
```
Reads settled observations from SQLite and writes a full report —
performance metrics, monthly ROI, calibration curve, and walk-forward
validation — to `data/backtest_results.json`.

**Dashboard:**
```
python3 dashboard/app.py
```
Serves the live dashboard at `http://localhost:5050`.

## Current status & findings

The fair value model (`model.py`) is a composite of three signals:
1. **Market Gaussian** — fitted to the current market-implied probability
   distribution across all open buckets.
2. **Conditional NWS blend** — the NWS hourly forecast is only blended in when
   it diverges more than 1σ from the market, scaling from 0% to 30% weight as
   divergence grows to 3σ+.
3. **Empirical bucket prior** — historical win rate per (floor, cap) bucket
   from settled observations (≥5 samples), shifting fair value by up to ±15%.

Backtesting (68 settled events, Mar–May 2026) showed the market-only Gaussian
signal alone is unprofitable (best Sharpe -0.43 across parameter sweep). Adding
the empirical prior and tuning entry time/edge threshold/Kelly fraction
produced a backtested Sharpe of **+1.01** (in-sample P&L +$55.69 on a $100
bankroll, out-of-sample +$6.03 — still positive but with significant decay,
meaning the edge is real but modest and noisy on this sample size).

The bot trades small ($10 base size) and conservatively: a 15%+ edge combined
with a 3°F+ market/NWS divergence triggers a live trade; smaller edges (5-15%)
are paper-traded only. Known limitation: sample size (tens of events) is too
small for high statistical confidence — treat current performance as
directional, not conclusive.
