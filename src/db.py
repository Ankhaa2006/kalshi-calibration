"""
Central SQLite helper. All reads/writes go through here.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path('/Users/ankhbayarbatkhurel/kalshi-calibration/data/kalshi.db')


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS observations (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                collected_at  TEXT    NOT NULL,
                target_date   TEXT    NOT NULL,
                ticker        TEXT    NOT NULL,
                floor         REAL,
                cap           REAL,
                price_mid     REAL,
                nws_forecast_high REAL,
                actual_high   REAL,
                result        TEXT,
                UNIQUE(target_date, ticker)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp        TEXT    NOT NULL,
                ticker           TEXT    NOT NULL,
                side             TEXT,
                action           TEXT,
                count            INTEGER,
                price_cents      INTEGER,
                cost             REAL,
                order_id         TEXT,
                client_order_id  TEXT    UNIQUE,
                dry_run          INTEGER DEFAULT 0,
                pnl              REAL,
                result           TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_obs_date   ON observations(target_date);
            CREATE INDEX IF NOT EXISTS idx_trade_tick ON trades(ticker);
        """)


def insert_observation(row: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO observations
              (collected_at, target_date, ticker, floor, cap,
               price_mid, nws_forecast_high, actual_high, result)
            VALUES (:collected_at, :target_date, :ticker, :floor, :cap,
                    :price_mid, :nws_forecast_high, :actual_high, :result)
        """, row)


def update_observation_result(ticker: str, actual_high: float, result: str):
    with get_conn() as conn:
        conn.execute("""
            UPDATE observations
               SET actual_high = ?, result = ?
             WHERE ticker = ? AND (result IS NULL OR result = '')
        """, (actual_high, result, ticker))


def insert_trade(row: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO trades
              (timestamp, ticker, side, action, count, price_cents,
               cost, order_id, client_order_id, dry_run, pnl, result)
            VALUES (:timestamp, :ticker, :side, :action, :count, :price_cents,
                    :cost, :order_id, :client_order_id, :dry_run, :pnl, :result)
        """, row)


def update_trade_pnl(client_order_id: str, pnl: float, result: str):
    with get_conn() as conn:
        conn.execute("""
            UPDATE trades SET pnl = ?, result = ?
             WHERE client_order_id = ?
        """, (pnl, result, client_order_id))


def has_open_position(ticker: str) -> bool:
    """True if there is already a live (non-dry-run, unsettled) trade on this ticker."""
    with get_conn() as conn:
        row = conn.execute("""
            SELECT COUNT(*) FROM trades
             WHERE ticker = ? AND dry_run = 0
               AND (pnl IS NULL AND (result IS NULL OR result = ''))
        """, (ticker,)).fetchone()
        return row[0] > 0


def get_all_observations() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM observations ORDER BY target_date, ticker"
        ).fetchall()
        return [dict(r) for r in rows]


def get_all_trades() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY timestamp"
        ).fetchall()
        return [dict(r) for r in rows]


def get_live_trades() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM trades WHERE dry_run = 0 ORDER BY timestamp"
        ).fetchall()
        return [dict(r) for r in rows]


def get_unsettled_live_trades() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM trades
             WHERE dry_run = 0
               AND (pnl IS NULL AND (result IS NULL OR result = ''))
             ORDER BY timestamp
        """).fetchall()
        return [dict(r) for r in rows]


def init_backtest_tables():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS backtest_prices (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                event_ticker    TEXT    NOT NULL,
                market_ticker   TEXT    NOT NULL,
                target_date     TEXT    NOT NULL,
                floor           REAL,
                cap             REAL,
                is_threshold    INTEGER DEFAULT 0,
                threshold_type  TEXT,
                result          TEXT,
                price_13utc     REAL,
                price_14utc     REAL,
                price_15utc     REAL,
                fetched_at      TEXT,
                UNIQUE(market_ticker)
            );

            CREATE TABLE IF NOT EXISTS backtest_runs (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at    TEXT    NOT NULL,
                n_events  INTEGER,
                params    TEXT,
                metrics   TEXT,
                equity    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_bp_event ON backtest_prices(event_ticker);
        """)


init_db()
init_backtest_tables()
