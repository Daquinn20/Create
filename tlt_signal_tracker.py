"""
TLT Signal Tracker — headless daily capture of TLT signal hits + forward returns.

Each run:
  1. Loads master_universe.csv tickers
  2. Runs TLTEngine.analyze_stock on each
  3. Persists every non-NEUTRAL signal (OVERSOLD, SPRING, SURGE, DANGER, LEADER)
     into the tlt_signals table (Neon Postgres if DATABASE_URL set, else SQLite)
  4. Updates fwd_30d / fwd_60d / fwd_90d returns for prior signals whose
     anchor dates have now passed

Designed to be invoked once per weekday after market close. The Streamlit
"Signal Performance History" tab reads from the same table.

CLI:
  python tlt_signal_tracker.py [--mode high_conviction|balanced]
                                [--limit N]      # cap symbols for testing
                                [--skip-scan]    # only update fwd returns
                                [--skip-fwd]     # only run today's scan
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

from tlt_engine_core import TLTEngine, fetch_history

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
USE_POSTGRES = DATABASE_URL is not None

if USE_POSTGRES:
    import psycopg2

TRACKED_TIERS = {"OVERSOLD", "SPRING", "SURGE", "DANGER", "LEADER"}
MAX_WORKERS = 15
SQLITE_PATH = "tlt_signals.db"

UNIVERSE_FILES = {
    "master": "master_universe.csv",
    "sp500": "SP500_list.xlsx",
}


# ============================================================================
# DATABASE
# ============================================================================

def get_connection():
    if USE_POSTGRES:
        return psycopg2.connect(DATABASE_URL)
    return sqlite3.connect(SQLITE_PATH)


def init_database() -> None:
    """Create tlt_signals table if missing, and migrate to include universe column."""
    conn = get_connection()
    cur = conn.cursor()

    if USE_POSTGRES:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tlt_signals (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                signal_date DATE NOT NULL,
                tier TEXT NOT NULL,
                mode TEXT NOT NULL,
                universe TEXT NOT NULL DEFAULT 'master',
                signal_price REAL,
                mfi REAL,
                rsi REAL,
                lr_ratio REAL,
                cmf REAL,
                mrs REAL,
                composite_score INTEGER,
                vs_ma50 REAL,
                vs_ma200 REAL,
                vs_52w_high REAL,
                rel_vol REAL,
                fwd_30d_return REAL,
                fwd_60d_return REAL,
                fwd_90d_return REAL,
                fwd_30d_price REAL,
                fwd_60d_price REAL,
                fwd_90d_price REAL,
                fwd_30d_captured_at DATE,
                fwd_60d_captured_at DATE,
                fwd_90d_captured_at DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, signal_date, tier, mode, universe)
            )
        """)
        # Migrate older tables that pre-date the universe column
        cur.execute("ALTER TABLE tlt_signals ADD COLUMN IF NOT EXISTS universe TEXT NOT NULL DEFAULT 'master'")
        cur.execute("ALTER TABLE tlt_signals DROP CONSTRAINT IF EXISTS tlt_signals_ticker_signal_date_tier_mode_key")
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'tlt_signals_universe_unique'
                ) THEN
                    ALTER TABLE tlt_signals
                        ADD CONSTRAINT tlt_signals_universe_unique
                        UNIQUE (ticker, signal_date, tier, mode, universe);
                END IF;
            END $$;
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tlt_signal_date ON tlt_signals(signal_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tlt_ticker ON tlt_signals(ticker)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tlt_tier ON tlt_signals(tier)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tlt_universe ON tlt_signals(universe)")
        print("Database initialized: Neon PostgreSQL")
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tlt_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                signal_date DATE NOT NULL,
                tier TEXT NOT NULL,
                mode TEXT NOT NULL,
                universe TEXT NOT NULL DEFAULT 'master',
                signal_price REAL,
                mfi REAL,
                rsi REAL,
                lr_ratio REAL,
                cmf REAL,
                mrs REAL,
                composite_score INTEGER,
                vs_ma50 REAL,
                vs_ma200 REAL,
                vs_52w_high REAL,
                rel_vol REAL,
                fwd_30d_return REAL,
                fwd_60d_return REAL,
                fwd_90d_return REAL,
                fwd_30d_price REAL,
                fwd_60d_price REAL,
                fwd_90d_price REAL,
                fwd_30d_captured_at DATE,
                fwd_60d_captured_at DATE,
                fwd_90d_captured_at DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, signal_date, tier, mode, universe)
            )
        """)
        # SQLite migration — best-effort ADD COLUMN if pre-existing without universe
        try:
            cur.execute("ALTER TABLE tlt_signals ADD COLUMN universe TEXT NOT NULL DEFAULT 'master'")
        except Exception:
            pass
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tlt_signal_date ON tlt_signals(signal_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tlt_ticker ON tlt_signals(ticker)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tlt_tier ON tlt_signals(tier)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tlt_universe ON tlt_signals(universe)")
        print(f"Database initialized: SQLite ({SQLITE_PATH})")

    conn.commit()
    conn.close()


def placeholder() -> str:
    return "%s" if USE_POSTGRES else "?"


# ============================================================================
# UNIVERSE
# ============================================================================

def load_universe(name: str = "master") -> List[str]:
    """Read universe file → list of clean uppercase tickers."""
    if name not in UNIVERSE_FILES:
        raise ValueError(f"Unknown universe '{name}'. Options: {list(UNIVERSE_FILES)}")
    path = Path(__file__).parent / UNIVERSE_FILES[name]

    if name == "master":
        df = pd.read_csv(path, header=None, names=["Ticker", "Name", "Exchange"])
        ticker_col = "Ticker"
    else:  # sp500
        df = pd.read_excel(path)
        ticker_col = "Symbol" if "Symbol" in df.columns else "Ticker"

    df = df[df[ticker_col].notna()]
    df[ticker_col] = df[ticker_col].astype(str).str.strip().str.upper()
    df = df[df[ticker_col] != ""]
    df = df[df[ticker_col] != "NAN"]
    return df[ticker_col].tolist()


# ============================================================================
# SCAN
# ============================================================================

def _scan_one(symbol: str, engine: TLTEngine, session: requests.Session) -> Optional[Dict]:
    df = fetch_history(symbol, "1y", session=session)
    if df is None or len(df) < 60:
        return None
    result = engine.analyze_stock(df)
    if result is None:
        return None
    tier = result.get("Signal_Tier")
    if tier not in TRACKED_TIERS:
        return None
    return {"symbol": symbol, "analysis": result}


def run_daily_scan(symbols: List[str], mode: str, signal_date: date, universe: str) -> int:
    """Run TLT scan and upsert hits into tlt_signals. Returns row count inserted."""
    print(f"\n[SCAN] Loading SPY benchmark...")
    spy = fetch_history("SPY", "2y")
    if spy is None or spy.empty:
        print("[SCAN] ERROR: could not load SPY benchmark — aborting scan")
        return 0

    engine = TLTEngine(benchmark_data=spy, mode=mode)
    session = requests.Session()

    print(f"[SCAN] Scanning {len(symbols)} symbols (mode={mode}, workers={MAX_WORKERS})...")
    hits: List[Dict] = []
    start = time.time()
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_scan_one, sym, engine, session): sym for sym in symbols}
        for fut in as_completed(futures):
            completed += 1
            try:
                result = fut.result()
                if result:
                    hits.append(result)
            except Exception:
                pass
            if completed % 250 == 0:
                elapsed = time.time() - start
                rate = completed / elapsed if elapsed > 0 else 0
                print(f"[SCAN] {completed}/{len(symbols)} — {len(hits)} hits — {rate:.1f}/s")

    print(f"[SCAN] Done: {len(hits)} signals from {completed} symbols in {time.time() - start:.1f}s")

    if not hits:
        return 0

    conn = get_connection()
    cur = conn.cursor()
    inserted = 0
    ph = placeholder()

    if USE_POSTGRES:
        insert_sql = f"""
            INSERT INTO tlt_signals
                (ticker, signal_date, tier, mode, universe, signal_price, mfi, rsi,
                 lr_ratio, cmf, mrs, composite_score, vs_ma50, vs_ma200, vs_52w_high, rel_vol)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT (ticker, signal_date, tier, mode, universe) DO NOTHING
        """
    else:
        insert_sql = f"""
            INSERT OR IGNORE INTO tlt_signals
                (ticker, signal_date, tier, mode, universe, signal_price, mfi, rsi,
                 lr_ratio, cmf, mrs, composite_score, vs_ma50, vs_ma200, vs_52w_high, rel_vol)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
        """

    def _f(x):
        return None if x is None or pd.isna(x) else float(x)

    def _i(x):
        return None if x is None or pd.isna(x) else int(x)

    for hit in hits:
        a = hit["analysis"]
        cur.execute(insert_sql, (
            hit["symbol"], signal_date, a["Signal_Tier"], mode, universe, _f(a["Price"]),
            _f(a["MFI"]), _f(a["RSI"]), _f(a["LR_Ratio"]), _f(a["CMF"]), _f(a["MRS"]),
            _i(a["Composite_Score"]), _f(a["vs_MA50"]), _f(a["vs_MA200"]),
            _f(a["vs_52wHigh"]), _f(a["RelVol"]),
        ))
        inserted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    conn.commit()
    conn.close()
    print(f"[SCAN] Inserted {inserted} new signal rows (duplicates skipped)")
    return len(hits)


# ============================================================================
# FORWARD RETURNS
# ============================================================================

def _pending_rows(window_days: int, today: date) -> List[Tuple]:
    """Rows whose fwd_{window}d_return is still NULL and signal_date is old enough."""
    cutoff = today - timedelta(days=window_days)
    col = f"fwd_{window_days}d_return"
    conn = get_connection()
    cur = conn.cursor()
    ph = placeholder()
    cur.execute(
        f"SELECT id, ticker, signal_date, signal_price FROM tlt_signals "
        f"WHERE {col} IS NULL AND signal_date <= {ph}",
        (cutoff,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def update_forward_returns(today: date) -> Dict[str, int]:
    """Fill fwd_30d / fwd_60d / fwd_90d for any rows now past their anchor date."""
    updated = {30: 0, 60: 0, 90: 0}
    session = requests.Session()

    # Pre-fetch each ticker once per session so 30/60/90 share the same price
    # series for that ticker rather than re-pulling.
    cache: Dict[str, Optional[pd.DataFrame]] = {}

    def history_for(sym: str) -> Optional[pd.DataFrame]:
        if sym not in cache:
            cache[sym] = fetch_history(sym, "1y", session=session)
        return cache[sym]

    for window in (30, 60, 90):
        rows = _pending_rows(window, today)
        if not rows:
            print(f"[FWD {window}d] no rows to update")
            continue
        print(f"[FWD {window}d] updating {len(rows)} rows...")
        conn = get_connection()
        cur = conn.cursor()
        ph = placeholder()
        col_ret = f"fwd_{window}d_return"
        col_price = f"fwd_{window}d_price"
        col_at = f"fwd_{window}d_captured_at"

        for row_id, ticker, sig_date_raw, signal_price in rows:
            sig_date = _to_date(sig_date_raw)
            anchor = sig_date + timedelta(days=window)
            df = history_for(ticker)
            if df is None or df.empty or signal_price in (None, 0):
                continue
            close_at = _close_on_or_after(df, anchor)
            if close_at is None:
                continue
            ret = (close_at - signal_price) / signal_price * 100
            cur.execute(
                f"UPDATE tlt_signals SET {col_ret}={ph}, {col_price}={ph}, {col_at}={ph} "
                f"WHERE id={ph}",
                (round(float(ret), 3), round(float(close_at), 4), today, row_id),
            )
            updated[window] += 1

        conn.commit()
        conn.close()
        print(f"[FWD {window}d] updated {updated[window]} rows")

    return updated


def _to_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _close_on_or_after(df: pd.DataFrame, target: date) -> Optional[float]:
    """First close at or after target trading date (handles weekends/holidays)."""
    ts = pd.Timestamp(target)
    idx = df.index
    if hasattr(idx, "tz") and idx.tz is not None:
        idx = idx.tz_localize(None)
        df = df.copy()
        df.index = idx
    on_or_after = df[df.index >= ts]
    if on_or_after.empty:
        return None
    return float(on_or_after["Close"].iloc[0])


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="TLT signal tracker — daily capture")
    parser.add_argument("--mode", choices=["high_conviction", "balanced"], default="high_conviction")
    parser.add_argument("--universe", choices=["master", "sp500", "both"], default="master",
                        help="Which universe to scan ('both' runs master then sp500)")
    parser.add_argument("--limit", type=int, default=None, help="Cap symbols for testing")
    parser.add_argument("--skip-scan", action="store_true", help="Only update forward returns")
    parser.add_argument("--skip-fwd", action="store_true", help="Only run today's scan")
    parser.add_argument("--date", type=str, default=None, help="Override signal_date YYYY-MM-DD")
    args = parser.parse_args()

    today = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    universes = ["master", "sp500"] if args.universe == "both" else [args.universe]

    print(f"=== TLT Signal Tracker ===")
    print(f"Date: {today}")
    print(f"Mode: {args.mode}")
    print(f"Universes: {universes}")
    print(f"DB: {'Neon Postgres' if USE_POSTGRES else f'SQLite ({SQLITE_PATH})'}")

    init_database()

    if not args.skip_scan:
        for uni in universes:
            symbols = load_universe(uni)
            if args.limit:
                symbols = symbols[: args.limit]
                print(f"[CLI] Limited {uni} to {len(symbols)} symbols")
            print(f"\n--- Scanning universe: {uni} ({len(symbols)} symbols) ---")
            run_daily_scan(symbols, args.mode, today, uni)

    if not args.skip_fwd:
        update_forward_returns(today)

    print("=== Done ===")


if __name__ == "__main__":
    sys.exit(main() or 0)
