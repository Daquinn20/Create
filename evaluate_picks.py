"""Nightly evaluator for matured industry-report picks.

For each (pick, horizon in [30, 90, 180]) where today >= run_date + horizon
and no evaluations row exists yet, fetch ticker + benchmark closes on the
eval date, compute returns / alpha / hit, and write the row.

Usage:
    python evaluate_picks.py                 # uses default DB and horizons
    python evaluate_picks.py --today 2026-08-15 --horizons 30,90
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
from datetime import date, datetime, timedelta
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import requests

from track_record import DEFAULT_DB_PATH, _connect, init_db

logger = logging.getLogger(__name__)

DEFAULT_HORIZONS = (30, 90, 180)
PRICE_LOOKBACK_DAYS = 7  # max days back to scan when eval_date falls on a non-trading day
FMP_BASE = "https://financialmodelingprep.com/api/v3"


# ---------- Price fetching ----------

PriceFetcher = Callable[[str, date, date], Dict[str, float]]


def fmp_historical_closes(symbol: str, start: date, end: date) -> Dict[str, float]:
    """Return {YYYY-MM-DD: close} for `symbol` over [start, end] inclusive."""
    api_key = os.environ.get("FMP_API_KEY")
    if not api_key:
        raise RuntimeError("FMP_API_KEY not set in environment")

    url = f"{FMP_BASE}/historical-price-full/{symbol}"
    params = {"from": start.isoformat(), "to": end.isoformat(), "apikey": api_key}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json() or {}
    out: Dict[str, float] = {}
    for row in data.get("historical", []) or []:
        d, c = row.get("date"), row.get("close")
        if d and c is not None:
            out[d] = float(c)
    return out


def _close_on_or_before(
    symbol: str,
    target: date,
    fetcher: PriceFetcher,
    lookback_days: int = PRICE_LOOKBACK_DAYS,
) -> Optional[Tuple[str, float]]:
    """Latest close on-or-before `target`, scanning back up to `lookback_days`.

    Handles weekends/holidays: FMP returns no row for non-trading days, so
    we take the most recent trading day in the window.
    """
    start = target - timedelta(days=lookback_days)
    try:
        closes = fetcher(symbol, start, target)
    except Exception as e:
        logger.warning(f"Price fetch failed for {symbol} @ {target}: {e}")
        return None
    if not closes:
        return None
    eligible = sorted(closes.items())  # YYYY-MM-DD sorts correctly as text
    return eligible[-1]


# ---------- Scoring ----------

def _compute_hit(direction: str, alpha: float) -> Optional[int]:
    """long -> hit if alpha > 0; short/avoid -> hit if alpha < 0; neutral -> None."""
    if direction == "long":
        return 1 if alpha > 0 else 0
    if direction in ("short", "avoid"):
        return 1 if alpha < 0 else 0
    return None


# ---------- Query for due picks ----------

def find_due_picks(
    today: date,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    db_path: str = DEFAULT_DB_PATH,
) -> List[Dict]:
    """Return picks whose horizon has matured and have no evaluations row yet."""
    today_str = today.isoformat()
    due: List[Dict] = []

    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for h in horizons:
            rows = conn.execute(
                """
                SELECT p.pick_id, p.report_id, p.ticker, p.agent, p.direction,
                       p.entry_price, p.benchmark_symbol, p.benchmark_entry_price,
                       r.run_date, ? AS horizon_days
                FROM picks p
                JOIN reports r ON r.report_id = p.report_id
                LEFT JOIN evaluations e
                    ON e.pick_id = p.pick_id AND e.horizon_days = ?
                WHERE e.pick_id IS NULL
                  AND DATE(r.run_date, '+' || ? || ' days') <= ?
                ORDER BY r.run_date, p.ticker
                """,
                (h, h, h, today_str),
            ).fetchall()
            due.extend(dict(r) for r in rows)
    return due


# ---------- Main evaluator ----------

def evaluate_pending(
    today: Optional[date] = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    db_path: str = DEFAULT_DB_PATH,
    price_fetcher: PriceFetcher = fmp_historical_closes,
) -> Dict[str, int]:
    """Score every matured but un-evaluated pick. Returns counts."""
    today = today or date.today()
    init_db(db_path)
    due = find_due_picks(today, horizons, db_path)
    logger.info(f"{len(due)} (pick, horizon) pairs due for evaluation.")

    price_cache: Dict[Tuple[str, str], Optional[Tuple[str, float]]] = {}

    def cached_close(symbol: str, target: date) -> Optional[Tuple[str, float]]:
        key = (symbol.upper(), target.isoformat())
        if key not in price_cache:
            price_cache[key] = _close_on_or_before(symbol, target, fetcher=price_fetcher)
        return price_cache[key]

    counts = {"evaluated": 0, "delisted": 0, "skipped": 0}

    with _connect(db_path) as conn:
        for row in due:
            run_date = datetime.fromisoformat(row["run_date"]).date()
            horizon = row["horizon_days"]
            eval_date = run_date + timedelta(days=horizon)
            ticker = row["ticker"]
            direction = row["direction"]
            entry_price = row["entry_price"]
            benchmark = row["benchmark_symbol"]
            benchmark_entry = row["benchmark_entry_price"]

            # Backfill missing entry prices from history (rare, but possible
            # if record_picks was called without a current FMP price).
            if entry_price is None:
                ep = cached_close(ticker, run_date)
                if ep:
                    entry_price = ep[1]
                    conn.execute(
                        "UPDATE picks SET entry_price = ? WHERE pick_id = ?",
                        (entry_price, row["pick_id"]),
                    )
            if benchmark_entry is None and benchmark:
                bep = cached_close(benchmark, run_date)
                if bep:
                    benchmark_entry = bep[1]
                    conn.execute(
                        "UPDATE picks SET benchmark_entry_price = ? WHERE pick_id = ?",
                        (benchmark_entry, row["pick_id"]),
                    )

            exit_row = cached_close(ticker, eval_date)
            bench_exit_row = cached_close(benchmark, eval_date) if benchmark else None

            # Ticker has no price near eval_date — treat as delisted/halted.
            if exit_row is None:
                bench_exit = bench_exit_row[1] if bench_exit_row else None
                bench_ret = (
                    (bench_exit - benchmark_entry) / benchmark_entry
                    if (bench_exit is not None and benchmark_entry)
                    else None
                )
                conn.execute(
                    """INSERT INTO evaluations
                       (pick_id, horizon_days, eval_date, exit_price,
                        benchmark_exit_price, ticker_return, benchmark_return,
                        alpha, hit, delisted)
                       VALUES (?, ?, ?, NULL, ?, NULL, ?, NULL, NULL, 1)""",
                    (row["pick_id"], horizon, eval_date.isoformat(), bench_exit, bench_ret),
                )
                counts["delisted"] += 1
                continue

            if not entry_price:
                logger.warning(
                    f"Skipping {ticker} pick {row['pick_id'][:8]}: no entry_price after backfill."
                )
                counts["skipped"] += 1
                continue

            exit_price = exit_row[1]
            ticker_return = (exit_price - entry_price) / entry_price

            benchmark_return = None
            alpha = None
            if bench_exit_row and benchmark_entry:
                benchmark_return = (bench_exit_row[1] - benchmark_entry) / benchmark_entry
                alpha = ticker_return - benchmark_return

            hit = _compute_hit(direction, alpha) if alpha is not None else None

            conn.execute(
                """INSERT INTO evaluations
                   (pick_id, horizon_days, eval_date, exit_price,
                    benchmark_exit_price, ticker_return, benchmark_return,
                    alpha, hit, delisted)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                (
                    row["pick_id"], horizon, eval_date.isoformat(),
                    exit_price,
                    bench_exit_row[1] if bench_exit_row else None,
                    ticker_return, benchmark_return, alpha, hit,
                ),
            )
            counts["evaluated"] += 1

    logger.info(
        f"Done. evaluated={counts['evaluated']} delisted={counts['delisted']} "
        f"skipped={counts['skipped']}"
    )
    return counts


# ---------- Track-record summary (for injection into report prompts) ----------

def summarize_track_record(
    horizon_days: int = 90,
    since_days: Optional[int] = 365,
    db_path: str = DEFAULT_DB_PATH,
) -> List[Dict]:
    """Per-agent hit rate + avg alpha at a given horizon, over the last `since_days`.

    Returns rows like:
        {"agent": "bull_agent", "n": 47, "hits": 27,
         "hit_rate": 0.574, "avg_alpha": 0.023}
    """
    where_clauses = ["e.horizon_days = ?", "e.delisted = 0", "e.hit IS NOT NULL"]
    params: List = [horizon_days]
    if since_days is not None:
        cutoff = (date.today() - timedelta(days=since_days)).isoformat()
        where_clauses.append("r.run_date >= ?")
        params.append(cutoff)
    where_sql = " AND ".join(where_clauses)

    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT p.agent,
                   COUNT(*) AS n,
                   SUM(e.hit) AS hits,
                   AVG(e.alpha) AS avg_alpha
            FROM evaluations e
            JOIN picks p ON p.pick_id = e.pick_id
            JOIN reports r ON r.report_id = p.report_id
            WHERE {where_sql}
            GROUP BY p.agent
            ORDER BY hits * 1.0 / n DESC
            """,
            params,
        ).fetchall()

    return [
        {
            "agent": r["agent"],
            "n": r["n"],
            "hits": r["hits"] or 0,
            "hit_rate": (r["hits"] or 0) / r["n"] if r["n"] else 0.0,
            "avg_alpha": r["avg_alpha"] or 0.0,
        }
        for r in rows
    ]


# ---------- CLI ----------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Evaluate matured industry-report picks.")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="path to track_record.sqlite")
    parser.add_argument("--today", default=None, help="override today as YYYY-MM-DD (for backfill)")
    parser.add_argument("--horizons", default="30,90,180", help="comma-separated horizons in days")
    parser.add_argument("--summary", action="store_true", help="print per-agent track record after evaluating")
    args = parser.parse_args()

    today_arg = date.fromisoformat(args.today) if args.today else None
    horizons = [int(x) for x in args.horizons.split(",") if x.strip()]
    result = evaluate_pending(today=today_arg, horizons=horizons, db_path=args.db)
    print(
        f"evaluated={result['evaluated']} "
        f"delisted={result['delisted']} "
        f"skipped={result['skipped']}"
    )

    if args.summary:
        for h in horizons:
            print(f"\n--- Track record @ {h}d ---")
            rows = summarize_track_record(horizon_days=h, db_path=args.db)
            if not rows:
                print("  (no matured evaluations)")
                continue
            for r in rows:
                print(
                    f"  {r['agent']:20s} n={r['n']:4d} "
                    f"hit_rate={r['hit_rate']*100:5.1f}% "
                    f"avg_alpha={r['avg_alpha']*100:+5.2f}%"
                )
