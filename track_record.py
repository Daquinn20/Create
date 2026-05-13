"""Track-record persistence for industry report picks.

Stores every report's picks at generation time so that, after horizons
mature (30/90/180d), each pick can be scored against a sector benchmark.
Builds a verifiable hit rate per agent persona over time.

The schema is embedded as SCHEMA and applied idempotently by init_db().
For this single-developer project that file IS the migration; if a
migrations/ directory is added later, lift SCHEMA into 001_track_record.sql.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), "track_record.sqlite")

SECTOR_ETFS = {
    "Technology": "XLK",
    "Financials": "XLF",
    "Financial Services": "XLF",
    "Health Care": "XLV",
    "Healthcare": "XLV",
    "Consumer Discretionary": "XLY",
    "Consumer Cyclical": "XLY",
    "Consumer Staples": "XLP",
    "Consumer Defensive": "XLP",
    "Energy": "XLE",
    "Industrials": "XLI",
    "Materials": "XLB",
    "Basic Materials": "XLB",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}

CONVICTION_MAP = {"high": 5, "medium": 3, "low": 1}

VALID_DIRECTIONS = {"long", "short", "avoid", "neutral"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    report_id        TEXT PRIMARY KEY,
    run_date         TEXT NOT NULL,
    theme            TEXT NOT NULL,
    sector           TEXT,
    tickers_json     TEXT NOT NULL,
    model            TEXT,
    prompt_version   TEXT,
    lookback_days    INTEGER,
    created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS picks (
    pick_id                 TEXT PRIMARY KEY,
    report_id               TEXT NOT NULL REFERENCES reports(report_id) ON DELETE CASCADE,
    ticker                  TEXT NOT NULL,
    agent                   TEXT NOT NULL,
    direction               TEXT NOT NULL CHECK (direction IN ('long','short','avoid','neutral')),
    conviction              INTEGER,
    thesis                  TEXT,
    trend                   TEXT,
    entry_price             REAL,
    benchmark_symbol        TEXT,
    benchmark_entry_price   REAL,
    created_at              TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_picks_report ON picks(report_id);
CREATE INDEX IF NOT EXISTS idx_picks_ticker ON picks(ticker);
CREATE INDEX IF NOT EXISTS idx_picks_agent  ON picks(agent);

CREATE TABLE IF NOT EXISTS evaluations (
    pick_id              TEXT NOT NULL REFERENCES picks(pick_id) ON DELETE CASCADE,
    horizon_days         INTEGER NOT NULL,
    eval_date            TEXT NOT NULL,
    exit_price           REAL,
    benchmark_exit_price REAL,
    ticker_return        REAL,
    benchmark_return     REAL,
    alpha                REAL,
    hit                  INTEGER,
    delisted             INTEGER DEFAULT 0,
    created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pick_id, horizon_days)
);

CREATE INDEX IF NOT EXISTS idx_evals_horizon ON evaluations(horizon_days);
"""


@dataclass
class Pick:
    """A single stock call made by an agent within a report."""
    ticker: str
    agent: str
    direction: str
    thesis: str = ""
    trend: str = ""
    conviction: int = 3
    entry_price: Optional[float] = None


def conviction_from_label(label: str) -> int:
    """Map 'High'/'Medium'/'Low' to integer 1-5; unknown -> 3."""
    return CONVICTION_MAP.get((label or "").strip().lower(), 3)


def benchmark_for_sector(sector: Optional[str]) -> str:
    """Return the sector-SPDR ETF for the given sector, or 'SPY' fallback."""
    if not sector:
        return "SPY"
    return SECTOR_ETFS.get(sector.strip(), "SPY")


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Create the track-record schema if it doesn't exist. Idempotent."""
    with _connect(db_path) as conn:
        conn.executescript(SCHEMA)


def picks_from_winners_losers(
    analysis,
    agent: str,
    companies: List[Dict],
) -> List[Pick]:
    """Convert a WinnersLosersAnalysis into a flat list of Pick objects.

    Duck-typed on `analysis` to avoid importing from industry_report_generator
    (and the circular import that would create). Pulls entry_price from the
    matching company dict (FMP profile) when present.
    """
    price_by_symbol = {
        (c.get("symbol") or "").upper(): c.get("price")
        for c in companies or []
    }

    def _build(items, direction: str) -> List[Pick]:
        out: List[Pick] = []
        for item in items or []:
            sym = (getattr(item, "symbol", "") or "").upper()
            if not sym:
                continue
            out.append(Pick(
                ticker=sym,
                agent=agent,
                direction=direction,
                thesis=getattr(item, "rationale", "") or "",
                trend=getattr(item, "trend", "") or "",
                conviction=conviction_from_label(getattr(item, "confidence", "")),
                entry_price=price_by_symbol.get(sym),
            ))
        return out

    picks: List[Pick] = []
    picks += _build(getattr(analysis, "winners", []), "long")
    picks += _build(getattr(analysis, "losers", []), "short")
    picks += _build(getattr(analysis, "neutral", []), "neutral")
    return picks


def record_picks(
    theme: str,
    tickers: Iterable[str],
    picks: Iterable[Pick],
    *,
    sector: Optional[str] = None,
    model: Optional[str] = None,
    prompt_version: Optional[str] = None,
    lookback_days: Optional[int] = None,
    benchmark_entry_price: Optional[float] = None,
    run_date: Optional[date] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> str:
    """Persist a report + its picks. Returns the new report_id.

    The caller supplies entry_price on each Pick (sourced from the FMP profile
    used to build the report). Benchmark entry price is optional; if omitted,
    the evaluator job can backfill it from a historical-price endpoint when
    scoring matures.
    """
    init_db(db_path)

    report_id = str(uuid.uuid4())
    run_date_str = (run_date or date.today()).isoformat()
    benchmark_symbol = benchmark_for_sector(sector)
    tickers_list = [t.upper() for t in tickers]
    picks_list = list(picks)

    for p in picks_list:
        if p.direction not in VALID_DIRECTIONS:
            raise ValueError(
                f"Invalid direction {p.direction!r} for {p.ticker}; "
                f"must be one of {sorted(VALID_DIRECTIONS)}"
            )

    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO reports
               (report_id, run_date, theme, sector, tickers_json,
                model, prompt_version, lookback_days)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                report_id,
                run_date_str,
                theme,
                sector,
                json.dumps(tickers_list),
                model,
                prompt_version,
                lookback_days,
            ),
        )

        rows = [
            (
                str(uuid.uuid4()),
                report_id,
                p.ticker.upper(),
                p.agent,
                p.direction,
                p.conviction,
                p.thesis,
                p.trend,
                p.entry_price,
                benchmark_symbol,
                benchmark_entry_price,
            )
            for p in picks_list
        ]
        if rows:
            conn.executemany(
                """INSERT INTO picks
                   (pick_id, report_id, ticker, agent, direction, conviction,
                    thesis, trend, entry_price, benchmark_symbol, benchmark_entry_price)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

    logger.info(
        f"Recorded report {report_id[:8]} ({theme}) "
        f"with {len(picks_list)} picks, benchmark={benchmark_symbol}"
    )
    return report_id
