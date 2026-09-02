"""
DCA Matrix — signal log & running edge tracker.

Persists every fired signal to a CSV, updates forward returns as they
mature, and reports the running edge vs the same-name base rate.

FAILURE CONDITION (fixed BEFORE we have live signals)
  Across ~30+ MATURED 26-week signals, the mean 26w forward return
  should be meaningfully above the same-name unconditional 26w return
  over the same window. If it isn't, the edge died.

  Below ~30 matured h26 signals the read is insufficient — do NOT
  react to whatever number the report shows. Judging by individual
  trades will make you abandon a working signal or keep a broken one.

Entry rule (locked; do not tune reactively):
  wr_breakout AND atr_pct / atr_pct.rolling(50).mean() < 1.00
  with wrDeep=-85, wrExit=-80, wrDeepLB=20

Usage
    python dca_signal_log.py --tickers-file disruption_index.csv
    python dca_signal_log.py --tickers-file disruption_index.csv --backfill 8
        (backfill seeds the log with signals from the last 8 years)
"""

from __future__ import annotations

import argparse
import io
import os
import smtplib
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from atr_direction_test import fetch_yf, fetch_fmp, load_tickers, to_weekly

load_dotenv()
EMAIL_RECIPIENT_DEFAULT = "daquinn@targetedequityconsulting.com"

# --- locked entry-rule params (validated across SP500 and disruption universes) ---
WR_LEN, WR_DEEP, WR_EXIT, WR_LB = 14, -85.0, -80.0, 20
ATR_LEN, ATR_BASE, ATR_THRESH = 14, 50, 1.00       # CALM: ATR% < 1.00x its own SMA
VOL_BASE, CLIMAX_MULT = 50, 1.50                    # PANIC: volume > 1.50x its own SMA
MIN_GAP = 5
HORIZONS = [4, 12, 26]

# path values: "CALM"    = W%R breakout + ATR contracts (primary BUY, atr_100 validated)
#              "PANIC"   = W%R breakout + volume climax  (primary BUY, vol_climax_150 validated)
#              "BLOCKED" = W%R breakout + neither        (informational)
# When both fire, CALM wins (matches Pine v6 semantics: isPanic = panicRaw and not calmRaw)

LOG_COLS = ["ticker", "sector", "industry", "signal_date", "close", "wr",
            "atr_ratio", "vol_ratio", "path",
            "price_h4", "ret_h4_pct",
            "price_h12", "ret_h12_pct",
            "price_h26", "ret_h26_pct"]

SECTOR_CACHE_PATH = "ticker_sectors_cache.csv"
SP500_SECTORS_XLSX = "SP500_list_with_sectors.xlsx"


# =====================================================================
def williams_r(df, n):
    hh = df["high"].rolling(n).max()
    ll = df["low"].rolling(n).min()
    return (-100.0 * (hh - df["close"]) / (hh - ll).replace(0, np.nan)).fillna(-50.0)


def atr_pct(df, n):
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"],
                    (df["high"] - pc).abs(),
                    (df["low"] - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0/n, adjust=False, min_periods=n).mean() / df["close"]


def scan_signals(weekly: pd.DataFrame) -> pd.DataFrame:
    """Return every W%R breakout with its path classification.

    CALM  : W%R breakout AND ATR% < ATR_THRESH x its 50-bar SMA
    PANIC : W%R breakout AND volume > CLIMAX_MULT x its 50-bar SMA
            (only if not CALM; CALM wins on overlap)
    BLOCKED: W%R breakout that fails both filters (informational)

    Each category is deduped separately (MIN_GAP bars between same-category
    signals). Requires the weekly frame to have a 'volume' column; if
    missing, PANIC classification is skipped and those breakouts fall into
    BLOCKED."""
    wr = williams_r(weekly, WR_LEN)
    was_deep = wr.rolling(WR_LB).min() <= WR_DEEP
    raw = ((wr > WR_EXIT) & (wr.shift(1) <= WR_EXIT) & was_deep)

    ap = atr_pct(weekly, ATR_LEN)
    atr_ratio = ap / ap.rolling(ATR_BASE).mean()

    if "volume" in weekly.columns:
        vol = weekly["volume"].replace(0, np.nan)
        vol_ratio = vol / vol.rolling(VOL_BASE).mean()
    else:
        vol_ratio = pd.Series(np.nan, index=weekly.index)

    contracting = atr_ratio < ATR_THRESH
    climax = vol_ratio > CLIMAX_MULT

    calm_mask = raw & contracting.fillna(False)
    panic_mask = raw & climax.fillna(False) & ~contracting.fillna(False)
    blocked_mask = raw & ~contracting.fillna(False) & ~climax.fillna(False)

    def _dedup(mask):
        idx = np.where(mask.values)[0]
        kept, last = [], -10**6
        for i in idx:
            if (i - last) > MIN_GAP:
                kept.append(i)
                last = i
        return kept

    calm_set = set(_dedup(calm_mask))
    panic_set = set(_dedup(panic_mask))
    blocked_set = set(_dedup(blocked_mask))
    all_idx = sorted(calm_set | panic_set | blocked_set)
    if not all_idx:
        return pd.DataFrame(columns=["signal_date", "close", "wr", "atr_ratio", "vol_ratio", "path"])

    def _path(i):
        if i in calm_set:
            return "CALM"
        if i in panic_set:
            return "PANIC"
        return "BLOCKED"

    return pd.DataFrame({
        "signal_date": weekly.index[all_idx].strftime("%Y-%m-%d"),
        "close": weekly["close"].values[all_idx].round(4),
        "wr": wr.values[all_idx].round(2),
        "atr_ratio": np.round(atr_ratio.values[all_idx], 3),
        "vol_ratio": np.round(vol_ratio.values[all_idx], 3),
        "path": [_path(i) for i in all_idx],
    })


# =====================================================================
# Sector / industry lookup — SP500 file first, yfinance fallback, cached to disk
# =====================================================================
def load_sector_cache() -> dict:
    """Return dict UPPER_TICKER -> (sector, industry)."""
    cache = {}
    if os.path.exists(SP500_SECTORS_XLSX):
        try:
            df = pd.read_excel(SP500_SECTORS_XLSX)
            for _, r in df.iterrows():
                tk = str(r["Symbol"]).strip().replace(".", "-").upper()
                cache[tk] = (
                    str(r["Sector"]) if pd.notna(r.get("Sector")) else "",
                    str(r["Industry"]) if pd.notna(r.get("Industry")) else "")
        except Exception as e:
            print(f"  !! could not read {SP500_SECTORS_XLSX}: {e}")
    if os.path.exists(SECTOR_CACHE_PATH):
        try:
            cdf = pd.read_csv(SECTOR_CACHE_PATH)
            for _, r in cdf.iterrows():
                tk = str(r["ticker"]).upper()
                sec = str(r["sector"]) if pd.notna(r.get("sector")) else ""
                ind = str(r["industry"]) if pd.notna(r.get("industry")) else ""
                if tk not in cache or not cache[tk][0]:
                    cache[tk] = (sec, ind)
        except Exception:
            pass
    return cache


def _fetch_sector_industry(ticker: str):
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        return info.get("sector") or "", info.get("industry") or ""
    except Exception:
        return "", ""


def ensure_sectors(tickers, cache: dict) -> dict:
    """Fill cache for any missing tickers via yfinance; persist to disk."""
    missing = [t for t in tickers
               if t.upper() not in cache or not cache[t.upper()][0]]
    if not missing:
        return cache
    print(f"looking up sector/industry for {len(missing)} tickers ...")
    new_rows = []
    for i, tk in enumerate(missing, 1):
        s, ind = _fetch_sector_industry(tk)
        cache[tk.upper()] = (s, ind)
        new_rows.append({"ticker": tk.upper(), "sector": s, "industry": ind})
        if i % 25 == 0:
            print(f"  ... {i}/{len(missing)}")
    if os.path.exists(SECTOR_CACHE_PATH):
        existing = pd.read_csv(SECTOR_CACHE_PATH)
    else:
        existing = pd.DataFrame(columns=["ticker", "sector", "industry"])
    combined = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ticker"], keep="last")
    combined.to_csv(SECTOR_CACHE_PATH, index=False)
    return cache


def fill_sector_industry(log: pd.DataFrame, cache: dict) -> pd.DataFrame:
    """Populate empty sector/industry cells from the cache."""
    if "sector" in log.columns:
        log["sector"] = log["sector"].astype(object)
    if "industry" in log.columns:
        log["industry"] = log["industry"].astype(object)
    for i, row in log.iterrows():
        cur_sec = row.get("sector")
        if pd.notna(cur_sec) and str(cur_sec).strip() and str(cur_sec).lower() != "nan":
            continue
        tk = str(row["ticker"]).upper()
        s, ind = cache.get(tk, ("", ""))
        # normalize "Unknown"/empty variants to a single label for grouping
        if not s or s.strip().lower() in ("unknown", "none", "nan", ""):
            s = "(unknown)"
        if not ind or ind.strip().lower() in ("unknown", "none", "nan", ""):
            ind = "(unknown)"
        log.at[i, "sector"] = s
        log.at[i, "industry"] = ind
    return log


def load_log(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        df = pd.read_csv(path)
        # Migrate legacy atr_ok column -> path (CALM/BLOCKED). Historical
        # rows have no volume info so we cannot retroactively detect PANIC.
        if "path" not in df.columns:
            if "atr_ok" in df.columns:
                df["path"] = np.where(df["atr_ok"].fillna(True).astype(bool),
                                      "CALM", "BLOCKED")
            elif "atr_ratio" in df.columns:
                df["path"] = np.where(df["atr_ratio"] < ATR_THRESH,
                                      "CALM", "BLOCKED")
            else:
                df["path"] = "CALM"
        for col in LOG_COLS:
            if col not in df.columns:
                df[col] = np.nan
        # Normalize path to string
        df["path"] = df["path"].fillna("BLOCKED").astype(str)
        return df[LOG_COLS]
    return pd.DataFrame(columns=LOG_COLS)


def fill_forward_returns(log: pd.DataFrame, weekly_by_ticker: dict) -> pd.DataFrame:
    """Populate price/ret columns for signals whose h-week bar now exists."""
    for i, row in log.iterrows():
        tk = row["ticker"]
        w = weekly_by_ticker.get(tk)
        if w is None or w.empty:
            continue
        sd = pd.to_datetime(row["signal_date"])
        matching = w.index[w.index >= sd]
        if len(matching) == 0:
            continue
        anchor = matching[0]
        anchor_i = w.index.get_loc(anchor)
        close0 = float(row["close"])
        for h in HORIZONS:
            tgt_i = anchor_i + h
            if tgt_i >= len(w):
                continue
            price_col = f"price_h{h}"
            ret_col = f"ret_h{h}_pct"
            if pd.notna(row.get(price_col)):
                continue        # already filled
            px = float(w["close"].iloc[tgt_i])
            log.at[i, price_col] = round(px, 4)
            log.at[i, ret_col] = round((px / close0 - 1.0) * 100.0, 3)
    return log


def find_panic_followups(log: pd.DataFrame, weekly_by_ticker: dict) -> pd.DataFrame:
    """PANIC signals that fired ~4 weeks ago (25-31 days) — the review point.
    Returns rows with current_close and ret_since_signal_pct attached."""
    panics = log[log["path"] == "PANIC"].copy()
    if panics.empty:
        return panics
    today = pd.Timestamp.now().normalize()
    lo = today - pd.Timedelta(days=31)
    hi = today - pd.Timedelta(days=25)
    panics["_date"] = pd.to_datetime(panics["signal_date"])
    fu = panics[(panics["_date"] >= lo) & (panics["_date"] <= hi)].copy()
    if fu.empty:
        return fu
    fu["current_close"] = np.nan
    fu["ret_since_signal_pct"] = np.nan
    for i, r in fu.iterrows():
        w = weekly_by_ticker.get(r["ticker"])
        if w is None or w.empty:
            continue
        px_now = float(w["close"].iloc[-1])
        fu.at[i, "current_close"] = round(px_now, 4)
        fu.at[i, "ret_since_signal_pct"] = round((px_now / float(r["close"]) - 1) * 100, 2)
    return fu


def compute_base_rates(weekly_by_ticker: dict, min_date, max_date) -> dict:
    """Per-ticker unconditional forward returns over [min_date, max_date]."""
    bases = {}
    for tk, w in weekly_by_ticker.items():
        wsub = w.loc[(w.index >= min_date) & (w.index <= max_date)]
        if len(wsub) < 30:
            continue
        c = wsub["close"].values
        bases[tk] = {}
        for h in HORIZONS:
            if len(c) <= h:
                bases[tk][h] = np.nan
                continue
            fwd = (c[h:] / c[:-h] - 1.0) * 100.0
            bases[tk][h] = float(np.mean(fwd))
    return bases


def _row_edge(r, h, bases):
    b = bases.get(r["ticker"], {}).get(h)
    ret = r[f"ret_h{h}_pct"]
    return (ret - b) if (b is not None and not np.isnan(b) and pd.notna(ret)) else np.nan


def _horizon_stats(rows: pd.DataFrame, h: int, bases: dict) -> str:
    ret_col = f"ret_h{h}_pct"
    mature = rows[rows[ret_col].notna()]
    n = len(mature)
    if n == 0:
        return f"h{h:>2}: 0 matured"
    mean_ret = mature[ret_col].mean()
    hit = (mature[ret_col] > 0).mean() * 100
    edges = [_row_edge(r, h, bases) for _, r in mature.iterrows()]
    edges = [e for e in edges if not np.isnan(e)]
    edge_mean = float(np.mean(edges)) if edges else float("nan")
    gate = "OK" if n >= 30 else f"INSUFF n={n}"
    return (f"h{h:>2}: matured={n:<4}  hit%={hit:5.1f}  "
            f"mean_ret={mean_ret:+6.2f}%  edge_mean={edge_mean:+6.2f} pp  [{gate}]")


def build_report_text(log: pd.DataFrame, bases: dict,
                      new_calm_df: pd.DataFrame,
                      new_panic_df: pd.DataFrame,
                      new_blocked_df: pd.DataFrame,
                      followups_df: pd.DataFrame = None) -> str:
    lines = []
    n_total = len(log)
    n_calm  = int((log["path"] == "CALM").sum())
    n_panic = int((log["path"] == "PANIC").sum())
    n_blk   = int((log["path"] == "BLOCKED").sum())

    lines.append("--- DCA Matrix Signal Log ---")
    lines.append("CALM  = W%R breakout AND ATR% < SMA(ATR%,50)      (calm accumulation)")
    lines.append("PANIC = W%R breakout AND volume > 1.5x SMA(vol,50) (climax capitulation)")
    lines.append("Params: wrDeep=-85, wrExit=-80, wrDeepLB=20")
    lines.append(f"Log: {n_total} W%R breakouts  ({n_calm} CALM / {n_panic} PANIC / {n_blk} blocked)")
    lines.append(f"Names: {log['ticker'].nunique()}   Date range: {log['signal_date'].min()} to {log['signal_date'].max()}")
    lines.append(f"New this run: {len(new_calm_df)} CALM, {len(new_panic_df)} PANIC, {len(new_blocked_df)} blocked")
    lines.append("")

    calms  = log[log["path"] == "CALM"].copy()
    panics = log[log["path"] == "PANIC"].copy()
    buys   = log[log["path"].isin(["CALM", "PANIC"])].copy()

    lines.append("--- Running edge, ALL BUYs (CALM + PANIC) ---")
    for h in HORIZONS:
        lines.append(_horizon_stats(buys, h, bases))
    lines.append("")

    lines.append("--- Running edge, CALM only ---")
    for h in HORIZONS:
        lines.append(_horizon_stats(calms, h, bases))
    lines.append("")

    lines.append("--- Running edge, PANIC only ---")
    for h in HORIZONS:
        lines.append(_horizon_stats(panics, h, bases))
    lines.append("")

    # ---- Performance by sector (BUY signals, h26 matured) ----
    lines.append("--- Performance by sector (BUY signals, h26 matured) ---")
    h = 26
    ret_col = f"ret_h{h}_pct"
    mb = buys[buys[ret_col].notna()].copy()
    if len(mb) == 0:
        lines.append("(none matured yet)")
    else:
        mb["_sector"] = (mb["sector"].fillna("").astype(str).str.strip()
                         .apply(lambda s: "(unknown)"
                                if not s or s.lower() in ("unknown", "nan")
                                else s))
        mb["_edge"] = mb.apply(lambda r: _row_edge(r, h, bases), axis=1)
        agg = mb.groupby("_sector").agg(
            n=("_sector", "size"),
            mean_ret=(ret_col, "mean"),
            edge_pp=("_edge", "mean"),
            hit=(ret_col, lambda s: (s > 0).mean() * 100)
        ).sort_values("n", ascending=False)
        lines.append(f"  {'Sector':<26} {'n':>4}  {'mean_ret%':>9}  {'edge_pp':>8}  {'hit%':>5}")
        for sec, r in agg.iterrows():
            lines.append(f"  {sec:<26} {int(r['n']):>4}  {r['mean_ret']:>+8.2f}%  "
                         f"{r['edge_pp']:>+7.2f}   {r['hit']:>4.1f}")
    lines.append("")

    def _fmt_row(r, marker):
        sec = (str(r.get("sector") or "") or "?").strip() or "?"
        ind = (str(r.get("industry") or "") or "?").strip() or "?"
        atr = r.get("atr_ratio")
        vol = r.get("vol_ratio")
        atr_s = f"{atr:>5.2f}" if pd.notna(atr) else "  -  "
        vol_s = f"{vol:>5.2f}" if pd.notna(vol) else "  -  "
        return (f"  {marker} {str(r['ticker']):<6}  [{sec} / {ind}]  {r['signal_date']}  "
                f"close={r['close']:>8.2f}  wr={r['wr']:>6.2f}  "
                f"atr={atr_s}  vol={vol_s}")

    if not new_calm_df.empty:
        lines.append(f"--- New CALM BUY signals this run ({len(new_calm_df)}) — quiet-base accumulation ---")
        for _, r in new_calm_df.sort_values("signal_date").iterrows():
            lines.append(_fmt_row(r, "***"))
        lines.append("")

    if not new_panic_df.empty:
        lines.append(f"--- New PANIC BUY signals this run ({len(new_panic_df)}) — climax capitulation ---")
        for _, r in new_panic_df.sort_values("signal_date").iterrows():
            lines.append(_fmt_row(r, "###"))
        lines.append("")

    if not new_blocked_df.empty:
        lines.append(f"--- New W%R breakouts blocked (informational, do not buy) ({len(new_blocked_df)}) ---")
        for _, r in new_blocked_df.sort_values("signal_date").iterrows():
            lines.append(_fmt_row(r, "   "))
        lines.append("")

    if followups_df is not None and not followups_df.empty:
        lines.append(f"--- PANIC follow-ups: signals from ~4 weeks ago, review for follow-through ({len(followups_df)}) ---")
        for _, r in followups_df.sort_values("signal_date").iterrows():
            sec = (str(r.get("sector") or "") or "?").strip() or "?"
            ret = r.get("ret_since_signal_pct")
            ret_s = f"{ret:+6.2f}%" if pd.notna(ret) else "  n/a "
            now_s = f"{r.get('current_close'):>8.2f}" if pd.notna(r.get('current_close')) else "    -   "
            lines.append(f"  !!! {str(r['ticker']):<6}  [{sec}]  fired {r['signal_date']}  "
                         f"entry={r['close']:>8.2f}  now={now_s}  ret={ret_s}")
        lines.append("")

    lines.append("Reminder: below the sufficiency gate the numbers are noise.")
    lines.append("Do NOT adjust the entry rule or take signals the system did not generate.")
    lines.append("Both CALM and PANIC are validated OOS (permutation p<0.05 at h12/h26). Edge concentrates 12-26 weeks out.")
    return "\n".join(lines)


def send_email(body: str, log_path: str, recipient: str) -> bool:
    user = os.getenv("EMAIL_ADDRESS")
    pwd = os.getenv("EMAIL_PASSWORD")
    if not user or not pwd:
        print("EMAIL_ADDRESS / EMAIL_PASSWORD not set — skipping email.")
        return False
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = recipient
    msg["Subject"] = f"DCA Signal Log — {datetime.now():%Y-%m-%d}"
    msg.attach(MIMEText(body, "plain"))
    if os.path.exists(log_path):
        with open(log_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition",
                        f"attachment; filename={os.path.basename(log_path)}")
        msg.attach(part)
    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)
    print(f"emailed report to {recipient}")
    return True


def report(log: pd.DataFrame, bases: dict,
           new_calm_df: pd.DataFrame, new_panic_df: pd.DataFrame,
           new_blocked_df: pd.DataFrame):
    print("\n" + "=" * 72)
    print(build_report_text(log, bases, new_calm_df, new_panic_df, new_blocked_df))
    print("=" * 72)


# =====================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="+", default=None)
    ap.add_argument("--tickers-file", default=None)
    ap.add_argument("--source", choices=["fmp", "yfinance"], default="yfinance")
    ap.add_argument("--years", type=int, default=15,
                    help="years of history to fetch (for base rate + backfill)")
    ap.add_argument("--backfill", type=int, default=0,
                    help="on first run, seed log with signals from the last N years "
                         "(0 = only add signals fired since the last run)")
    ap.add_argument("--log", default="dca_signal_log.csv")
    ap.add_argument("--email", action="store_true",
                    help="email the summary report (uses EMAIL_ADDRESS/EMAIL_PASSWORD from .env)")
    ap.add_argument("--recipient", default=EMAIL_RECIPIENT_DEFAULT)
    args = ap.parse_args()

    if not (args.tickers or args.tickers_file):
        ap.error("provide --tickers or --tickers-file")

    fetch = {"fmp": fetch_fmp, "yfinance": fetch_yf}[args.source]
    tickers = load_tickers(args.tickers_file) if args.tickers_file else args.tickers

    log = load_log(args.log)
    is_first_run = log.empty
    if is_first_run and args.backfill == 0:
        print("NOTE: log is empty and --backfill not set. First run will start")
        print("with today's fires only. Use --backfill 8 to seed the last 8 years.\n")

    print(f"fetching {len(tickers)} tickers ({args.years}y weekly) ...")
    weekly_by_ticker = {}
    for i, tk in enumerate(tickers, 1):
        try:
            daily = fetch(tk, args.years)
        except Exception:
            continue
        w = to_weekly(daily)
        if len(w) < 60:
            continue
        weekly_by_ticker[tk] = w
        if i % 50 == 0:
            print(f"  ... {i}/{len(tickers)}")
    print(f"loaded {len(weekly_by_ticker)} tickers")

    # scan for signals
    new_rows = []
    cutoff = (pd.Timestamp.now() - pd.Timedelta(days=365 * max(args.backfill, 0))
              if args.backfill > 0 else
              (pd.to_datetime(log["signal_date"]).max() if not log.empty else None))
    for tk, w in weekly_by_ticker.items():
        fires = scan_signals(w)
        if fires.empty:
            continue
        fires["ticker"] = tk
        if cutoff is not None and not is_first_run:
            fires = fires[pd.to_datetime(fires["signal_date"]) > cutoff]
        elif cutoff is not None and is_first_run:
            fires = fires[pd.to_datetime(fires["signal_date"]) >= cutoff]
        for col in LOG_COLS:
            if col not in fires.columns:
                fires[col] = np.nan
        new_rows.append(fires[LOG_COLS])

    new_df_for_email = pd.DataFrame(columns=LOG_COLS)
    if new_rows:
        new_df = pd.concat(new_rows, ignore_index=True)
        combined = pd.concat([log, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["ticker", "signal_date"], keep="first")
        added = len(combined) - len(log)
        print(f"added {added} new signals to log")
        merged = combined.merge(log[["ticker", "signal_date"]].assign(_seen=1),
                                on=["ticker", "signal_date"], how="left")
        new_df_for_email = merged[merged["_seen"].isna()].drop(columns=["_seen"])
        log = combined
    else:
        added = 0
        print("no new signals since last run")

    log = fill_forward_returns(log, weekly_by_ticker)

    # populate sector/industry from cache (fills legacy rows too)
    print("loading sector/industry cache ...")
    sector_cache = load_sector_cache()
    sector_cache = ensure_sectors(list(weekly_by_ticker.keys()), sector_cache)
    log = fill_sector_industry(log, sector_cache)
    if not new_df_for_email.empty:
        new_df_for_email = fill_sector_industry(new_df_for_email, sector_cache)

    log = log.sort_values(["signal_date", "ticker"]).reset_index(drop=True)
    log.to_csv(args.log, index=False)

    if log.empty:
        print("\nlog empty — nothing to report yet.")
        return

    min_date = pd.to_datetime(log["signal_date"]).min()
    max_date = pd.Timestamp.now()
    bases = compute_base_rates(weekly_by_ticker, min_date, max_date)

    new_calm_df    = new_df_for_email[new_df_for_email["path"] == "CALM"]
    new_panic_df   = new_df_for_email[new_df_for_email["path"] == "PANIC"]
    new_blocked_df = new_df_for_email[new_df_for_email["path"] == "BLOCKED"]
    followups_df   = find_panic_followups(log, weekly_by_ticker)

    print(build_report_text(log, bases, new_calm_df, new_panic_df, new_blocked_df, followups_df))
    print(f"\nwrote {args.log}")

    if args.email:
        body = build_report_text(log, bases, new_calm_df, new_panic_df, new_blocked_df, followups_df)
        try:
            send_email(body, args.log, args.recipient)
        except Exception as e:
            print(f"email failed: {e}")


if __name__ == "__main__":
    main()
