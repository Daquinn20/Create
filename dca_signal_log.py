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

# --- locked entry-rule params ---
WR_LEN, WR_DEEP, WR_EXIT, WR_LB = 14, -85.0, -80.0, 20
ATR_LEN, ATR_BASE, ATR_THRESH = 14, 50, 1.00
MIN_GAP = 5
HORIZONS = [4, 12, 26]

LOG_COLS = ["ticker", "signal_date", "close", "wr", "atr_ratio",
            "price_h4", "ret_h4_pct",
            "price_h12", "ret_h12_pct",
            "price_h26", "ret_h26_pct"]


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
    """Return DataFrame of (signal_date, close, wr, atr_ratio) rows where the
    entry rule fires."""
    wr = williams_r(weekly, WR_LEN)
    was_deep = wr.rolling(WR_LB).min() <= WR_DEEP
    raw = ((wr > WR_EXIT) & (wr.shift(1) <= WR_EXIT) & was_deep)

    ap = atr_pct(weekly, ATR_LEN)
    ratio = ap / ap.rolling(ATR_BASE).mean()
    fire = raw & (ratio < ATR_THRESH)

    # min-gap dedup: no two fires within MIN_GAP bars
    fires_idx = np.where(fire.values)[0]
    kept = []
    last = -10**6
    for i in fires_idx:
        if (i - last) > MIN_GAP:
            kept.append(i)
            last = i
    if not kept:
        return pd.DataFrame(columns=["signal_date", "close", "wr", "atr_ratio"])

    return pd.DataFrame({
        "signal_date": weekly.index[kept].strftime("%Y-%m-%d"),
        "close": weekly["close"].values[kept].round(4),
        "wr": wr.values[kept].round(2),
        "atr_ratio": ratio.values[kept].round(3),
    })


def load_log(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        df = pd.read_csv(path)
        for col in LOG_COLS:
            if col not in df.columns:
                df[col] = np.nan
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


def build_report_text(log: pd.DataFrame, bases: dict, added: int,
                      new_rows_df: pd.DataFrame) -> str:
    lines = []
    lines.append("--- DCA Matrix Signal Log ---")
    lines.append("Rule: wr_breakout AND atr_pct/atr_pct.rolling(50).mean() < 1.00")
    lines.append("      wrDeep=-85, wrExit=-80, wrDeepLB=20")
    lines.append(f"Total signals: {len(log)} across {log['ticker'].nunique()} names")
    lines.append(f"Date range: {log['signal_date'].min()} to {log['signal_date'].max()}")
    lines.append(f"New this run: {added}")
    lines.append("")

    for h in HORIZONS:
        ret_col = f"ret_h{h}_pct"
        mature = log[log[ret_col].notna()].copy()
        n = len(mature)
        if n == 0:
            lines.append(f"h{h:>2}: 0 matured — check back in {h} weeks")
            continue
        mean_ret = mature[ret_col].mean()
        median_ret = mature[ret_col].median()
        hit = (mature[ret_col] > 0).mean() * 100
        edges = []
        for _, r in mature.iterrows():
            b = bases.get(r["ticker"], {}).get(h)
            if b is not None and not np.isnan(b):
                edges.append(r[ret_col] - b)
        edge_mean = float(np.mean(edges)) if edges else float("nan")
        gate = "OK" if n >= 30 else f"INSUFFICIENT (need >=30, have {n})"
        lines.append(f"h{h:>2}: matured={n:<4}  hit%={hit:5.1f}  "
                     f"mean_ret={mean_ret:+6.2f}%  edge_mean={edge_mean:+6.2f} pp  [{gate}]")

    lines.append("")
    if added > 0 and not new_rows_df.empty:
        lines.append("--- New signals this run ---")
        for _, r in new_rows_df.sort_values("signal_date").iterrows():
            lines.append(f"  *** {r['ticker']:<6}  {r['signal_date']}  "
                         f"close={r['close']:>8.2f}  wr={r['wr']:>6.2f}  "
                         f"atr_ratio={r['atr_ratio']:>5.2f}")
        lines.append("")

    lines.append("Reminder: below the sufficiency gate the numbers are noise.")
    lines.append("Do NOT adjust the entry rule or take signals the system did not generate.")
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


def report(log: pd.DataFrame, bases: dict):
    print("\n" + "=" * 72)
    print(f"SIGNAL LOG — {len(log)} total signals across {log['ticker'].nunique()} names")
    print(f"date range: {log['signal_date'].min()} to {log['signal_date'].max()}")
    print("=" * 72)

    for h in HORIZONS:
        ret_col = f"ret_h{h}_pct"
        mature = log[log[ret_col].notna()].copy()
        n = len(mature)
        if n == 0:
            print(f"\n  h{h}: 0 matured — check back in {h} weeks")
            continue

        mean_ret = mature[ret_col].mean()
        median_ret = mature[ret_col].median()
        hit = (mature[ret_col] > 0).mean() * 100

        edges = []
        for _, r in mature.iterrows():
            b = bases.get(r["ticker"], {}).get(h)
            if b is None or np.isnan(b):
                continue
            edges.append(r[ret_col] - b)
        edge_mean = float(np.mean(edges)) if edges else np.nan
        edge_median = float(np.median(edges)) if edges else np.nan

        gate = "OK" if n >= 30 else f"INSUFFICIENT (need >=30, have {n})"
        print(f"\n  h{h}: matured={n}  hit%={hit:.0f}  "
              f"mean_ret={mean_ret:+.2f}%  median_ret={median_ret:+.2f}%")
        print(f"        edge (signal - same-name base): "
              f"mean {edge_mean:+.2f} pp, median {edge_median:+.2f} pp   [{gate}]")

    print("\n" + "=" * 72)
    print("REMINDER: below the sufficiency gate the numbers are noise. Do NOT")
    print("adjust the entry rule or 'take signals the system did not generate'.")
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
        # keep just the rows actually added, for the email body
        merged = combined.merge(log[["ticker", "signal_date"]].assign(_seen=1),
                                on=["ticker", "signal_date"], how="left")
        new_df_for_email = merged[merged["_seen"].isna()].drop(columns=["_seen"])
        log = combined
    else:
        added = 0
        print("no new signals since last run")

    log = fill_forward_returns(log, weekly_by_ticker)
    log = log.sort_values(["signal_date", "ticker"]).reset_index(drop=True)
    log.to_csv(args.log, index=False)

    if log.empty:
        print("\nlog empty — nothing to report yet.")
        return

    min_date = pd.to_datetime(log["signal_date"]).min()
    max_date = pd.Timestamp.now()
    bases = compute_base_rates(weekly_by_ticker, min_date, max_date)

    report(log, bases)
    print(f"\nwrote {args.log}")

    if args.email:
        body = build_report_text(log, bases, added, new_df_for_email)
        try:
            send_email(body, args.log, args.recipient)
        except Exception as e:
            print(f"email failed: {e}")


if __name__ == "__main__":
    main()
