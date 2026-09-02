"""
Tune the W%R breakout params for DCA entry timing.

Sweeps three params — wrDeep (oversold floor), wrExit (cross level),
wrDeepLb (bars to look back for the deep reading) — and measures the
out-of-sample forward-return edge vs the unconditional base rate at
three horizons.

Only tunes the `wr_only` signal, since the SP500/200 test showed the
state-machinery filters add no OOS edge.

Baseline for reference: wrDeep=-80, wrExit=-80, wrDeepLb=10.

Usage
    python dca_v3_wr_tune.py --universe sp500 --limit 200 --timeframe weekly --years 15
    python dca_v3_wr_tune.py --tickers ZETA AAOI NKE SBUX META GOOGL LITE OSCR
"""

from __future__ import annotations

import argparse
import io
import os
import sys

import numpy as np
import pandas as pd

FMP_BASE = "https://financialmodelingprep.com/api/v3"


# =====================================================================
def williams_r(df, n):
    hh = df["high"].rolling(n).max()
    ll = df["low"].rolling(n).min()
    return (-100.0 * (hh - df["close"]) / (hh - ll).replace(0, np.nan)).fillna(-50.0)


def fetch_yf(t, y):
    import yfinance as yf
    df = yf.download(t, period=f"{y}y", auto_adjust=True, progress=False)
    if df is None or df.empty:
        raise ValueError("no data")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [c.lower() for c in df.columns]
    return df[["open", "high", "low", "close"]].astype(float)


def fetch_fmp(t, y):
    import requests
    k = os.environ.get("FMP_API_KEY")
    if not k:
        sys.exit("Set FMP_API_KEY or use --source yfinance")
    r = requests.get(f"{FMP_BASE}/historical-price-full/{t}",
                     params={"apikey": k, "timeseries": y * 260}, timeout=30)
    r.raise_for_status()
    d = r.json().get("historical", [])
    if not d:
        raise ValueError("no data")
    df = pd.DataFrame(d)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    adj = ["adjOpen", "adjHigh", "adjLow", "adjClose"]
    if all(x in df.columns for x in adj):
        df = df[adj]
        df.columns = ["open", "high", "low", "close"]
    else:
        df = df[["open", "high", "low", "close"]]
    return df.astype(float)


def to_weekly(df):
    return df.resample("W-FRI").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}).dropna()


def sp500():
    import requests
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    t = pd.read_html(io.StringIO(r.text))[0]
    return [s.replace(".", "-") for s in t["Symbol"].tolist()]


# =====================================================================
def measure(close: pd.Series, wr: pd.Series, deep: float, exit_lvl: float,
            lb: int, horizons: tuple) -> dict:
    """Median edge over unconditional base rate at each horizon, on this series."""
    was_deep = wr.rolling(lb).min() <= deep
    trig = (wr > exit_lvl) & (wr.shift(1) <= exit_lvl) & was_deep
    out = {"n_signals": int(trig.sum())}
    for h in horizons:
        fwd = (close.shift(-h) / close - 1.0) * 100.0
        base = fwd.dropna()
        at = fwd[trig.fillna(False)].dropna()
        out[f"h{h}_edge"] = (at.mean() - base.mean()) if len(at) else np.nan
        out[f"h{h}_n"] = len(at)
    return out


def run_combo(cache: dict, deep: float, exit_lvl: float, lb: int,
              horizons: tuple, split: bool) -> list[dict]:
    rows = []
    for tk, (close, wr) in cache.items():
        segs = ([("in_sample", close.iloc[:len(close)//2], wr.iloc[:len(wr)//2]),
                 ("out_sample", close.iloc[len(close)//2:], wr.iloc[len(wr)//2:])]
                if split else [("full", close, wr)])
        for seg_name, c_seg, w_seg in segs:
            if len(c_seg) < 150:
                continue
            m = measure(c_seg, w_seg, deep, exit_lvl, lb, horizons)
            rows.append({"ticker": tk, "segment": seg_name, **m})
    return rows


# =====================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="+",
                    default=["ZETA", "AAOI", "NKE", "SBUX", "META", "GOOGL", "LITE", "OSCR"])
    ap.add_argument("--universe", choices=["sp500"], default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--source", choices=["fmp", "yfinance"], default="yfinance")
    ap.add_argument("--years", type=int, default=15)
    ap.add_argument("--timeframe", choices=["daily", "weekly"], default="weekly")
    ap.add_argument("--wr-len", type=int, default=14)
    ap.add_argument("--deep-grid", nargs="+", type=float,
                    default=[-90, -85, -80, -75, -70])
    ap.add_argument("--exit-grid", nargs="+", type=float,
                    default=[-80, -70, -60, -50])
    ap.add_argument("--lb-grid", nargs="+", type=int,
                    default=[5, 10, 15, 20])
    ap.add_argument("--no-split", action="store_true")
    ap.add_argument("--out", default="dca_v3_wr_tune.csv")
    args = ap.parse_args()

    fetch = {"fmp": fetch_fmp, "yfinance": fetch_yf}[args.source]
    tickers = sp500() if args.universe else args.tickers
    if args.limit:
        tickers = tickers[:args.limit]
    horizons = (20, 60, 120) if args.timeframe == "daily" else (4, 12, 26)

    # Pre-fetch and pre-compute wr once per ticker
    print(f"Fetching {len(tickers)} tickers...")
    cache = {}
    for i, tk in enumerate(tickers, 1):
        try:
            daily = fetch(tk, args.years)
        except Exception as e:
            if not args.universe:
                print(f"  !! {tk}: {e}")
            continue
        data = daily if args.timeframe == "daily" else to_weekly(daily)
        if len(data) < 260:
            continue
        cache[tk] = (data["close"], williams_r(data, args.wr_len))
        if args.universe and i % 25 == 0:
            print(f"  ... {i}/{len(tickers)}")
    print(f"Cached {len(cache)} tickers")

    # Sweep
    combos = [(d, e, lb) for d in args.deep_grid for e in args.exit_grid
              for lb in args.lb_grid if e >= d]
    print(f"Sweeping {len(combos)} param combos...")

    summary = []
    per_ticker = []
    for d, e, lb in combos:
        rows = run_combo(cache, d, e, lb, horizons, split=not args.no_split)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        for r in rows:
            r.update({"wr_deep": d, "wr_exit": e, "wr_deep_lb": lb})
        per_ticker.extend(rows)

        for seg, g in df.groupby("segment"):
            row = {"wr_deep": d, "wr_exit": e, "wr_deep_lb": lb, "segment": seg,
                   "names": len(g), "avg_signals_per_name": g["n_signals"].mean().round(2)}
            for h in horizons:
                col = f"h{h}_edge"
                row[f"h{h}_median_edge"] = round(g[col].median(), 3)
                row[f"h{h}_names_pos_%"] = round((g[col] > 0).mean() * 100, 1)
            row["mean_of_medians"] = round(
                np.mean([row[f"h{h}_median_edge"] for h in horizons]), 3)
            summary.append(row)

    if not summary:
        print("no results")
        return

    sdf = pd.DataFrame(summary)
    sdf.to_csv(args.out, index=False)
    pd.DataFrame(per_ticker).to_csv(args.out.replace(".csv", "_per_ticker.csv"), index=False)

    print(f"\n{'='*80}\nTOP 10 param sets by OOS mean-of-median-edges\n{'='*80}")
    oos = sdf[sdf.segment == "out_sample"] if not args.no_split else sdf
    top = oos.sort_values("mean_of_medians", ascending=False).head(10)
    show_cols = (["wr_deep", "wr_exit", "wr_deep_lb", "avg_signals_per_name",
                  "mean_of_medians"]
                 + [f"h{h}_median_edge" for h in horizons]
                 + [f"h{h}_names_pos_%" for h in horizons])
    print(top[show_cols].to_string(index=False))

    # Baseline for comparison
    base = oos[(oos.wr_deep == -80) & (oos.wr_exit == -80) & (oos.wr_deep_lb == 10)]
    if len(base):
        print(f"\n{'='*80}\nBASELINE (deep=-80, exit=-80, lb=10) OOS\n{'='*80}")
        print(base[show_cols].to_string(index=False))

    # IS/OOS consistency check on the OOS top pick
    if not args.no_split and len(top):
        pick = top.iloc[0]
        is_row = sdf[(sdf.wr_deep == pick.wr_deep) & (sdf.wr_exit == pick.wr_exit)
                     & (sdf.wr_deep_lb == pick.wr_deep_lb) & (sdf.segment == "in_sample")]
        print(f"\n{'='*80}\nIN-SAMPLE consistency for OOS top pick "
              f"(deep={pick.wr_deep}, exit={pick.wr_exit}, lb={pick.wr_deep_lb})\n{'='*80}")
        print(is_row[show_cols].to_string(index=False))

    print(f"\nwrote {args.out}  and  {args.out.replace('.csv', '_per_ticker.csv')}")


if __name__ == "__main__":
    main()
