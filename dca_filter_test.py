"""
DCA Matrix — downtrend filter test.

The question: the W%R breakout is the one signal that survived out-of-sample
testing, but it fires on names that keep falling. Does adding a filter for
"this decline is stabilizing" or "this is a pullback, not a downtrend"
improve the entries?

FILTERS TESTED (each applied to the same wr_only breakout, one at a time)
  raw          no filter — the baseline that survived the earlier test
  atr_contract ATR% is below its own recent average (volatility contracting)
  ema200_up    price below the 200 EMA but the 200 EMA is RISING (pullback,
               not downtrend)
  higher_low   the most recent swing low is above the prior swing low
  rs_index     6-month return beats the benchmark's over the same window
  atr_and_ema  both atr_contract and ema200_up

CRITERION, SET BEFORE LOOKING
  A filter is worth using only if, OUT OF SAMPLE, it:
    (a) beats raw's median edge at more than one horizon, AND
    (b) leaves at least ~30 signals per name on average
  A filter that improves edge by cutting to 5 signals has found noise.
  The script prints both numbers side by side and flags the verdict.

Usage
    python dca_filter_test.py --universe sp500 --limit 200 --timeframe weekly
    python dca_filter_test.py --tickers ZETA AAOI NKE SBUX META GOOGL LITE OSCR
    python dca_filter_test.py --source synthetic          # false-positive check

Requires: pandas, numpy, yfinance (or requests + FMP_API_KEY)
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd

FMP_BASE = "https://financialmodelingprep.com/api/v3"
FILTERS = ["raw", "contract_100", "contract_085", "contract_110",
           "expand_110", "expand_140", "expand_any"]


@dataclass
class P:
    wr_len: int = 14
    wr_deep: float = -80.0
    wr_exit: float = -80.0
    wr_deep_lb: int = 10
    min_gap: int = 5

    atr_len: int = 14
    atr_base: int = 50
    atr_thresh: float = 1.00      # ATR% must be below this x its own average

    ema_slow: int = 200
    slope_lb: int = 20            # bars over which the 200 EMA must be rising

    pivot_lb: int = 5             # swing-low detection
    rs_lb: int = 26               # relative strength lookback (bars)


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


def swing_lows(low: pd.Series, k: int) -> pd.Series:
    """True where `low` is the minimum of the surrounding 2k+1 bars."""
    return low == low.rolling(2*k + 1, center=True).min()


def build(df: pd.DataFrame, p: P, bench: pd.Series | None) -> pd.DataFrame:
    out = df.copy()
    c = out["close"]

    out["wr"] = williams_r(out, p.wr_len)
    was_deep = out["wr"].rolling(p.wr_deep_lb).min() <= p.wr_deep
    raw_break = (out["wr"] > p.wr_exit) & (out["wr"].shift(1) <= p.wr_exit) & was_deep

    # enforce minimum spacing so clusters don't inflate the sample
    fired = np.zeros(len(out), dtype=bool)
    last = -10**6
    rb = raw_break.values
    for i in range(len(out)):
        if rb[i] and (i - last) > p.min_gap:
            fired[i] = True
            last = i
    out["raw"] = fired

    # ---- ATR ratio: contraction vs expansion, several cut points ----
    ap = atr_pct(out, p.atr_len)
    ratio = ap / ap.rolling(p.atr_base).mean()
    out["_ratio"] = ratio

    out["contract_100"] = out["raw"] & (ratio < 1.00)
    out["contract_085"] = out["raw"] & (ratio < 0.85)
    out["contract_110"] = out["raw"] & (ratio < 1.10)
    out["expand_110"]   = out["raw"] & (ratio > 1.10)
    out["expand_140"]   = out["raw"] & (ratio > 1.40)
    out["expand_any"]   = out["raw"] & (ratio >= 1.00)

    return out
def study(sig: pd.DataFrame, horizons) -> list[dict]:
    close = sig["close"]
    rows = []
    for h in horizons:
        fwd = (close.shift(-h) / close - 1.0) * 100.0
        base = fwd.dropna()
        for f in FILTERS:
            at = fwd[sig[f].fillna(False)].dropna()
            rows.append({
                "filter": f, "horizon": h, "n": len(at),
                "mean_%": at.mean() if len(at) else np.nan,
                "hit_%": (at > 0).mean()*100 if len(at) else np.nan,
                "edge_pp": (at.mean() - base.mean()) if len(at) else np.nan,
                "hit_edge_pp": ((at > 0).mean()*100 - (base > 0).mean()*100) if len(at) else np.nan,
            })
    return rows


# =====================================================================
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
                     params={"apikey": k, "timeseries": y*260}, timeout=30)
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


def synthetic(t, y, seed=0):
    rng = np.random.default_rng(abs(hash(t)) % 2**32 + seed)
    n = y*252
    c = pd.Series(100*np.exp(np.cumsum(rng.normal(0.0004, 0.022, n))),
                  index=pd.bdate_range("2010-01-01", periods=n))
    return pd.DataFrame({"open": c.shift(1).fillna(c.iloc[0]),
                         "high": c*(1+np.abs(rng.normal(0, .008, n))),
                         "low": c*(1-np.abs(rng.normal(0, .008, n))),
                         "close": c})


def to_weekly(df):
    return df.resample("W-FRI").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}).dropna()


def sp500():
    """Load tickers from the local SP500_list.xlsx (Symbol column)."""
    df = pd.read_excel("SP500_list.xlsx")
    return [str(s).strip().replace(".", "-") for s in df["Symbol"].dropna().tolist()]


# =====================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="+",
                    default=["ZETA", "AAOI", "NKE", "SBUX", "META", "GOOGL", "LITE", "OSCR"])
    ap.add_argument("--universe", choices=["sp500"], default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--source", choices=["fmp", "yfinance", "synthetic"], default="yfinance")
    ap.add_argument("--years", type=int, default=15)
    ap.add_argument("--timeframe", choices=["daily", "weekly"], default="weekly")
    ap.add_argument("--benchmark", default="SPY")
    ap.add_argument("--atr-thresh", type=float, default=1.00)
    ap.add_argument("--no-split", action="store_true")
    ap.add_argument("--out", default="dca_filter_results.csv")
    args = ap.parse_args()

    p = P(atr_thresh=args.atr_thresh)
    fetch = {"fmp": fetch_fmp, "yfinance": fetch_yf, "synthetic": synthetic}[args.source]

    bench_daily = None
    if args.source != "synthetic":
        try:
            bench_daily = fetch(args.benchmark, args.years)["close"]
            print(f"benchmark {args.benchmark}: {len(bench_daily)} bars")
        except Exception as e:
            print(f"  !! benchmark unavailable ({e}) — rs_index will be empty")

    tickers = sp500() if args.universe else args.tickers
    if args.limit:
        tickers = tickers[:args.limit]

    rows = []
    for n, tk in enumerate(tickers, 1):
        try:
            daily = fetch(tk, args.years)
        except Exception as e:
            if not args.universe:
                print(f"  !! {tk}: {e}")
            continue
        if args.universe and n % 25 == 0:
            print(f"  ... {n}/{len(tickers)}")

        if args.timeframe == "daily":
            data, hz = daily, (20, 60, 120)
            bench = bench_daily
        else:
            data, hz = to_weekly(daily), (4, 12, 26)
            bench = to_weekly(bench_daily.to_frame("close").assign(
                open=bench_daily, high=bench_daily, low=bench_daily))["close"] if bench_daily is not None else None

        if len(data) < 260:
            continue
        segs = [("full", data)] if args.no_split else [
            ("in_sample", data.iloc[:len(data)//2]),
            ("out_sample", data.iloc[len(data)//2:])]
        for seg_name, seg in segs:
            if len(seg) < 150:
                continue
            sig = build(seg, p, bench)
            for r in study(sig, hz):
                r.update({"ticker": tk, "segment": seg_name})
                rows.append(r)

    if not rows:
        print("no results")
        return

    res = pd.DataFrame(rows)
    res.to_csv(args.out, index=False)
    n_names = res.ticker.nunique()

    for seg, g in res.groupby("segment"):
        print(f"\n{'='*78}\n{seg.upper()}   ({n_names} names)\n{'='*78}")
        for h, gh in g.groupby("horizon"):
            agg = gh.groupby("filter").agg(
                total_signals=("n", "sum"),
                sig_per_name=("n", lambda s: s.sum()/max(n_names, 1)),
                median_edge_pp=("edge_pp", "median"),
                mean_edge_pp=("edge_pp", "mean"),
                median_hit_edge=("hit_edge_pp", "median"),
                names_positive=("edge_pp", lambda s: (s > 0).mean()*100),
            ).reindex(FILTERS)
            print(f"\n  horizon {h} bars")
            print(agg.round(2).to_string())

    if not args.no_split:
        print(f"\n{'='*78}\nVERDICT — out of sample, vs unfiltered 'raw'\n{'='*78}")
        oos = res[res.segment == "out_sample"]
        base = oos[oos["filter"] == "raw"].groupby("horizon")["edge_pp"].median()
        raw_per_name = oos[oos["filter"] == "raw"]["n"].sum() / max(n_names, 1) / len(base)
        print(f"  raw baseline: {raw_per_name:.1f} signals per name, "
              f"edges {', '.join(f'h{h}: {base[h]:+.2f}' for h in base.index)}\n")
        for f in FILTERS:
            if f == "raw":
                continue
            gf = oos[oos["filter"] == f]
            me = gf.groupby("horizon")["edge_pp"].median()
            per_name = gf["n"].sum() / max(n_names, 1) / len(base)
            wins = [h for h in base.index if pd.notna(me.get(h)) and me[h] > base[h]]
            enough = per_name >= 0.40 * raw_per_name
            ok = len(wins) > 1 and enough
            why = "" if ok else (f" [keeps only {per_name/max(raw_per_name,1e-9)*100:.0f}% of raw signals]" if not enough else " [edge not better]")
            print(f"  {f:13s} {per_name:6.1f} sig/name  beats raw at {len(wins)}/{len(base)} "
                  f"-> {'PASSES' if ok else 'fails'}{why}")
            print(f"                {', '.join(f'h{h}: {me.get(h, float(chr(110)+chr(97)+chr(110))) - base[h]:+.2f}' if pd.notna(me.get(h)) else f'h{h}: n/a' for h in base.index)}")

    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
