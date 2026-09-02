"""
Permutation test for the DCA filter results.

THE QUESTION
Three filters improved forward-return edge at all three horizons, and all
three did so while retaining only ~25-32% of the W%R breakout signals.
That pattern is suspicious: if ANY subset of ~30% of signals tends to show
a different (often better) mean than the full set, then the filters found
nothing — the improvement is a property of subsetting, not of the filter.

THE TEST
For each filter, hold its SIGNAL COUNT PER TICKER fixed, but choose which
signals to keep AT RANDOM from that ticker's breakouts. Repeat N times.
That builds the null distribution of "what edge would a filter of this
selectivity produce by chance?"

Then compare the real filter's edge to that distribution:
    p = fraction of random subsets whose edge >= the real filter's edge

  p < 0.05  ->  the filter is selecting genuinely better entries
  p > 0.20  ->  the improvement is explained by selectivity alone
  in between ->  suggestive, not established

Usage
    python dca_permutation_test.py --universe sp500 --limit 200
    python dca_permutation_test.py --tickers-file disruption_index.csv
    python dca_permutation_test.py --n-perm 500 --source synthetic
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

FMP_BASE = "https://financialmodelingprep.com/api/v3"
FILTERS = ["atr_contract", "ema200_up", "higher_low", "rs_index", "atr_and_ema"]

WR_LEN, WR_DEEP, WR_EXIT, WR_LB, MIN_GAP = 14, -80.0, -80.0, 10, 5
ATR_LEN, ATR_BASE, ATR_THRESH = 14, 50, 1.00
EMA_SLOW, SLOPE_LB, PIVOT_LB, RS_LB = 200, 20, 5, 26


def williams_r(df, n):
    hh = df["high"].rolling(n).max()
    ll = df["low"].rolling(n).min()
    return (-100.0 * (hh - df["close"]) / (hh - ll).replace(0, np.nan)).fillna(-50.0)


def atr_pct(df, n):
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(),
                    (df["low"] - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0/n, adjust=False, min_periods=n).mean() / df["close"]


def build(df: pd.DataFrame, bench: pd.Series | None) -> pd.DataFrame:
    out = df.copy()
    c = out["close"]
    out["wr"] = williams_r(out, WR_LEN)
    was_deep = out["wr"].rolling(WR_LB).min() <= WR_DEEP
    rb = ((out["wr"] > WR_EXIT) & (out["wr"].shift(1) <= WR_EXIT) & was_deep).values

    fired = np.zeros(len(out), dtype=bool)
    last = -10**6
    for i in range(len(out)):
        if rb[i] and (i - last) > MIN_GAP:
            fired[i] = True
            last = i
    out["raw"] = fired

    ap = atr_pct(out, ATR_LEN)
    out["_contract"] = (ap / ap.rolling(ATR_BASE).mean()) < ATR_THRESH

    ema = c.ewm(span=EMA_SLOW, adjust=False).mean()
    out["_ema_up"] = ema > ema.shift(SLOPE_LB)

    # one-sided swing-low: pivot known PIVOT_LB bars late (no look-ahead)
    win_min = out["low"].rolling(2*PIVOT_LB + 1, min_periods=2*PIVOT_LB + 1).min()
    piv = (out["low"].shift(PIVOT_LB) == win_min).fillna(False)
    lows = out["low"].shift(PIVOT_LB).where(piv)
    out["_higher_low"] = (lows.ffill() > lows.ffill().shift(1).where(piv).ffill()).fillna(False)

    if bench is not None:
        b = bench.reindex(out.index).ffill()
        out["_rs"] = ((c / c.shift(RS_LB) - 1) > (b / b.shift(RS_LB) - 1)).fillna(False)
    else:
        out["_rs"] = False

    out["atr_contract"] = out["raw"] & out["_contract"]
    out["ema200_up"] = out["raw"] & out["_ema_up"]
    out["higher_low"] = out["raw"] & out["_higher_low"]
    out["rs_index"] = out["raw"] & out["_rs"]
    out["atr_and_ema"] = out["raw"] & out["_contract"] & out["_ema_up"]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="+",
                    default=["ZETA", "AAOI", "NKE", "SBUX", "META", "GOOGL", "LITE", "OSCR"])
    ap.add_argument("--universe", choices=["sp500"], default=None)
    ap.add_argument("--tickers-file", default=None,
                    help="path to a file of tickers (one per line, or comma/space separated)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--source", choices=["fmp", "yfinance", "synthetic"], default="yfinance")
    ap.add_argument("--years", type=int, default=15)
    ap.add_argument("--benchmark", default="SPY")
    ap.add_argument("--n-perm", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", default="dca_permutation_results.csv")
    args = ap.parse_args()

    from dca_filter_test import fetch_yf, fetch_fmp, synthetic, to_weekly, sp500
    from atr_direction_test import load_tickers
    fetch = {"fmp": fetch_fmp, "yfinance": fetch_yf, "synthetic": synthetic}[args.source]

    bench_w = None
    if args.source != "synthetic":
        try:
            bd = fetch(args.benchmark, args.years)
            bench_w = to_weekly(bd)["close"]
        except Exception as e:
            print(f"  !! benchmark unavailable ({e}) — rs_index skipped")

    if args.tickers_file:
        tickers = load_tickers(args.tickers_file)
        print(f"loaded {len(tickers)} tickers from {args.tickers_file}")
    elif args.universe:
        tickers = sp500()
    else:
        tickers = args.tickers
    if args.limit:
        tickers = tickers[:args.limit]

    horizons = (4, 12, 26)
    store = []
    for n, tk in enumerate(tickers, 1):
        try:
            daily = fetch(tk, args.years)
        except Exception:
            continue
        if (args.universe or args.tickers_file) and n % 25 == 0:
            print(f"  ... {n}/{len(tickers)}")
        w = to_weekly(daily)
        if len(w) < 260:
            continue
        oos = w.iloc[len(w)//2:]
        if len(oos) < 150:
            continue
        sig = build(oos, bench_w)
        raw_idx = np.where(sig["raw"].values)[0]
        if len(raw_idx) < 4:
            continue
        c = sig["close"].values
        fwd = {}
        for h in horizons:
            f = np.full(len(c), np.nan)
            f[:-h] = (c[h:] / c[:-h] - 1.0) * 100.0
            fwd[h] = f
        masks = {f: sig[f].values[raw_idx] for f in FILTERS}
        store.append({"ticker": tk, "raw_idx": raw_idx, "fwd": fwd, "masks": masks,
                      "base": {h: np.nanmean(fwd[h]) for h in horizons}})

    if not store:
        print("no data")
        return
    print(f"\n{len(store)} tickers with usable out-of-sample data\n")

    rng = np.random.default_rng(args.seed)
    rows = []

    for f in FILTERS:
        keep_counts = [int(s["masks"][f].sum()) for s in store]
        if sum(keep_counts) == 0:
            print(f"  {f}: no signals, skipped")
            continue

        for h in horizons:
            actual = []
            for s in store:
                m = s["masks"][f]
                if m.sum() == 0:
                    continue
                vals = s["fwd"][h][s["raw_idx"][m]]
                vals = vals[~np.isnan(vals)]
                if len(vals) == 0:
                    continue
                actual.append(np.mean(vals) - s["base"][h])
            if not actual:
                continue
            actual_edge = float(np.median(actual))

            null = np.empty(args.n_perm)
            for p_i in range(args.n_perm):
                per = []
                for s, k in zip(store, keep_counts):
                    if k == 0:
                        continue
                    idx = rng.choice(s["raw_idx"], size=min(k, len(s["raw_idx"])), replace=False)
                    vals = s["fwd"][h][idx]
                    vals = vals[~np.isnan(vals)]
                    if len(vals) == 0:
                        continue
                    per.append(np.mean(vals) - s["base"][h])
                null[p_i] = np.median(per) if per else np.nan

            null = null[~np.isnan(null)]
            p_val = float((null >= actual_edge).mean()) if len(null) else np.nan
            rows.append({
                "filter": f, "horizon": h,
                "signals": int(sum(keep_counts)),
                "actual_edge_pp": round(actual_edge, 3),
                "null_mean_pp": round(float(null.mean()), 3),
                "null_p95_pp": round(float(np.percentile(null, 95)), 3),
                "p_value": round(p_val, 4),
            })

    res = pd.DataFrame(rows)
    res.to_csv(args.out, index=False)

    print("=" * 84)
    print("ACTUAL FILTER EDGE vs RANDOM SUBSETS OF THE SAME SIZE")
    print("=" * 84)
    print(res.to_string(index=False))

    print("\n" + "=" * 84)
    print("VERDICT")
    print("=" * 84)
    for f, g in res.groupby("filter"):
        sig_h = (g.p_value < 0.05).sum()
        marg_h = ((g.p_value >= 0.05) & (g.p_value < 0.20)).sum()
        if sig_h >= 2:
            v = "REAL - selects better entries than chance"
        elif sig_h + marg_h >= 2:
            v = "suggestive, not established"
        else:
            v = "NOT REAL - explained by selectivity alone"
        ps = ", ".join(f"h{int(r.horizon)}: p={r.p_value:.3f}" for r in g.itertuples())
        print(f"  {f:13s} {v}")
        print(f"                {ps}")

    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
