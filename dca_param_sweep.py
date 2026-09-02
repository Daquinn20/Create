"""
DCA Matrix — parameter stability sweep with permutation testing.

The ATR-contraction filter passed a permutation test at the DEFAULT
parameters (wrDeep=-80, wrExit=-80, wrDeepLB=10, atr_thresh=1.00).
Two things remain unverified:

  1. The recommended rule uses TUNED W%R params (-85 / -80 / 20) and
     atr_thresh 0.85. Those were never permutation-tested.
  2. Whether the effect is STABLE across nearby parameter values, or
     only appears at one lucky combination.

This script runs the permutation test across a grid and reports p-values
for every cell, so you can see the shape of the result rather than a
single number.

READ IT THIS WAY
  - If p stays below ~0.05 across most of the grid, the effect is robust
    and you can pick parameters on other grounds (signal frequency, say).
  - If only one or two cells are significant and neighbours are not, you
    found a lucky threshold. That is the same failure mode that made
    'orange' look like the best signal on 8 tickers.

Put this in the same folder as dca_filter_test.py (it imports the
data-fetching helpers from there).

Usage
    python dca_param_sweep.py --universe sp500 --limit 200
    python dca_param_sweep.py --universe sp500 --limit 200 --n-perm 500
    python dca_param_sweep.py --source synthetic --n-perm 200
"""

from __future__ import annotations

import argparse
import itertools

import numpy as np
import pandas as pd

HORIZONS = (4, 12, 26)


def williams_r(df, n):
    hh = df["high"].rolling(n).max()
    ll = df["low"].rolling(n).min()
    return (-100.0 * (hh - df["close"]) / (hh - ll).replace(0, np.nan)).fillna(-50.0)


def atr_pct(df, n):
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(),
                    (df["low"] - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0/n, adjust=False, min_periods=n).mean() / df["close"]


def breakouts(df, wr_len, wr_deep, wr_exit, wr_lb, min_gap=5):
    wr = williams_r(df, wr_len)
    was_deep = wr.rolling(wr_lb).min() <= wr_deep
    rb = ((wr > wr_exit) & (wr.shift(1) <= wr_exit) & was_deep).values
    fired = np.zeros(len(df), dtype=bool)
    last = -10**6
    for i in range(len(df)):
        if rb[i] and (i - last) > min_gap:
            fired[i] = True
            last = i
    return fired


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="+",
                    default=["ZETA", "AAOI", "NKE", "SBUX", "META", "GOOGL", "LITE", "OSCR"])
    ap.add_argument("--universe", choices=["sp500"], default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--source", choices=["fmp", "yfinance", "synthetic"], default="yfinance")
    ap.add_argument("--years", type=int, default=15)
    ap.add_argument("--n-perm", type=int, default=500)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--atr-len", type=int, default=14)
    ap.add_argument("--atr-base", type=int, default=50)
    ap.add_argument("--out", default="dca_param_sweep.csv")
    args = ap.parse_args()

    from dca_filter_test import fetch_yf, fetch_fmp, synthetic, to_weekly, sp500
    fetch = {"fmp": fetch_fmp, "yfinance": fetch_yf, "synthetic": synthetic}[args.source]

    # ---- the grid ----
    WR_DEEP  = [-90, -85, -80, -75]
    WR_EXIT  = [-80, -70, -60]
    WR_LB    = [10, 20]
    ATR_TH   = [0.75, 0.85, 1.00, 1.15]

    tickers = sp500() if args.universe else args.tickers
    if args.limit:
        tickers = tickers[:args.limit]

    # ---- load once, reuse for every grid cell ----
    print("loading data ...")
    panels = []
    for n, tk in enumerate(tickers, 1):
        try:
            daily = fetch(tk, args.years)
        except Exception:
            continue
        if args.universe and n % 50 == 0:
            print(f"  ... {n}/{len(tickers)}")
        w = to_weekly(daily)
        if len(w) < 260:
            continue
        oos = w.iloc[len(w)//2:]
        if len(oos) < 150:
            continue
        c = oos["close"].values
        fwd = {}
        for h in HORIZONS:
            f = np.full(len(c), np.nan)
            f[:-h] = (c[h:] / c[:-h] - 1.0) * 100.0
            fwd[h] = f
        ap_ = atr_pct(oos, args.atr_len)
        contraction = (ap_ / ap_.rolling(args.atr_base).mean()).values
        panels.append({"ticker": tk, "df": oos, "fwd": fwd, "contract": contraction,
                       "base": {h: np.nanmean(fwd[h]) for h in HORIZONS}})
    print(f"{len(panels)} tickers loaded\n")
    if not panels:
        print("no data")
        return

    rng = np.random.default_rng(args.seed)
    rows = []
    grid = list(itertools.product(WR_DEEP, WR_EXIT, WR_LB, ATR_TH))
    print(f"sweeping {len(grid)} parameter combinations x {args.n_perm} permutations ...\n")

    # cache breakouts per (deep, exit, lb) so the ATR loop is cheap
    bo_cache: dict[tuple, list] = {}

    for gi, (deep, exitv, lb, th) in enumerate(grid, 1):
        if exitv < deep:          # exit must be at or above the deep zone
            continue
        key = (deep, exitv, lb)
        if key not in bo_cache:
            bo_cache[key] = [np.where(breakouts(p["df"], 14, deep, exitv, lb))[0]
                             for p in panels]
        raw_list = bo_cache[key]

        for h in HORIZONS:
            actual, keep_counts, usable = [], [], []
            for p, raw_idx in zip(panels, raw_list):
                if len(raw_idx) < 3:
                    continue
                mask = p["contract"][raw_idx] < th
                k = int(mask.sum())
                if k == 0:
                    continue
                vals = p["fwd"][h][raw_idx[mask]]
                vals = vals[~np.isnan(vals)]
                if len(vals) == 0:
                    continue
                actual.append(float(np.mean(vals) - p["base"][h]))
                keep_counts.append(k)
                usable.append((p, raw_idx))
            if len(actual) < max(4, len(panels) // 3):
                continue
            actual_edge = float(np.median(actual))

            null = np.empty(args.n_perm)
            for pi in range(args.n_perm):
                per = []
                for (p, raw_idx), k in zip(usable, keep_counts):
                    idx = rng.choice(raw_idx, size=min(k, len(raw_idx)), replace=False)
                    vals = p["fwd"][h][idx]
                    vals = vals[~np.isnan(vals)]
                    if len(vals):
                        per.append(float(np.mean(vals) - p["base"][h]))
                null[pi] = np.median(per) if per else np.nan
            null = null[~np.isnan(null)]
            if not len(null):
                continue

            rows.append({
                "wr_deep": deep, "wr_exit": exitv, "wr_lb": lb, "atr_th": th,
                "horizon": h,
                "names": len(actual),
                "sig_per_name": round(float(np.mean(keep_counts)), 2),
                "edge_pp": round(actual_edge, 3),
                "null_p95": round(float(np.percentile(null, 95)), 3),
                "p_value": round(float((null >= actual_edge).mean()), 4),
            })

        if gi % 8 == 0:
            print(f"  ... {gi}/{len(grid)} cells")

    if not rows:
        print("no usable cells")
        return

    res = pd.DataFrame(rows)
    res.to_csv(args.out, index=False)

    print("\n" + "=" * 90)
    print("P-VALUE BY PARAMETER COMBINATION (lower = filter beats random subsets of same size)")
    print("=" * 90)
    for h in HORIZONS:
        g = res[res.horizon == h]
        if g.empty:
            continue
        piv = g.pivot_table(index=["wr_deep", "wr_exit", "wr_lb"],
                            columns="atr_th", values="p_value")
        print(f"\n  horizon {h} bars   (columns = ATR threshold)")
        print(piv.round(3).to_string())

    print("\n" + "=" * 90)
    print("SIGNALS PER NAME (same layout) - a cell with <2 is too thin to trust")
    print("=" * 90)
    g = res[res.horizon == HORIZONS[-1]]
    piv = g.pivot_table(index=["wr_deep", "wr_exit", "wr_lb"],
                        columns="atr_th", values="sig_per_name")
    print(piv.round(1).to_string())

    print("\n" + "=" * 90)
    print("STABILITY")
    print("=" * 90)
    frac = (res.p_value < 0.05).mean() * 100
    print(f"  {frac:.0f}% of all parameter/horizon cells are significant at p<0.05")
    per_cell = res.groupby(["wr_deep", "wr_exit", "wr_lb", "atr_th"])["p_value"].apply(
        lambda s: (s < 0.05).sum())
    robust = per_cell[per_cell >= 2]
    print(f"  {len(robust)} of {len(per_cell)} combinations significant at 2+ horizons")
    if frac > 60:
        print("\n  -> Broad significance across the grid. The effect is not a lucky threshold;")
        print("     pick parameters on signal frequency or preference.")
    elif frac > 25:
        print("\n  -> Mixed. Significant in part of the grid. Check whether the significant")
        print("     cells form a contiguous region (real) or are scattered (noise).")
    else:
        print("\n  -> Sparse significance. Consistent with a lucky threshold rather than a")
        print("     robust effect. Treat the earlier single-point result with caution.")

    best = res[res.p_value < 0.05].sort_values("edge_pp", ascending=False).head(12)
    if not best.empty:
        print("\n  strongest significant cells (do NOT just pick the top one - that is tuning):")
        print(best.to_string(index=False))

    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
