"""
Permutation test for the Wyckoff-volume filter results.

Five filters vs random subsets of the same size, per ticker:
  atr_100         — validated ATR filter, CONTROL (should reproduce
                    p=0.003/0.013/0.002 on SP500; if not, something is
                    wrong with the script and no other row can be trusted)
  vol_climax_150  — the sturdier climax variant (2.8 sig/name)
  vol_climax_200  — the extreme climax (1.1 sig/name; may be too thin to test)
  vol_dry_090     — supply-exhaustion cell, direction inconsistent across universes
  atr_and_climax  — stacked (only ~0.4 sig/name; also may be too thin)

CRITERION: p < 0.05 at 2+ horizons on the same universe passes.

Usage
    python climax_permutation_test.py --universe sp500 --limit 200 --years 15
    python climax_permutation_test.py --tickers-file disruption_index.csv --years 15
    python climax_permutation_test.py --source synthetic --n-perm 500
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

FILTERS = ["atr_100", "vol_climax_150", "vol_climax_200",
           "vol_dry_090", "atr_and_climax"]
HORIZONS = (4, 12, 26)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="+",
                    default=["ZETA", "AAOI", "NKE", "SBUX", "META", "GOOGL", "LITE", "OSCR"])
    ap.add_argument("--universe", choices=["sp500"], default=None)
    ap.add_argument("--tickers-file", default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--source", choices=["fmp", "yfinance", "synthetic"], default="yfinance")
    ap.add_argument("--years", type=int, default=15)
    ap.add_argument("--n-perm", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", default="climax_permutation_results.csv")
    args = ap.parse_args()

    # Reuse the wyckoff test's fetch + build so filter definitions can't drift
    from wyckoff_volume_test import (
        fetch_yf, fetch_fmp, synthetic, to_weekly, sp500, load_tickers, build, P
    )
    fetch = {"fmp": fetch_fmp, "yfinance": fetch_yf, "synthetic": synthetic}[args.source]
    p = P()

    if args.tickers_file:
        tickers = load_tickers(args.tickers_file)
        print(f"loaded {len(tickers)} tickers from {args.tickers_file}")
    elif args.universe:
        tickers = sp500()
    else:
        tickers = args.tickers
    if args.limit:
        tickers = tickers[:args.limit]

    print(f"fetching {len(tickers)} tickers ({args.years}y weekly) ...")
    store = []
    for n, tk in enumerate(tickers, 1):
        try:
            daily = fetch(tk, args.years)
        except Exception:
            continue
        if (args.universe or args.tickers_file) and n % 50 == 0:
            print(f"  ... {n}/{len(tickers)}")
        w = to_weekly(daily)
        if len(w) < 260:
            continue
        oos = w.iloc[len(w)//2:]
        if len(oos) < 150:
            continue
        sig = build(oos, p, None)
        raw_idx = np.where(sig["raw"].values)[0]
        if len(raw_idx) < 4:
            continue
        c = sig["close"].values
        fwd = {}
        for h in HORIZONS:
            f = np.full(len(c), np.nan)
            f[:-h] = (c[h:] / c[:-h] - 1.0) * 100.0
            fwd[h] = f
        masks = {f: sig[f].values[raw_idx] for f in FILTERS}
        store.append({"ticker": tk, "raw_idx": raw_idx, "fwd": fwd, "masks": masks,
                      "base": {h: np.nanmean(fwd[h]) for h in HORIZONS}})
    if not store:
        print("no data")
        return
    print(f"\n{len(store)} tickers with usable out-of-sample data\n")

    rng = np.random.default_rng(args.seed)
    rows = []

    for f in FILTERS:
        keep_counts = [int(s["masks"][f].sum()) for s in store]
        if sum(keep_counts) == 0:
            print(f"  {f}: no signals across any ticker — skipped (setup too rare)")
            continue

        for h in HORIZONS:
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
                "tickers_used": len(actual),
                "signals": int(sum(keep_counts)),
                "actual_edge_pp": round(actual_edge, 3),
                "null_mean_pp": round(float(null.mean()), 3),
                "null_p95_pp": round(float(np.percentile(null, 95)), 3),
                "p_value": round(p_val, 4),
            })

    if not rows:
        print("no filter produced enough signals to test.")
        return

    res = pd.DataFrame(rows)
    res.to_csv(args.out, index=False)

    print("=" * 88)
    print("ACTUAL FILTER EDGE vs RANDOM SUBSETS OF THE SAME SIZE")
    print("=" * 88)
    print(res.to_string(index=False))

    print("\n" + "=" * 88)
    print("VERDICT")
    print("=" * 88)
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
        print(f"  {f:15s} {v}")
        print(f"                  {ps}")

    # Flag missing filters (produced no signals -> not tested)
    tested_filters = set(res["filter"].unique())
    missing = [f for f in FILTERS if f not in tested_filters]
    if missing:
        print("\n  Not tested (setup too rare to produce signals):")
        for f in missing:
            print(f"    - {f}")

    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
