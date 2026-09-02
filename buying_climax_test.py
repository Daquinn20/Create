"""
DCA Matrix — buying-climax test (inverted).

Hypothesis: W%R sitting near the TOP of its range PLUS a volume spike
should predict WEAKNESS. This is the opposite of the PANIC bottom-side
finding — buying climax at the top vs capitulation climax at the bottom.

Five variants + baseline:
  raw            any bar with W%R > -10, no volume condition (baseline)
  top10_vol150   W%R > -10 AND volume > 1.5x average
  top10_vol200   W%R > -10 AND volume > 2.0x average
  top20_vol150   W%R > -20 AND volume > 1.5x average
  top20_vol200   W%R > -20 AND volume > 2.0x average
  top10_quiet    W%R > -10 AND volume < 0.9x average (control — is it
                 the top-of-range or the volume doing the work?)

VERDICT (inverted for this test):
  PREDICTS WEAKNESS if the variant is:
    - negative at 2+ horizons AND
    - worse than the top-of-range baseline at 2+ horizons AND
    - retains at least 40% of baseline signals
  If top10_quiet is ALSO negative, the top-of-range position is doing
  the work, not the volume spike.

Usage
    python buying_climax_test.py --universe sp500 --limit 200 --timeframe weekly
    python buying_climax_test.py --tickers-file disruption_index.csv --label disruption
    python buying_climax_test.py --source synthetic
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd

FMP_BASE = "https://financialmodelingprep.com/api/v3"
FILTERS = ["raw", "top10_vol150", "top10_vol200",
           "top20_vol150", "top20_vol200", "top10_quiet"]


@dataclass
class P:
    wr_len: int = 14
    min_gap: int = 5
    vol_base: int = 50


# =====================================================================
def williams_r(df, n):
    hh = df["high"].rolling(n).max()
    ll = df["low"].rolling(n).min()
    return (-100.0 * (hh - df["close"]) / (hh - ll).replace(0, np.nan)).fillna(-50.0)


def build(df: pd.DataFrame, p: P) -> pd.DataFrame:
    """Buying-climax hypothesis: W%R near the TOP of its range with a
    volume spike should predict WEAKNESS, not strength."""
    out = df.copy()
    out["wr"] = williams_r(out, p.wr_len)
    vol = out["volume"].replace(0, np.nan)
    vol_ratio = vol / vol.rolling(p.vol_base).mean()

    def spaced(mask):
        m = mask.values
        fired = np.zeros(len(out), dtype=bool)
        last = -10**6
        for i in range(len(out)):
            if m[i] and (i - last) > p.min_gap:
                fired[i] = True
                last = i
        return fired

    top10 = out["wr"] > -10.0
    top20 = out["wr"] > -20.0
    out["raw"] = spaced(top10)

    out["top10_vol150"] = spaced(top10 & (vol_ratio > 1.50))
    out["top10_vol200"] = spaced(top10 & (vol_ratio > 2.00))
    out["top20_vol150"] = spaced(top20 & (vol_ratio > 1.50))
    out["top20_vol200"] = spaced(top20 & (vol_ratio > 2.00))
    out["top10_quiet"]  = spaced(top10 & (vol_ratio < 0.90))
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
    return df[["open", "high", "low", "close", "volume"]].astype(float)


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
    adj = ["adjOpen", "adjHigh", "adjLow", "adjClose", "volume"]
    if all(x in df.columns for x in adj):
        df = df[adj]
        df.columns = ["open", "high", "low", "close", "volume"]
    else:
        df = df[["open", "high", "low", "close", "volume"]]
    return df.astype(float)


def synthetic(t, y, seed=0):
    rng = np.random.default_rng(abs(hash(t)) % 2**32 + seed)
    n = y*252
    c = pd.Series(100*np.exp(np.cumsum(rng.normal(0.0004, 0.022, n))),
                  index=pd.bdate_range("2010-01-01", periods=n))
    return pd.DataFrame({"open": c.shift(1).fillna(c.iloc[0]),
                         "high": c*(1+np.abs(rng.normal(0, .008, n))),
                         "low": c*(1-np.abs(rng.normal(0, .008, n))),
                         "close": c,
                         "volume": pd.Series(rng.lognormal(14, .5, n), index=c.index)})


def to_weekly(df):
    return df.resample("W-FRI").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last",
         "volume": "sum"}).dropna()


def load_tickers(path):
    if not os.path.exists(path):
        sys.exit(f"ticker file not found: {path}")
    raw = open(path).read()
    out = []
    if path.lower().endswith(".csv"):
        for row in csv.reader(raw.splitlines()):
            if row and row[0].strip() and not row[0].strip().startswith("#"):
                out.append(row[0].strip())
        if out and out[0].lower() in ("ticker", "symbol", "tickers", "symbols"):
            out = out[1:]
    else:
        for line in raw.splitlines():
            line = line.split("#")[0].strip()
            if not line:
                continue
            for tok in line.replace(",", " ").split():
                out.append(tok.strip())
    seen, uniq = set(), []
    for t in out:
        t = t.upper().replace(".", "-")
        if t and t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


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
    ap.add_argument("--tickers-file", default=None)
    ap.add_argument("--label", default="universe")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--source", choices=["fmp", "yfinance", "synthetic"], default="yfinance")
    ap.add_argument("--years", type=int, default=15)
    ap.add_argument("--timeframe", choices=["daily", "weekly"], default="weekly")
    ap.add_argument("--no-split", action="store_true")
    ap.add_argument("--out", default="buying_climax_results.csv")
    args = ap.parse_args()

    p = P()
    fetch = {"fmp": fetch_fmp, "yfinance": fetch_yf, "synthetic": synthetic}[args.source]

    if args.tickers_file:
        tickers = load_tickers(args.tickers_file)
        print(f"loaded {len(tickers)} tickers from {args.tickers_file}")
    elif args.universe:
        tickers = sp500()
    else:
        tickers = args.tickers
    if args.limit:
        tickers = tickers[:args.limit]

    rows = []
    for n, tk in enumerate(tickers, 1):
        try:
            daily = fetch(tk, args.years)
        except Exception as e:
            if not (args.universe or args.tickers_file):
                print(f"  !! {tk}: {e}")
            continue
        if (args.universe or args.tickers_file) and n % 25 == 0:
            print(f"  ... {n}/{len(tickers)}")

        if args.timeframe == "daily":
            data, hz = daily, (20, 60, 120)
        else:
            data, hz = to_weekly(daily), (4, 12, 26)

        if len(data) < 260:
            continue
        segs = [("full", data)] if args.no_split else [
            ("in_sample", data.iloc[:len(data)//2]),
            ("out_sample", data.iloc[len(data)//2:])]
        for seg_name, seg in segs:
            if len(seg) < 150:
                continue
            sig = build(seg, p)
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
        print(f"\n{'='*78}\n{args.label.upper()} / {seg.upper()}   ({n_names} names)\n{'='*78}")
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
        print(f"\n{'='*78}\nVERDICT - does the setup predict WEAKNESS? (looking for NEGATIVE edge)\n{'='*78}")
        oos = res[res.segment == "out_sample"]
        base = oos[oos["filter"] == "raw"].groupby("horizon")["edge_pp"].median()
        raw_per_name = oos[oos["filter"] == "raw"]["n"].sum() / max(n_names, 1) / len(base)
        print(f"  baseline (top of range, any volume): {raw_per_name:.1f} sig/name, "
              f"{', '.join(f'h{h}: {base[h]:+.2f}' for h in base.index)}\n")
        for f in FILTERS:
            if f == "raw":
                continue
            gf = oos[oos["filter"] == f]
            me = gf.groupby("horizon")["edge_pp"].median()
            per_name = gf["n"].sum() / max(n_names, 1) / len(base)
            neg = [h for h in base.index if pd.notna(me.get(h)) and me[h] < 0]
            worse = [h for h in base.index if pd.notna(me.get(h)) and me[h] < base[h]]
            enough = per_name >= 0.40 * raw_per_name
            ok = len(neg) >= 2 and len(worse) >= 2 and enough
            note = "" if ok else (f" [keeps only {per_name/max(raw_per_name,1e-9)*100:.0f}% of baseline]"
                                  if not enough else " [not consistently negative]")
            print(f"  {f:14s} {per_name:6.1f} sig/name  negative at {len(neg)}/{len(base)}, "
                  f"worse than baseline at {len(worse)}/{len(base)} -> "
                  f"{'PREDICTS WEAKNESS' if ok else 'no'}{note}")
            print(f"                 {', '.join(f'h{h}: {me.get(h):+.2f}' if pd.notna(me.get(h)) else f'h{h}: n/a' for h in base.index)}")

    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
