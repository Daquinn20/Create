"""
DCA Matrix V3 — signal comparison backtest.

Tests the question you actually asked: does waiting for confirmation
(teal), or requiring a W%R breakout alongside the state change, produce
better entries than the plain zero-cross?

SIGNALS COMPARED
  orange       state 0 -> 1   spread still below zero, turning up (earliest)
  blue         state -> 2     spread crosses above zero (the old "flip")
  teal         state -> 3     spread above zero AND expanding (confirmation)
  wr_only      W%R exits the -80/-100 zone, nothing else
  combo        state advance (blue or teal) + W%R breakout within N bars

For each: forward returns at 3 horizons vs the unconditional base rate over
the same sample, hit rate, signal count, and bars-from-local-low timing.

THE CRITERION, SET BEFORE LOOKING
  A signal is better than 'blue' only if it beats it OUT OF SAMPLE at more
  than one horizon. In-sample improvement does not count. The script prints
  in-sample and out-of-sample separately and will not aggregate them.

Usage
    python dca_v3_signal_test.py --tickers ZETA AAOI NKE SBUX META GOOGL LITE OSCR
    python dca_v3_signal_test.py --timeframe weekly --years 15
    python dca_v3_signal_test.py --universe sp500 --limit 200
    python dca_v3_signal_test.py --source synthetic          # noise baseline

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
SIGNALS = ["orange", "blue", "teal", "wr_only", "combo"]


@dataclass
class Params:
    wr_len: int = 14
    rsi_len: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_sig: int = 9
    ema_fast: int = 50
    ema_slow: int = 200

    vol_len: int = 50
    z_cap: float = 3.0

    w_wr: float = 40.0
    w_rsi: float = 30.0
    w_macd: float = 30.0

    matrix_source: str = "hybrid"
    mix_w: float = 0.50
    ch_len: int = 3

    fast_len: int = 5          # matrix fast EMA
    slow_len: int = 20         # matrix slow EMA
    sig_len: int = 1
    teal_k: float = 0.35       # blue -> teal threshold, x spread stdev
    state_lb: int = 3

    wr_deep: float = -80.0
    wr_exit: float = -80.0
    wr_deep_lb: int = 10
    align_win: int = 5


# =====================================================================
def wilder_rsi(close, n):
    d = close.diff()
    ag = d.clip(lower=0).ewm(alpha=1/n, adjust=False, min_periods=n).mean()
    al = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False, min_periods=n).mean()
    return (100 - 100 / (1 + ag / al.replace(0, np.nan))).fillna(50.0)


def williams_r(df, n):
    hh = df["high"].rolling(n).max()
    ll = df["low"].rolling(n).min()
    return (-100.0 * (hh - df["close"]) / (hh - ll).replace(0, np.nan)).fillna(-50.0)


def macd_hist(close, f, s, g):
    line = close.ewm(span=f, adjust=False).mean() - close.ewm(span=s, adjust=False).mean()
    return line - line.ewm(span=g, adjust=False).mean()


def clamp(x, lo, hi):
    return np.clip(x, lo, hi)


def build(df: pd.DataFrame, p: Params) -> pd.DataFrame:
    out = df.copy()
    c = out["close"]

    out["wr"] = williams_r(out, p.wr_len)
    out["rsi"] = wilder_rsi(c, p.rsi_len)
    h = macd_hist(c, p.macd_fast, p.macd_slow, p.macd_sig)
    out["ema_slow"] = c.ewm(span=p.ema_slow, adjust=False).mean()

    hv = h.rolling(p.vol_len).std()
    mz = clamp((h / hv.replace(0, np.nan)).fillna(0.0), -p.z_cap, p.z_cap)
    mdir = mz / p.z_cap * 100.0

    def zdiff(s):
        d = s.diff(p.ch_len)
        sd = d.rolling(p.vol_len).std()
        return clamp((d / sd.replace(0, np.nan)).fillna(0.0), -p.z_cap, p.z_cap) / p.z_cap * 100.0

    wr_lvl = clamp((-out["wr"] - 50.0) * 2.0, -100, 100)
    rsi_lvl = clamp((50.0 - out["rsi"]) * 2.0, -100, 100)

    if p.matrix_source == "level":
        a, b, d = wr_lvl, rsi_lvl, -mdir
    elif p.matrix_source == "directional":
        a, b, d = zdiff(out["wr"]), zdiff(out["rsi"]), zdiff(mdir)
    else:
        m = p.mix_w
        a = (1-m)*wr_lvl + m*zdiff(out["wr"])
        b = (1-m)*rsi_lvl + m*zdiff(out["rsi"])
        d = (1-m)*(-mdir) + m*zdiff(mdir)

    tw = p.w_wr + p.w_rsi + p.w_macd
    matrix = (a*p.w_wr + b*p.w_rsi + d*p.w_macd) / tw

    # ---- fast/slow spread, the V3 histogram ----
    spread = (matrix.ewm(span=p.fast_len, adjust=False).mean()
              - matrix.ewm(span=p.slow_len, adjust=False).mean()
              ).ewm(span=p.sig_len, adjust=False).mean()
    out["spread"] = spread

    svol = spread.rolling(p.vol_len).std()
    rising = spread > spread.shift(p.state_lb)

    state = pd.Series(0, index=out.index, dtype=int)
    state[(spread >= 0)] = 2
    state[(spread >= 0) & rising & (spread > p.teal_k * svol)] = 3
    state[(spread < 0) & rising] = 1
    out["state"] = state
    prev = state.shift(1).fillna(0)

    out["orange"] = (state == 1) & (prev == 0)
    out["blue"] = (state == 2) & (prev < 2)
    out["teal"] = (state == 3) & (prev < 3)

    # ---- W%R breakout ----
    was_deep = out["wr"].rolling(p.wr_deep_lb).min() <= p.wr_deep
    out["wr_only"] = (out["wr"] > p.wr_exit) & (out["wr"].shift(1) <= p.wr_exit) & was_deep

    # ---- combo: state advance and W%R breakout within align_win, either order
    adv = out["blue"] | out["teal"]
    adv_recent = adv.rolling(p.align_win + 1, min_periods=1).max().astype(bool)
    wr_recent = out["wr_only"].rolling(p.align_win + 1, min_periods=1).max().astype(bool)
    raw_combo = (adv & wr_recent) | (out["wr_only"] & adv_recent)

    # suppress repeats inside the window
    fire = np.zeros(len(out), dtype=bool)
    last = -10**6
    rc = raw_combo.values
    for i in range(len(out)):
        if rc[i] and (i - last) > p.align_win:
            fire[i] = True
            last = i
    out["combo"] = fire
    return out


# =====================================================================
def study(sig: pd.DataFrame, horizons) -> list[dict]:
    close = sig["close"]
    rows = []
    for h in horizons:
        fwd = (close.shift(-h) / close - 1.0) * 100.0
        base = fwd.dropna()
        for name in SIGNALS:
            at = fwd[sig[name].fillna(False)].dropna()
            rows.append({
                "signal": name,
                "horizon": h,
                "n": len(at),
                "mean_%": at.mean() if len(at) else np.nan,
                "median_%": at.median() if len(at) else np.nan,
                "hit_%": (at > 0).mean() * 100 if len(at) else np.nan,
                "base_mean_%": base.mean(),
                "base_hit_%": (base > 0).mean() * 100,
                "edge_pp": (at.mean() - base.mean()) if len(at) else np.nan,
                "hit_edge_pp": ((at > 0).mean() * 100 - (base > 0).mean() * 100) if len(at) else np.nan,
            })
    return rows


def timing(sig: pd.DataFrame, name: str, window: int = 20) -> float:
    close = sig["close"]
    lows = np.where((close <= close.rolling(window*2+1, center=True).min()).values)[0]
    ev = np.where(sig[name].fillna(False).values)[0]
    if len(lows) == 0 or len(ev) == 0:
        return np.nan
    return float(np.median([e - lows[np.argmin(np.abs(lows - e))] for e in ev]))


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
    import io
    import requests
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    t = pd.read_html(io.StringIO(r.text))[0]
    return [s.replace(".", "-") for s in t["Symbol"].tolist()]


# =====================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="+",
                    default=["ZETA", "AAOI", "NKE", "SBUX", "META", "GOOGL", "LITE", "OSCR"])
    ap.add_argument("--universe", choices=["sp500"], default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--source", choices=["fmp", "yfinance", "synthetic"], default="yfinance")
    ap.add_argument("--years", type=int, default=15)
    ap.add_argument("--timeframe", choices=["daily", "weekly", "both"], default="both")
    ap.add_argument("--fast", type=int, default=5)
    ap.add_argument("--slow", type=int, default=20)
    ap.add_argument("--teal-k", type=float, default=0.35)
    ap.add_argument("--no-split", action="store_true", help="skip the in/out sample split")
    ap.add_argument("--out", default="dca_v3_signals.csv")
    args = ap.parse_args()

    p = Params(fast_len=args.fast, slow_len=args.slow, teal_k=args.teal_k)
    fetch = {"fmp": fetch_fmp, "yfinance": fetch_yf, "synthetic": synthetic}[args.source]

    tickers = sp500() if args.universe else args.tickers
    if args.limit:
        tickers = tickers[:args.limit]
    tfs = ["daily", "weekly"] if args.timeframe == "both" else [args.timeframe]

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

        for tf in tfs:
            data = daily if tf == "daily" else to_weekly(daily)
            hz = (20, 60, 120) if tf == "daily" else (4, 12, 26)
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
                    r.update({"ticker": tk, "tf": tf, "segment": seg_name})
                    rows.append(r)

    if not rows:
        print("no results")
        return

    res = pd.DataFrame(rows)
    res.to_csv(args.out, index=False)

    for (tf, seg), g in res.groupby(["tf", "segment"]):
        print(f"\n{'='*72}\n{tf.upper()} / {seg.upper()}\n{'='*72}")
        for h, gh in g.groupby("horizon"):
            agg = gh.groupby("signal").agg(
                signals=("n", "sum"),
                median_edge_pp=("edge_pp", "median"),
                mean_edge_pp=("edge_pp", "mean"),
                median_hit_edge=("hit_edge_pp", "median"),
                names_positive=("edge_pp", lambda s: (s > 0).mean()*100),
            ).reindex(SIGNALS)
            print(f"\n  horizon {h} bars")
            print(agg.round(2).to_string())

    if not args.no_split:
        print(f"\n{'='*72}\nDOES ANY SIGNAL BEAT 'blue' OUT OF SAMPLE?\n{'='*72}")
        oos = res[res.segment == "out_sample"]
        for tf, g in oos.groupby("tf"):
            print(f"\n  {tf}:")
            base = g[g.signal == "blue"].groupby("horizon")["edge_pp"].median()
            for s in SIGNALS:
                if s == "blue":
                    continue
                me = g[g.signal == s].groupby("horizon")["edge_pp"].median()
                wins = [h for h in base.index if pd.notna(me.get(h)) and me[h] > base[h]]
                verdict = "PASSES" if len(wins) > 1 else "fails"
                deltas = ", ".join(f"h{h}: {me.get(h, float('nan')) - base[h]:+.2f}" for h in base.index)
                print(f"    {s:9s} beats blue at {len(wins)}/{len(base)} horizons -> {verdict}   ({deltas})")

    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
