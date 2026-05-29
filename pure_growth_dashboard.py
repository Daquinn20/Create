"""
Pure Growth Screen - Streamlit dashboard

Universe -> Run Scan -> Table of:
  Ticker | Company | Sector | TTM Rev Growth % | NTM Rev Growth % |
  STM Rev Growth % | TTM P/E | Forward P/E

Data source: FMP only.
  TTM = sum of last 4 reported quarters
  NTM = sum of next 4 quarterly revenue estimates
  STM = sum of forward quarters 5-8
  TTM P/E from key-metrics-ttm
  Forward P/E = price / FY1 EPS estimate
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Pure Growth Screen", page_icon="📈", layout="wide")

FMP_API_KEY = os.getenv("FMP_API_KEY") or (st.secrets.get("FMP_API_KEY") if hasattr(st, "secrets") else None)
FMP_BASE = "https://financialmodelingprep.com/api/v3"

ROOT = Path(__file__).parent
MASTER_UNIVERSE_CSV = ROOT / "master_universe.csv"
SP500_XLSX = ROOT / "SP500_list_with_sectors.xlsx"
DISRUPTION_CSV = ROOT / "disruption_index.csv"
EVOLUTION_FUND_XLSX = Path(
    r"C:\Users\daqui\OneDrive\Documents\Kite Evolution\Evolution Fund DEC 2024.xlsx"
)
EVOLUTION_SHEET = "Fund 2023"


# ---------- Universe loaders ----------

def _read_excel_unlocked(path: Path, **kwargs) -> pd.DataFrame:
    import shutil, tempfile
    try:
        return pd.read_excel(path, **kwargs)
    except PermissionError:
        tmp = Path(tempfile.gettempdir()) / f"_tmp_{path.name}"
        shutil.copy2(path, tmp)
        try:
            return pd.read_excel(tmp, **kwargs)
        finally:
            try:
                tmp.unlink()
            except Exception:
                pass


@st.cache_data(show_spinner=False)
def load_master_universe() -> pd.DataFrame:
    df = pd.read_csv(MASTER_UNIVERSE_CSV, header=None, names=["Ticker", "Name", "Exchange"])
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    df["Sector"] = ""
    return df[["Ticker", "Name", "Sector"]].dropna(subset=["Ticker"]).drop_duplicates("Ticker")


@st.cache_data(show_spinner=False)
def load_sp500() -> pd.DataFrame:
    df = pd.read_excel(SP500_XLSX)
    df = df.rename(columns={"Symbol": "Ticker"})
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    return df[["Ticker", "Name", "Sector"]].dropna(subset=["Ticker"]).drop_duplicates("Ticker")


@st.cache_data(show_spinner=False)
def load_disruption() -> pd.DataFrame:
    df = pd.read_csv(DISRUPTION_CSV)
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    df["Name"] = df["Name"].fillna("")
    df["Sector"] = df["Sector"].fillna("")
    return df[["Ticker", "Name", "Sector"]].dropna(subset=["Ticker"]).drop_duplicates("Ticker")


@st.cache_data(show_spinner=False)
def load_evolution_fund() -> pd.DataFrame:
    fund = _read_excel_unlocked(EVOLUTION_FUND_XLSX, sheet_name=EVOLUTION_SHEET, header=13)
    h = fund[fund["SYMBOL"].notna()].copy()
    h = h[h["SYMBOL"].astype(str).str.strip().str.lower() != "symbol"]
    h["SYMBOL"] = h["SYMBOL"].astype(str).str.strip().str.upper()
    h = h[h["SYMBOL"].str.match(r"^[A-Z]{1,5}$", na=False)]
    h = h[~h["SYMBOL"].isin({"CASH", "DATE"})]
    out = pd.DataFrame({"Ticker": sorted(set(h["SYMBOL"]))})
    out["Name"] = ""
    out["Sector"] = ""
    return out


UNIVERSES = {
    "S&P 500": load_sp500,
    "Disruption Index": load_disruption,
    "Master Universe": load_master_universe,
}
if EVOLUTION_FUND_XLSX.exists():
    UNIVERSES["Evolution Fund (Dec 2024)"] = load_evolution_fund


# ---------- FMP fetchers ----------

def _fmp_get(path: str, params: dict | None = None) -> list | dict | None:
    if not FMP_API_KEY:
        return None
    params = dict(params or {})
    params["apikey"] = FMP_API_KEY
    try:
        r = requests.get(f"{FMP_BASE}/{path}", params=params, timeout=15)
        if r.status_code != 200:
            return None
        return r.json()
    except requests.RequestException:
        return None


def _pct(curr, prev) -> float | None:
    if curr is None or prev is None:
        return None
    try:
        curr = float(curr); prev = float(prev)
    except (TypeError, ValueError):
        return None
    if prev <= 0:
        return None
    return round((curr / prev - 1) * 100, 2)


def fetch_growth_row(ticker: str) -> dict:
    """One ticker: TTM/NTM/STM rev growth + TTM P/E + Forward P/E."""
    out = {
        "Ticker": ticker,
        "TTM Rev Growth %": None,
        "NTM Rev Growth %": None,
        "STM Rev Growth %": None,
        "TTM P/E": None,
        "Forward P/E": None,
    }

    # Quarterly actuals (need last 8 to compute TTM and prior-TTM)
    quarters = _fmp_get(f"income-statement/{ticker}", {"period": "quarter", "limit": 8})
    if isinstance(quarters, list) and len(quarters) >= 8:
        quarters_sorted = sorted(quarters, key=lambda x: x.get("date", ""), reverse=True)
        last4 = quarters_sorted[:4]
        prior4 = quarters_sorted[4:8]
        ttm_rev = sum(q.get("revenue") or 0 for q in last4)
        prior_ttm_rev = sum(q.get("revenue") or 0 for q in prior4)
        out["TTM Rev Growth %"] = _pct(ttm_rev, prior_ttm_rev)
    else:
        ttm_rev = None

    # Forward quarterly estimates
    fwd = _fmp_get(f"analyst-estimates/{ticker}", {"period": "quarter", "limit": 12})
    ntm_rev = stm_rev = None
    if isinstance(fwd, list) and fwd:
        fwd_sorted = sorted(fwd, key=lambda x: x.get("date", ""))
        last_actual_date = quarters_sorted[0].get("date", "") if isinstance(quarters, list) and quarters else ""
        future = [e for e in fwd_sorted if e.get("date", "") > last_actual_date]
        if len(future) >= 4:
            ntm_rev = sum((e.get("estimatedRevenueAvg") or 0) for e in future[:4])
            out["NTM Rev Growth %"] = _pct(ntm_rev, ttm_rev) if ttm_rev else None
        if len(future) >= 8:
            stm_rev = sum((e.get("estimatedRevenueAvg") or 0) for e in future[4:8])
            out["STM Rev Growth %"] = _pct(stm_rev, ntm_rev) if ntm_rev else None

    # TTM P/E
    km = _fmp_get(f"key-metrics-ttm/{ticker}", {"limit": 1})
    if isinstance(km, list) and km:
        pe = km[0].get("peRatioTTM")
        if pe is not None:
            try:
                out["TTM P/E"] = round(float(pe), 2)
            except (TypeError, ValueError):
                pass

    # Forward P/E = price / FY1 EPS estimate
    fy = _fmp_get(f"analyst-estimates/{ticker}", {"period": "annual", "limit": 2})
    fy1_eps = None
    if isinstance(fy, list) and fy:
        fy_sorted = sorted(fy, key=lambda x: x.get("date", ""))
        future_annual = [e for e in fy_sorted if e.get("date", "") > (quarters_sorted[0].get("date", "") if isinstance(quarters, list) and quarters else "")]
        if future_annual:
            fy1_eps = future_annual[0].get("estimatedEpsAvg")

    if fy1_eps:
        quote = _fmp_get(f"quote/{ticker}")
        if isinstance(quote, list) and quote:
            price = quote[0].get("price")
            try:
                if price and float(fy1_eps) > 0:
                    out["Forward P/E"] = round(float(price) / float(fy1_eps), 2)
            except (TypeError, ValueError):
                pass

    return out


def run_scan(tickers: list[str], workers: int, progress, status) -> pd.DataFrame:
    rows = []
    total = len(tickers)
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_growth_row, t): t for t in tickers}
        for fut in as_completed(futures):
            try:
                rows.append(fut.result())
            except Exception:
                rows.append({"Ticker": futures[fut]})
            done += 1
            if done % 10 == 0 or done == total:
                progress.progress(done / total)
                status.text(f"Scanned {done}/{total}")
    return pd.DataFrame(rows)


# ---------- UI ----------

st.title("📈 Pure Growth Screen")
st.caption("TTM / NTM / STM revenue growth with TTM and forward P/E. Data source: FMP.")

if not FMP_API_KEY:
    st.error("FMP_API_KEY not set in .env or Streamlit secrets.")
    st.stop()

col_a, col_b = st.columns([2, 1])
with col_a:
    universe_name = st.radio("Universe", list(UNIVERSES.keys()), horizontal=True)
with col_b:
    workers = st.slider("Parallel workers", 1, 30, 12)

try:
    universe_df = UNIVERSES[universe_name]()
except Exception as e:
    st.error(f"Failed to load {universe_name}: {e}")
    st.stop()

st.write(f"**{len(universe_df)}** tickers in {universe_name}.")

if universe_name == "Master Universe" and len(universe_df) > 2000:
    st.warning(
        f"Master Universe has {len(universe_df):,} tickers. Scan will take a while and consume FMP credits."
    )

if st.button("Run Scan", type="primary"):
    tickers = universe_df["Ticker"].tolist()
    progress = st.progress(0.0)
    status = st.empty()
    results = run_scan(tickers, workers, progress, status)

    # Merge Name + Sector from universe
    results = results.merge(universe_df, on="Ticker", how="left")
    results = results[[
        "Ticker", "Name", "Sector",
        "TTM Rev Growth %", "NTM Rev Growth %", "STM Rev Growth %",
        "TTM P/E", "Forward P/E",
    ]]
    st.session_state["scan_results"] = results
    st.session_state["scan_universe"] = universe_name

if "scan_results" in st.session_state:
    st.subheader(f"Results — {st.session_state['scan_universe']}")
    st.dataframe(st.session_state["scan_results"], use_container_width=True, hide_index=True)
