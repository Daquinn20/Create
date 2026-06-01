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
import smtplib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Pure Growth Screen", page_icon="📈", layout="wide")

def _secret(name: str) -> str | None:
    v = os.getenv(name)
    if v:
        return v
    try:
        return st.secrets.get(name)
    except Exception:
        return None


FMP_API_KEY = _secret("FMP_API_KEY")
EMAIL_ADDRESS = _secret("EMAIL_ADDRESS")
EMAIL_PASSWORD = _secret("EMAIL_PASSWORD")
DEFAULT_RECIPIENT = "daquinn@targetedequityconsulting.com"
FMP_BASE = "https://financialmodelingprep.com/api/v3"

ROOT = Path(__file__).parent
MASTER_UNIVERSE_CSV = ROOT / "master_universe.csv"
SP500_XLSX = ROOT / "SP500_list_with_sectors.xlsx"
DISRUPTION_CSV = ROOT / "disruption_index.csv"
GICS_XLSX = ROOT / "BLOOMBERG Tickers GICS.xlsx"
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


# Manual sector overrides for tickers whose Sector cell is blank in the GICS file.
MANUAL_GICS_OVERRIDES: dict[str, str] = {
    "BF.B": "Consumer Staples",      # Brown-Forman Corp Class B
    "NOVO-B.CO": "Health Care",      # Novo Nordisk
    "GBTC": "Financials",            # Grayscale Bitcoin Trust
}


@st.cache_data(show_spinner=False)
def load_gics_sectors() -> dict[str, str]:
    """Symbol -> Sector lookup from BLOOMBERG Tickers GICS.xlsx + manual overrides."""
    if not GICS_XLSX.exists():
        return dict(MANUAL_GICS_OVERRIDES)
    out: dict[str, str] = {}
    try:
        df1 = pd.read_excel(GICS_XLSX, sheet_name="Bloomberg_Tickers_Full")
        for _, r in df1.iterrows():
            sym = str(r.get("Symbol", "")).strip().upper()
            sec = r.get("Sector")
            if sym and pd.notna(sec) and sec != "SECTOR":
                out[sym] = str(sec)
    except Exception:
        pass
    try:
        df2 = pd.read_excel(GICS_XLSX, sheet_name="Foglio1")
        for _, r in df2.iterrows():
            raw = r.get("SYMBOL")
            if pd.isna(raw):
                continue
            sym = str(raw).strip().split()[0].upper()
            sec = r.get("SECTOR")
            if pd.notna(sec) and sym not in out:
                out[sym] = str(sec)
    except Exception:
        pass
    # Manual overrides always win (covers tickers whose GICS cells are blank).
    out.update(MANUAL_GICS_OVERRIDES)
    return out


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
    """One ticker: name + TTM/NTM/STM rev growth + TTM P/E + FY1/FY2 P/E."""
    out = {
        "Ticker": ticker,
        "Name_FMP": None,
        "TTM Rev Growth %": None,
        "NTM Rev Growth %": None,
        "STM Rev Growth %": None,
        "TTM P/E": None,
        "FY1 P/E": None,
        "FY1 End": None,
        "FY2 P/E": None,
        "FY2 End": None,
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

    # FY1 / FY2 P/E = price / first-future and second-future annual EPS estimates.
    fy = _fmp_get(f"analyst-estimates/{ticker}", {"period": "annual", "limit": 8})
    fy1_eps = fy2_eps = None
    if isinstance(fy, list) and fy:
        fy_sorted = sorted(fy, key=lambda x: x.get("date", ""))
        last_actual_date = quarters_sorted[0].get("date", "") if isinstance(quarters, list) and quarters else ""
        future_annual = [e for e in fy_sorted if e.get("date", "") > last_actual_date]
        if len(future_annual) >= 1:
            fy1_eps = future_annual[0].get("estimatedEpsAvg")
            out["FY1 End"] = future_annual[0].get("date")
        if len(future_annual) >= 2:
            fy2_eps = future_annual[1].get("estimatedEpsAvg")
            out["FY2 End"] = future_annual[1].get("date")

    quote = _fmp_get(f"quote/{ticker}")
    if isinstance(quote, list) and quote:
        out["Name_FMP"] = quote[0].get("name")
        price = quote[0].get("price")
        for key, eps in (("FY1 P/E", fy1_eps), ("FY2 P/E", fy2_eps)):
            try:
                if price and eps and float(eps) > 0:
                    out[key] = round(float(price) / float(eps), 2)
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


# ---------- Email ----------

def send_results_email(df: pd.DataFrame, universe: str, recipient: str) -> tuple[bool, str]:
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        return False, "EMAIL_ADDRESS / EMAIL_PASSWORD not configured."

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        df.to_excel(xw, sheet_name="Pure Growth", index=False)
    buf.seek(0)

    msg = MIMEMultipart()
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = recipient
    msg["Subject"] = f"Pure Growth Screen — {universe} — {stamp}"
    body = (
        f"Pure Growth Screen results for {universe}.\n"
        f"Run: {stamp}\n"
        f"Rows: {len(df)}\n"
        f"Columns: Ticker, Name, Sector, TTM/NTM/STM Rev Growth %, TTM P/E, Forward P/E\n"
    )
    msg.attach(MIMEText(body, "plain"))

    attach = MIMEBase("application", "octet-stream")
    attach.set_payload(buf.read())
    encoders.encode_base64(attach)
    filename = f"pure_growth_{universe.replace(' ', '_').replace('&','and')}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    attach.add_header("Content-Disposition", f"attachment; filename={filename}")
    msg.attach(attach)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            s.send_message(msg)
        return True, f"Sent to {recipient}."
    except Exception as e:
        return False, f"SMTP error: {e}"


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

    # Merge universe Name/Sector + GICS sector lookup
    gics = load_gics_sectors()
    results = results.merge(universe_df, on="Ticker", how="left")
    # Prefer FMP company name; fall back to universe Name
    results["Name"] = results["Name_FMP"].fillna(results["Name"]).fillna("")
    # Prefer GICS sector; fall back to universe Sector.
    # Try exact match, then strip Yahoo-style .SUFFIX (e.g. ABBN.SW -> ABBN, BRK.B -> BRK)
    def _gics_sector(t):
        if t in gics:
            return gics[t]
        if "." in t:
            stem = t.split(".")[0]
            if stem in gics:
                return gics[stem]
        return None
    results["Sector"] = results["Ticker"].map(_gics_sector).fillna(results["Sector"]).fillna("")
    results = results[[
        "Ticker", "Name", "Sector",
        "TTM Rev Growth %", "NTM Rev Growth %", "STM Rev Growth %",
        "TTM P/E", "FY1 P/E", "FY1 End", "FY2 P/E", "FY2 End",
    ]]
    st.session_state["scan_results"] = results
    st.session_state["scan_universe"] = universe_name

if "scan_results" in st.session_state:
    st.subheader(f"Results — {st.session_state['scan_universe']}")
    st.dataframe(st.session_state["scan_results"], use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("**Email results**")
    e_col1, e_col2 = st.columns([3, 1])
    with e_col1:
        recipient = st.text_input("Recipient", value=DEFAULT_RECIPIENT, label_visibility="collapsed")
    with e_col2:
        if st.button("📧 Email Results"):
            ok, msg = send_results_email(
                st.session_state["scan_results"],
                st.session_state["scan_universe"],
                recipient,
            )
            (st.success if ok else st.error)(msg)
