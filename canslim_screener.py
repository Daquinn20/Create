"""
CANSLIM Stock Screener - Streamlit Dashboard
High-Quality Growth Stock Screener Based on O'Neil Methodology
Targeted Equity Consulting Group
"""

import streamlit as st
import os
import sys
import time
import json
import hashlib
import threading
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from io import BytesIO

# Load environment variables
load_dotenv()
FMP_KEY = os.getenv('FMP_API_KEY')

# Try to get from Streamlit secrets if not in env
if not FMP_KEY:
    try:
        FMP_KEY = st.secrets.get("FMP_API_KEY")
    except:
        pass

BASE_URL = "https://financialmodelingprep.com/api"

# Stock Universe Files - OneDrive local sync path
ONEDRIVE_DATA_PATH = Path(r"C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\AI dashboard Data")
ONEDRIVE_INDEX_PATH = Path(r"C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\INDEXES")
INDEX_FILE = ONEDRIVE_DATA_PATH / "Index_Broad_US.xlsx"
SP500_FILE = ONEDRIVE_DATA_PATH / "SP500_list_with_sectors.xlsx"
DISRUPTION_FILE = ONEDRIVE_DATA_PATH / "Disruption Index.xlsx"
NASDAQ100_FILE = ONEDRIVE_DATA_PATH / "NASDAQ100_LIST.xlsx"
RUSSELL2000_FILE = Path(r"C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\INDEXES\Russell_2000_index_dec 2025.xlsx")

# Page config MUST be first Streamlit command
st.set_page_config(
    page_title="CANSLIM Screener - TECG",
    page_icon="",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .main-title {
        text-align: center;
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .subtitle {
        text-align: center;
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        text-align: center;
        color: white;
        margin-bottom: 1rem;
    }
    .pass-cell {
        background-color: #c6efce !important;
        color: #375623 !important;
        font-weight: bold;
    }
    .fail-cell {
        background-color: #ffc7ce !important;
        color: #9c0006 !important;
    }
    .score-6 {
        background-color: #00b050 !important;
        color: white !important;
        font-weight: bold;
    }
    .score-5 {
        background-color: #92d050 !important;
        color: #1a4a00 !important;
        font-weight: bold;
    }
    .score-4 {
        background-color: #ffeb9c !important;
        color: #7b6c00 !important;
    }
    </style>
""", unsafe_allow_html=True)


# ============================================================================
# SETTINGS
# ============================================================================
DEFAULT_SETTINGS = {
    'C_min_qtrly_eps_growth': 25,
    'C_min_qtrly_rev_growth': 20,
    'A_min_eps_cagr_3yr': 25,
    'A_require_consecutive_pos': True,
    'A_min_roe_pct': 17,
    'N_max_pct_from_52w_high': -25,
    'S_min_vol_ratio': 1.0,
    'L_min_rs_rank': 70,
    'I_fetch_inst_holders': False,
    'I_min_inst_holders': 10,
    'universe': 'russell1000',
    'min_price': 10.0,
    'min_mkt_cap_M': 500,
    'min_avg_volume': 200_000,
    'max_workers': 6,
    'qualified_score_threshold': 5,
}


# ============================================================================
# API HELPERS
# ============================================================================
_api_sem = threading.Semaphore(6)


@st.cache_data(ttl=14400, show_spinner=False)
def fmp_cached(endpoint: str, params_json: str, version: int = 3):
    """FMP API call with caching"""
    params = json.loads(params_json) if params_json else {}
    params['apikey'] = FMP_KEY
    try:
        r = requests.get(f"{BASE_URL}/v{version}/{endpoint}", params=params, timeout=25)
        r.raise_for_status()
        d = r.json()
        if isinstance(d, dict) and ('Error Message' in d or 'message' in d):
            return None
        return d
    except Exception:
        return None


def fmp(endpoint: str, params: dict = None, version: int = 3):
    """Wrapper for cached FMP calls"""
    params_json = json.dumps(params or {}, sort_keys=True)
    return fmp_cached(endpoint, params_json, version)


# ============================================================================
# UNIVERSE BUILDERS (same indexes as Technical Screen)
# ============================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def load_stock_index() -> pd.DataFrame:
    """Load stock universe from Excel file (legacy - used for Russell 3000 and Broad US Index)"""
    try:
        df = pd.read_excel(INDEX_FILE)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_sp500() -> pd.DataFrame:
    """Load S&P 500 from FMP API"""
    try:
        url = f"{BASE_URL}/v3/sp500_constituent?apikey={FMP_KEY}"
        response = requests.get(url, timeout=30)
        data = response.json()

        if data and len(data) > 0:
            df = pd.DataFrame(data)
            df = df.rename(columns={"symbol": "Ticker", "name": "Name", "sector": "Sector"})
            df["Exchange"] = ""
            df["Index"] = "S&P 500"
            df["Industry"] = df.get("subSector", "")
            return df[["Ticker", "Name", "Sector", "Industry", "Exchange", "Index"]]
    except Exception as e:
        st.warning(f"Could not fetch S&P 500 from API: {e}")

    # Fallback to Excel file
    try:
        df = pd.read_excel(SP500_FILE)
        df = df.rename(columns={"Symbol": "Ticker"})
        df["Index"] = "S&P 500"
        df["Industry"] = df.get("subSector", "")
        if "Exchange" not in df.columns:
            df["Exchange"] = ""
        return df
    except:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_nasdaq100() -> pd.DataFrame:
    """Load NASDAQ 100 from FMP API (with Excel fallback)"""
    try:
        url = f"{BASE_URL}/v3/nasdaq_constituent?apikey={FMP_KEY}"
        response = requests.get(url, timeout=30)
        data = response.json()

        if data and len(data) > 0:
            df = pd.DataFrame(data)
            return pd.DataFrame({
                "Ticker": df["symbol"] if "symbol" in df.columns else df.get("ticker", ""),
                "Name": df.get("name", df.get("companyName", "")),
                "Sector": df.get("sector", ""),
                "Industry": df.get("subSector", ""),
                "Exchange": "NASDAQ",
                "Index": "NASDAQ 100"
            })
    except Exception:
        pass

    # Fallback to Excel file
    try:
        df = pd.read_excel(NASDAQ100_FILE)
        df_clean = df.iloc[4:].copy()
        result = pd.DataFrame({
            "Ticker": df_clean["Unnamed: 2"].values,
            "Name": df_clean["Unnamed: 3"].values,
            "Sector": "",
            "Industry": "",
            "Exchange": "NASDAQ",
            "Index": "NASDAQ 100"
        })
        result = result.dropna(subset=["Ticker"])
        result["Ticker"] = result["Ticker"].str.upper()
        return result
    except Exception:
        pass

    return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_russell2000() -> pd.DataFrame:
    """Load Russell 2000 from CSV file in repo (works on Streamlit Cloud)"""
    # First try CSV file in repo
    try:
        csv_path = Path(__file__).parent / "russell_2000.csv"
        df = pd.read_csv(csv_path)
        result = pd.DataFrame({
            "Ticker": df["Ticker"].str.upper().str.strip(),
            "Name": df["Name"].fillna(""),
            "Sector": df["Sector"].fillna(""),
            "Industry": df.get("Industry", "").fillna("") if "Industry" in df.columns else "",
            "Exchange": df["Exchange"].fillna("") if "Exchange" in df.columns else "",
            "Index": "Russell 2000"
        })
        result = result.dropna(subset=["Ticker"])
        result = result.drop_duplicates(subset=["Ticker"], keep="first")
        return result
    except Exception:
        pass

    # Fallback to FMP API
    try:
        url = f"{BASE_URL}/v3/russell_2000_constituent?apikey={FMP_KEY}"
        response = requests.get(url, timeout=30)
        data = response.json()

        if data and len(data) > 0:
            df = pd.DataFrame(data)
            df = df.rename(columns={"symbol": "Ticker", "name": "Name", "sector": "Sector"})
            df["Industry"] = df.get("subSector", "")
            df["Exchange"] = ""
            df["Index"] = "Russell 2000"
            df = df[["Ticker", "Name", "Sector", "Industry", "Exchange", "Index"]]
            df = df.drop_duplicates(subset=["Ticker"], keep="first")
            return df
    except Exception:
        pass

    return pd.DataFrame()


def load_disruption() -> pd.DataFrame:
    """Load Disruption Index from Excel file"""
    try:
        df = pd.read_excel(DISRUPTION_FILE)
        # Symbols are in column B (Unnamed: 1), skip header row
        symbols = df["Unnamed: 1"].dropna().tolist()
        # Clean symbols - uppercase, strip whitespace, remove header
        symbols = [str(s).upper().strip() for s in symbols
                   if str(s).upper().strip() not in ["SYMBOL", "", "NAN"]]
        # Remove duplicates while preserving order
        seen = set()
        unique_symbols = [s for s in symbols if not (s in seen or seen.add(s))]
        result = pd.DataFrame({
            "Ticker": unique_symbols,
            "Name": "",
            "Sector": "",
            "Industry": "",
            "Exchange": "",
            "Index": "Disruption"
        })
        return result
    except Exception as e:
        st.warning(f"Error loading Disruption Index: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_master_universe() -> pd.DataFrame:
    """Load Master Universe from local project directory"""
    local_path = Path(__file__).parent / "master_universe.csv"

    try:
        df = pd.read_csv(local_path, header=None, names=["Ticker", "Name", "Exchange"])
        # Filter out invalid rows (nan tickers, empty tickers)
        df = df[df["Ticker"].notna()]
        df = df[df["Ticker"].astype(str).str.strip() != ""]
        df = df[df["Ticker"].astype(str).str.lower() != "nan"]
        # Clean up ticker symbols
        df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
        df["Name"] = df["Name"].fillna("")
        df["Exchange"] = df["Exchange"].fillna("")
        df["Sector"] = ""
        df["Industry"] = ""
        df["Index"] = "Master Universe"
        return df[["Ticker", "Name", "Sector", "Industry", "Exchange", "Index"]]
    except Exception as e:
        st.warning(f"Could not load Master Universe: {e}")
        return pd.DataFrame()


def get_available_indices() -> list:
    """Get available indices"""
    return ["S&P 500", "NASDAQ 100", "Russell 2000", "Disruption", "Master Universe", "Russell 1000 (Screener)"]


@st.cache_data(ttl=3600, show_spinner=False)
def build_universe(universe_type: str) -> list:
    """Build stock universe based on selected index"""

    if universe_type == "S&P 500":
        df = load_sp500()
    elif universe_type == "NASDAQ 100":
        df = load_nasdaq100()
    elif universe_type == "Russell 2000":
        df = load_russell2000()
    elif universe_type == "Disruption":
        df = load_disruption()
    elif universe_type == "Master Universe":
        df = load_master_universe()
    elif universe_type == "Russell 1000 (Screener)":
        # Use FMP screener for Russell 1000 equivalent
        sp500_data = fmp("sp500_constituent")
        sp500 = [(d['symbol'], d.get('name', ''), d.get('sector', ''), d.get('subSector', ''))
                 for d in (sp500_data or []) if d.get('symbol') and '.' not in d['symbol']]

        screener = fmp("stock-screener", {
            'marketCapMoreThan': 1_000_000_000,
            'volumeMoreThan': 100_000,
            'country': 'US',
            'isEtf': 'false',
            'exchange': 'NYSE,NASDAQ',
            'limit': 1500,
        })
        rows = [
            (d['symbol'], d.get('companyName', ''), d.get('sector', ''), d.get('industry', ''))
            for d in (screener or [])
            if d.get('symbol') and '.' not in d['symbol']
        ]

        # Merge: prefer S&P 500 metadata when available
        combined = {r[0]: r for r in rows}
        for r in sp500:
            combined[r[0]] = r
        return list(combined.values())
    else:
        # Default to S&P 500
        df = load_sp500()

    if df.empty:
        return []

    # Convert DataFrame to list of tuples (ticker, name, sector, industry)
    result = []
    for _, row in df.iterrows():
        ticker = row.get('Ticker', '')
        if ticker and '.' not in ticker:
            result.append((
                ticker,
                row.get('Name', ''),
                row.get('Sector', ''),
                row.get('Industry', '')
            ))
    return result


@st.cache_data(ttl=3600, show_spinner=False)
def bulk_quotes(tickers: list) -> dict:
    """Fetch bulk quotes"""
    out = {}
    for chunk in [tickers[i:i+100] for i in range(0, len(tickers), 100)]:
        data = fmp(f"quote/{','.join(chunk)}")
        for q in (data or []):
            out[q['symbol']] = q
        time.sleep(0.1)
    return out


def spy_returns() -> tuple:
    """Return (1Y%, 3M%) for SPY"""
    d = fmp("stock-price-change/SPY")
    if d:
        return d[0].get('1Y', 0.0), d[0].get('3M', 0.0)
    return 0.0, 0.0


# ============================================================================
# DATA FETCHER
# ============================================================================

def _fetch_one(sym: str) -> dict:
    """Fetch all data for one ticker"""
    with _api_sem:
        time.sleep(0.05)
        q_inc = fmp(f"income-statement/{sym}", {'period': 'quarter', 'limit': 9})
        a_inc = fmp(f"income-statement/{sym}", {'period': 'annual', 'limit': 5})
        ests = fmp(f"analyst-estimates/{sym}", {'limit': 6})
        pchg = fmp(f"stock-price-change/{sym}")
        km_ttm = fmp(f"key-metrics-ttm/{sym}")
        updown = fmp("upgrades-downgrades", {'symbol': sym, 'limit': 25}, version=4)
        float_d = fmp("shares_float", {'symbol': sym})

    return {
        'q_inc': q_inc or [],
        'a_inc': a_inc or [],
        'ests': sorted(ests or [], key=lambda x: x.get('date', '')),
        'pchg': (pchg or [{}])[0],
        'km_ttm': (km_ttm or [{}])[0],
        'updown': updown or [],
        'float': (float_d or [{}])[0] if float_d else {},
        'inst_count': 0,
    }


def fetch_all_parallel(sym_list: list, progress_bar, status_text) -> dict:
    """Fetch data for all tickers in parallel"""
    total = len(sym_list)
    results = {}
    completed = [0]
    lock = threading.Lock()

    def _wrapped(sym):
        try:
            d = _fetch_one(sym)
        except Exception:
            d = None
        with lock:
            completed[0] += 1
            n = completed[0]
            if n % 25 == 0 or n == total:
                progress_bar.progress(n / total)
                status_text.text(f"Fetching data: {n}/{total} stocks ({n/total*100:.0f}%)")
        return sym, d

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_wrapped, s): s for s in sym_list}
        for future in as_completed(futures):
            try:
                sym, d = future.result()
            except Exception:
                sym = futures[future]
                d = None
            results[sym] = d

    return results


# ============================================================================
# METRIC CALCULATOR
# ============================================================================

def _safe(v, decimals=2):
    """Return rounded float or np.nan if None/NaN"""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return np.nan
    return round(float(v), decimals)


def calculate(ticker, name, sector, industry, quote, data, settings) -> dict:
    """Calculate CANSLIM metrics for one stock"""
    rec = {}
    price = quote.get('price') or np.nan

    q_inc = data['q_inc']
    a_inc = data['a_inc']
    ests = data['ests']
    pchg = data['pchg']
    km_ttm = data['km_ttm']
    updown = data['updown']
    float_d = data['float']

    # Identification
    rec['Ticker'] = ticker
    rec['Company'] = name or quote.get('name', '')
    rec['Sector'] = sector
    rec['Industry'] = industry
    rec['Price ($)'] = _safe(price, 2)
    rec['Mkt Cap ($M)'] = round((quote.get('marketCap') or 0) / 1e6, 0)

    fs = float_d.get('floatShares')
    ff = float_d.get('freeFloat')
    rec['Float ($M shares)'] = _safe(fs / 1e6, 1) if fs is not None and fs > 0 else np.nan
    rec['Float %'] = round(float(ff) * 100, 1) if ff is not None else np.nan

    # C: Current Quarterly EPS + Revenue
    q_eps_g = q_rev_g = q_accel = np.nan
    rq_eps = rq_yago_eps = rq_rev = rq_yago_rev = np.nan

    if len(q_inc) >= 5:
        eq0, eq4 = q_inc[0].get('eps'), q_inc[4].get('eps')
        rv0, rv4 = q_inc[0].get('revenue'), q_inc[4].get('revenue')

        if eq0 is not None and eq4 is not None:
            rq_eps = eq0
            rq_yago_eps = eq4
            if eq4 != 0:
                q_eps_g = (eq0 - eq4) / abs(eq4) * 100
            elif eq4 < 0 and eq0 > 0:
                q_eps_g = 100.0

        if rv0 and rv4 and rv4 > 0:
            rq_rev = rv0
            rq_yago_rev = rv4
            q_rev_g = (rv0 - rv4) / rv4 * 100

        if len(q_inc) >= 9:
            eq1 = q_inc[1].get('eps')
            eq5 = q_inc[5].get('eps')
            if eq1 is not None and eq5 is not None and eq5 != 0 and not np.isnan(q_eps_g):
                q_accel = q_eps_g - (eq1 - eq5) / abs(eq5) * 100

    c_eps_pass = not np.isnan(q_eps_g) and q_eps_g >= settings['C_min_qtrly_eps_growth']
    c_rev_pass = not np.isnan(q_rev_g) and q_rev_g >= settings['C_min_qtrly_rev_growth']
    c_pass = c_eps_pass and c_rev_pass

    rec['C Pass'] = 'Y' if c_pass else 'N'
    rec['C: Qtr EPS YoY %'] = _safe(q_eps_g, 1)
    rec['C: Qtr Rev YoY %'] = _safe(q_rev_g, 1)
    rec['C: EPS Acceleration'] = _safe(q_accel, 1)

    # A: Annual EPS + ROE
    a_cagr = np.nan
    eps_yr = [None] * 4

    if len(a_inc) >= 4:
        eps_yr = [a_inc[i].get('eps') for i in range(4)]
        e0, e1, e2, e3 = eps_yr
        if e0 and e3 and e3 > 0 and e0 > 0:
            a_cagr = ((e0 / e3) ** (1/3) - 1) * 100

    consec_pos = all(v is not None and v > 0 for v in eps_yr[:3])

    roe_raw = km_ttm.get('roeTTM')
    roe_pct = (float(roe_raw) * 100) if isinstance(roe_raw, (int, float)) and not np.isnan(float(roe_raw)) else np.nan
    roe_pass = not np.isnan(roe_pct) and roe_pct >= settings['A_min_roe_pct']

    a_pass = (not np.isnan(a_cagr) and a_cagr >= settings['A_min_eps_cagr_3yr']
              and consec_pos and roe_pass)

    rec['A Pass'] = 'Y' if a_pass else 'N'
    rec['A: ROE TTM %'] = _safe(roe_pct, 1)
    rec['A: EPS CAGR 3Y %'] = _safe(a_cagr, 1)
    rec['A: Consec Pos EPS'] = 'Y' if consec_pos else 'N'

    # N: New High
    hi = quote.get('yearHigh') or np.nan
    lo = quote.get('yearLow') or np.nan
    pct_from_hi = ((price - hi) / hi * 100) if (not np.isnan(price) and not np.isnan(hi) and hi > 0) else np.nan
    n_pass = not np.isnan(pct_from_hi) and pct_from_hi >= settings['N_max_pct_from_52w_high']
    rec['N Pass'] = 'Y' if n_pass else 'N'
    rec['N: % From 52W High'] = _safe(pct_from_hi, 1)

    # S: Supply & Demand
    vol = quote.get('volume') or 0
    avg_vol = quote.get('avgVolume') or 1
    vol_r = vol / avg_vol if avg_vol > 0 else np.nan
    s_pass = not np.isnan(vol_r) and vol_r >= settings['S_min_vol_ratio']
    rec['S Pass'] = 'Y' if s_pass else 'N'
    rec['S: Vol / Avg'] = _safe(vol_r, 2)

    # L: Leadership (RS scores - rank computed later)
    rs_1y = pchg.get('1Y') or np.nan
    rs_3m = pchg.get('3M') or np.nan

    if not np.isnan(rs_1y) and not np.isnan(rs_3m):
        rs_w = rs_3m * 0.4 + rs_1y * 0.6
    elif not np.isnan(rs_1y):
        rs_w = rs_1y
    else:
        rs_w = np.nan

    rec['L Pass'] = 'PENDING'
    rec['L: RS Rank (1-99)'] = np.nan
    rec['L: 1Y Return %'] = _safe(rs_1y, 1)
    rec['L: 3M Return %'] = _safe(rs_3m, 1)

    # I: Institutional
    i_pass = True
    rec['I Pass'] = 'Y'

    # M: Market Direction
    rec['M Pass'] = 'Manual'

    # Analyst recommendations
    cutoff_str = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    recent = [u for u in updown if (u.get('publishedDate') or '') >= cutoff_str]
    ups = sum(1 for u in recent if str(u.get('action', '')).lower() in ('up', 'upgrade', 'upgraded', 'initiated', 'reit'))
    downs = sum(1 for u in recent if str(u.get('action', '')).lower() in ('down', 'downgrade', 'downgraded'))
    rec['Analyst Net (90d)'] = ups - downs

    # Valuation
    today_str = datetime.now().strftime('%Y-%m-%d')
    future = [e for e in ests if e.get('date', '') > today_str]
    fy1_eps = fy1_pe = peg = fy1_g = np.nan

    if future:
        raw = future[0].get('estimatedEpsAvg')
        fy1_eps = float(raw) if raw is not None else np.nan
        if not np.isnan(fy1_eps) and fy1_eps > 0 and not np.isnan(price):
            fy1_pe = round(price / fy1_eps, 1)

    trailing_eps = eps_yr[0]
    if trailing_eps and trailing_eps > 0 and not np.isnan(fy1_eps):
        fy1_g = (fy1_eps - trailing_eps) / abs(trailing_eps) * 100
    if not np.isnan(fy1_pe) and not np.isnan(fy1_g) and fy1_g > 0:
        peg = round(fy1_pe / fy1_g, 2)

    trailing_pe = np.nan
    if trailing_eps and trailing_eps > 0 and not np.isnan(price):
        trailing_pe = round(price / trailing_eps, 1)
    elif quote.get('pe'):
        trailing_pe = round(float(quote['pe']), 1)

    rec['Trailing P/E'] = trailing_pe
    rec['FY1 Est P/E'] = fy1_pe
    rec['PEG'] = peg

    # Internal helpers
    rec['_c_pass'] = c_pass
    rec['_a_pass'] = a_pass
    rec['_n_pass'] = n_pass
    rec['_s_pass'] = s_pass
    rec['_i_pass'] = i_pass
    rec['_rs_w'] = rs_w

    return rec


def post_process(df: pd.DataFrame, settings: dict) -> pd.DataFrame:
    """Post-process: compute RS ranks and final scores"""
    df = df.copy()

    # True RS Rank across full universe (1-99 scale)
    df['L: RS Rank (1-99)'] = (
        df['_rs_w']
        .rank(pct=True, na_option='bottom') * 98 + 1
    ).round(0).astype('Int64')

    min_rs = settings['L_min_rs_rank']
    df['L Pass'] = df['L: RS Rank (1-99)'].apply(
        lambda x: 'Y' if (pd.notna(x) and x >= min_rs) else 'N')

    # Industry Group RS Rank
    ind_avg = (
        df.groupby('Industry')['L: RS Rank (1-99)']
        .mean()
        .rank(pct=True) * 98 + 1
    )
    df['Industry RS Rank'] = df['Industry'].map(ind_avg).round(0).astype('Int64')

    # CANSLIM composite score
    def _score(row):
        return sum([
            bool(row['_c_pass']),
            bool(row['_a_pass']),
            bool(row['_n_pass']),
            bool(row['_s_pass']),
            row['L Pass'] == 'Y',
            bool(row['_i_pass']),
        ])

    df['CANSLIM Score'] = df.apply(_score, axis=1)
    df['Score Display'] = df['CANSLIM Score'].apply(lambda x: f"{x} of 6")

    # Drop internal helper columns
    df.drop(columns=[c for c in df.columns if c.startswith('_')],
            inplace=True, errors='ignore')
    return df


# ============================================================================
# MAIN APP
# ============================================================================

def run_screener(settings):
    """Run the CANSLIM screener"""

    progress_container = st.container()

    with progress_container:
        status_text = st.empty()
        progress_bar = st.progress(0)

        # Step 1: Build Universe
        status_text.text("Building stock universe...")
        universe = build_universe(settings['universe'])
        if not universe:
            st.error("Could not build universe. Check API key.")
            return None

        info_map = {sym: (name, sec, ind) for sym, name, sec, ind in universe}
        tickers = list(info_map.keys())
        st.write(f"Universe: {len(tickers)} stocks")

        # Step 2: Bulk quotes + pre-filter
        status_text.text("Fetching bulk quotes...")
        progress_bar.progress(0.1)
        quotes = bulk_quotes(tickers)

        candidates = {
            sym: q for sym, q in quotes.items()
            if (q.get('price') or 0) >= settings['min_price']
            and (q.get('marketCap') or 0) >= settings['min_mkt_cap_M'] * 1e6
            and (q.get('avgVolume') or 0) >= settings['min_avg_volume']
        }
        st.write(f"After pre-filter: {len(candidates)} stocks")

        # Step 3: SPY benchmark
        status_text.text("Fetching benchmark...")
        progress_bar.progress(0.15)
        spy_1y, spy_3m = spy_returns()

        # Step 4: Parallel data fetch
        status_text.text("Fetching detailed stock data...")
        progress_bar.progress(0.2)
        sym_list = sorted(candidates.keys())
        stock_data = fetch_all_parallel(sym_list, progress_bar, status_text)

        # Step 5: Calculate metrics
        status_text.text("Calculating CANSLIM metrics...")
        progress_bar.progress(0.9)
        records = []
        for sym in sym_list:
            d = stock_data.get(sym)
            if d is None:
                continue
            name, sec, ind = info_map.get(sym, ('', '', ''))
            rec = calculate(sym, name, sec, ind, candidates[sym], d, settings)
            records.append(rec)

        # Step 6: Post-process
        status_text.text("Computing RS ranks and final scores...")
        df_raw = pd.DataFrame(records)
        df = post_process(df_raw, settings)
        df = df.sort_values(['CANSLIM Score', 'C: Qtr EPS YoY %'], ascending=[False, False])

        progress_bar.progress(1.0)
        status_text.text("Complete!")
        time.sleep(0.5)
        progress_bar.empty()
        status_text.empty()

    return df, spy_1y, spy_3m


def main():
    # Check API key
    if not FMP_KEY:
        st.error("FMP_API_KEY not found. Please set it in .env file or Streamlit secrets.")
        st.stop()

    # Header
    st.markdown('<p class="main-title">CANSLIM Stock Screener</p>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">High-Quality Growth Stock Screening Based on O\'Neil Methodology</p>', unsafe_allow_html=True)

    st.divider()

    # Sidebar settings
    with st.sidebar:
        st.header("Screener Settings")

        universe = st.selectbox(
            "Stock Universe",
            get_available_indices(),
            index=0  # Default to S&P 500
        )

        st.subheader("C - Current Earnings")
        c_eps = st.slider("Min Quarterly EPS Growth %", 0, 100, 25)
        c_rev = st.slider("Min Quarterly Revenue Growth %", 0, 100, 20)

        st.subheader("A - Annual Earnings")
        a_cagr = st.slider("Min 3-Year EPS CAGR %", 0, 100, 25)
        a_roe = st.slider("Min ROE %", 0, 50, 17)

        st.subheader("N - New Highs")
        n_high = st.slider("Max % From 52-Week High", -50, 0, -25)

        st.subheader("L - Leadership")
        l_rs = st.slider("Min RS Rank (1-99)", 1, 99, 70)

        st.subheader("Pre-Filters")
        min_price = st.number_input("Min Price ($)", value=10.0)
        min_mktcap = st.number_input("Min Market Cap ($M)", value=500)
        min_vol = st.number_input("Min Avg Volume", value=200000)

        qualified_threshold = st.slider("Qualified Score Threshold", 1, 6, 5)

    # Build settings dict
    settings = {
        'universe': universe,
        'C_min_qtrly_eps_growth': c_eps,
        'C_min_qtrly_rev_growth': c_rev,
        'A_min_eps_cagr_3yr': a_cagr,
        'A_require_consecutive_pos': True,
        'A_min_roe_pct': a_roe,
        'N_max_pct_from_52w_high': n_high,
        'S_min_vol_ratio': 1.0,
        'L_min_rs_rank': l_rs,
        'I_fetch_inst_holders': False,
        'I_min_inst_holders': 10,
        'min_price': min_price,
        'min_mkt_cap_M': min_mktcap,
        'min_avg_volume': min_vol,
        'max_workers': 6,
        'qualified_score_threshold': qualified_threshold,
    }

    # Run button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        run_button = st.button("Run CANSLIM Screener", type="primary", use_container_width=True)

    if run_button:
        with st.spinner("Running CANSLIM Screener..."):
            result = run_screener(settings)

        if result is not None:
            df, spy_1y, spy_3m = result

            # Store in session state
            st.session_state['canslim_results'] = df
            st.session_state['spy_1y'] = spy_1y
            st.session_state['spy_3m'] = spy_3m
            st.session_state['settings'] = settings

    # Display results if available
    if 'canslim_results' in st.session_state:
        df = st.session_state['canslim_results']
        spy_1y = st.session_state.get('spy_1y', 0)
        spy_3m = st.session_state.get('spy_3m', 0)
        settings = st.session_state.get('settings', DEFAULT_SETTINGS)

        # Summary metrics
        st.subheader("Summary")
        threshold = settings.get('qualified_score_threshold', 5)
        df_qualified = df[df['CANSLIM Score'] >= threshold]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Analyzed", len(df))
        with col2:
            st.metric(f"Qualified (>= {threshold}/6)", len(df_qualified))
        with col3:
            st.metric("SPY 1Y Return", f"{spy_1y:.1f}%")
        with col4:
            st.metric("SPY 3M Return", f"{spy_3m:.1f}%")

        st.divider()

        # Tabs for results
        tab1, tab2, tab3 = st.tabs(["Qualified Stocks", "All Stocks", "Top Industries"])

        # Column ordering for display
        display_cols = [
            'Ticker', 'Company', 'Sector', 'Industry', 'Price ($)', 'Mkt Cap ($M)',
            'Score Display', 'CANSLIM Score',
            'C Pass', 'C: Qtr EPS YoY %', 'C: Qtr Rev YoY %',
            'A Pass', 'A: ROE TTM %', 'A: EPS CAGR 3Y %',
            'N Pass', 'N: % From 52W High',
            'S Pass', 'S: Vol / Avg',
            'L Pass', 'L: RS Rank (1-99)', 'L: 1Y Return %', 'L: 3M Return %',
            'Industry RS Rank',
            'Analyst Net (90d)', 'Trailing P/E', 'FY1 Est P/E', 'PEG'
        ]
        display_cols = [c for c in display_cols if c in df.columns]

        with tab1:
            st.subheader(f"CANSLIM Qualified Stocks (Score >= {threshold}/6)")
            if len(df_qualified) > 0:
                st.dataframe(
                    df_qualified[display_cols],
                    use_container_width=True,
                    height=600
                )

                # Download button
                csv = df_qualified.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Download Qualified CSV",
                    csv,
                    f"canslim_qualified_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    "text/csv",
                    key='download_qualified'
                )
            else:
                st.info("No stocks met the qualification criteria.")

        with tab2:
            st.subheader("All Analyzed Stocks")
            st.dataframe(
                df[display_cols],
                use_container_width=True,
                height=600
            )

            # Download button
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download All CSV",
                csv,
                f"canslim_all_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                "text/csv",
                key='download_all'
            )

        with tab3:
            st.subheader("Top Industry Groups by RS Rank")
            ind_summary = df.groupby('Industry').agg({
                'L: RS Rank (1-99)': 'mean',
                'Industry RS Rank': 'first',
                'Ticker': 'count'
            }).rename(columns={
                'L: RS Rank (1-99)': 'Avg Member RS',
                'Ticker': 'Stock Count'
            }).sort_values('Industry RS Rank', ascending=False)

            st.dataframe(ind_summary, use_container_width=True, height=500)

    # CANSLIM Legend
    with st.expander("CANSLIM Criteria Explained"):
        st.markdown("""
        **C = Current Quarterly Earnings**
        - Quarterly EPS growth >= 25% YoY
        - Quarterly Revenue growth >= 20% YoY

        **A = Annual Earnings Growth**
        - 3-Year EPS CAGR >= 25%
        - ROE >= 17% (TTM)
        - Consecutive positive EPS for last 3 years

        **N = New Highs**
        - Stock within 25% of 52-week high (buying zone)

        **S = Supply & Demand**
        - Volume ratio >= 1.0 (current vs average)

        **L = Leader in Industry**
        - Relative Strength Rank >= 70 (top 30% of universe)

        **I = Institutional Sponsorship**
        - Assumed pass for S&P/Russell components

        **M = Market Direction**
        - Manual assessment (not auto-scored)

        **Score**: 6/6 = Perfect CANSLIM candidate
        """)


if __name__ == "__main__":
    main()
