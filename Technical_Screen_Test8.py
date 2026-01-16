"""
Technical Analysis Screen - Test Version 8
Adding load functions (Excel/API) and screener UI
"""
import streamlit as st
import pandas as pd
import numpy as np
import requests
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, Dict, List, Tuple
import threading

# Page config MUST be first
st.set_page_config(
    page_title="Technical Screen Test 8",
    page_icon="📈",
    layout="wide"
)

load_dotenv()

def get_secret(key: str):
    try:
        return st.secrets.get(key)
    except:
        return os.getenv(key)

FMP_API_KEY = get_secret("FMP_API_KEY")

# File paths
INDEX_FILE = Path(__file__).parent / "Index_Broad_US.xlsx"
SP500_FILE = Path(__file__).parent / "SP500_list_with_sectors.xlsx"
DISRUPTION_FILE = Path(__file__).parent / "Disruption Index.xlsx"
NASDAQ100_FILE = Path(__file__).parent / "NASDAQ100_LIST.xlsx"


@st.cache_data(ttl=3600)
def load_stock_index() -> pd.DataFrame:
    try:
        df = pd.read_excel(INDEX_FILE)
        return df
    except Exception as e:
        st.warning(f"Could not load index file: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_sp500() -> pd.DataFrame:
    try:
        url = f"https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=30)
        data = response.json()
        if data and len(data) > 0:
            df = pd.DataFrame(data)
            df = df.rename(columns={"symbol": "Ticker", "name": "Name", "sector": "Sector"})
            df["Index"] = "S&P 500"
            return df[["Ticker", "Name", "Sector", "Index"]]
    except Exception as e:
        st.warning(f"Could not fetch S&P 500: {e}")
    return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_nasdaq100() -> pd.DataFrame:
    try:
        df = pd.read_excel(NASDAQ100_FILE)
        df_clean = df.iloc[4:].copy()
        result = pd.DataFrame({
            "Ticker": df_clean["Unnamed: 2"].values,
            "Name": df_clean["Unnamed: 3"].values,
            "Sector": "",
            "Index": "NASDAQ 100"
        })
        result = result.dropna(subset=["Ticker"])
        result["Ticker"] = result["Ticker"].str.upper()
        return result
    except:
        return pd.DataFrame()


def load_disruption() -> pd.DataFrame:
    try:
        df = pd.read_excel(DISRUPTION_FILE)
        symbols = df["Unnamed: 1"].dropna().tolist()
        symbols = [str(s).upper().strip() for s in symbols if str(s).upper().strip() not in ["SYMBOL", "", "NAN"]]
        seen = set()
        unique_symbols = [s for s in symbols if not (s in seen or seen.add(s))]
        return pd.DataFrame({"Ticker": unique_symbols, "Name": "", "Sector": "", "Index": "Disruption"})
    except:
        return pd.DataFrame()


class DataFetcher:
    def __init__(self):
        self.fmp_base = "https://financialmodelingprep.com/api/v3"
        self._session = None
        self._session_lock = threading.Lock()

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            with self._session_lock:
                if self._session is None:
                    self._session = requests.Session()
                    adapter = requests.adapters.HTTPAdapter(pool_connections=25, pool_maxsize=25, max_retries=2)
                    self._session.mount('https://', adapter)
                    self._session.mount('http://', adapter)
        return self._session

    def get_historical_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        try:
            stock = yf.Ticker(symbol)
            df = stock.history(period=period)
            if df is not None and not df.empty:
                return df
        except:
            pass
        try:
            days_map = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730}
            days = days_map.get(period, 365)
            url = f"{self.fmp_base}/historical-price-full/{symbol}?apikey={FMP_API_KEY}"
            response = self.session.get(url, timeout=10)
            data = response.json()
            if "historical" in data:
                df = pd.DataFrame(data["historical"])
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date").set_index("date")
                cutoff = datetime.now() - timedelta(days=days)
                df = df[df.index >= cutoff]
                df = df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
                return df[["Open", "High", "Low", "Close", "Volume"]]
        except:
            pass
        return None

    def get_stock_list(self, index_name: str) -> Tuple[List[str], pd.DataFrame]:
        if index_name == "S&P 500":
            df = load_sp500()
        elif index_name == "NASDAQ 100":
            df = load_nasdaq100()
        elif index_name == "Disruption":
            df = load_disruption()
        else:
            df = load_stock_index()

        if df.empty:
            fallback = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA"]
            return fallback, pd.DataFrame({"Ticker": fallback})

        return df["Ticker"].tolist(), df

    def get_indices(self) -> List[str]:
        return ["S&P 500", "NASDAQ 100", "Disruption", "Broad US Index"]


def main():
    st.title("Technical Analysis Screen - Test 8")

    fetcher = DataFetcher()

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Select Module", ["Chart Dashboard", "Stock Screener", "Signal Scanner"])

    if page == "Chart Dashboard":
        st.header("Chart Dashboard")
        symbol = st.text_input("Symbol", value="AAPL").upper()
        if st.button("Load"):
            df = fetcher.get_historical_data(symbol, "3mo")
            if df is not None:
                st.success(f"Loaded {len(df)} rows")
                fig = go.Figure(go.Candlestick(x=df.index, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"]))
                st.plotly_chart(fig)

    elif page == "Stock Screener":
        st.header("Stock Screener - Test 8")

        screen_type = st.selectbox("Select Screen", ["VCP Compression", "Pullback", "Oversold"])

        col1, col2 = st.columns(2)
        with col1:
            index_name = st.selectbox("Stock Universe", fetcher.get_indices())
        with col2:
            limit = st.number_input("Limit (0 = all)", min_value=0, max_value=500, value=10)

        if st.button("Load Universe", type="primary"):
            with st.spinner("Loading stock list..."):
                symbols, stock_df = fetcher.get_stock_list(index_name)
                if limit > 0:
                    symbols = symbols[:limit]
                st.success(f"Loaded {len(symbols)} stocks from {index_name}")
                st.dataframe(stock_df.head(20))

    elif page == "Signal Scanner":
        st.header("Signal Scanner")
        st.info("Scanner - Test 8")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Test 8** - load functions")


if __name__ == "__main__":
    main()
