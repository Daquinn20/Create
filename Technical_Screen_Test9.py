"""
Technical Analysis Screen - Test Version 9
Adding StockScreener with parallel processing
"""
import streamlit as st
import pandas as pd
import numpy as np
import requests
import yfinance as yf
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, Dict, List, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Page config MUST be first
st.set_page_config(
    page_title="Technical Screen Test 9",
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


class TechnicalIndicators:
    @staticmethod
    def sma(data: pd.Series, period: int) -> pd.Series:
        return data.rolling(window=period).mean()

    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))


class StockScreener:
    """Screen stocks with parallel processing"""

    MAX_WORKERS = 10

    def __init__(self, fetcher: DataFetcher):
        self.fetcher = fetcher
        self.ti = TechnicalIndicators()

    def _process_single(self, symbol: str) -> Optional[Dict]:
        """Process a single stock"""
        try:
            df = self.fetcher.get_historical_data(symbol, "1y")
            if df is None or len(df) < 50:
                return None

            close = df["Close"]
            current_price = close.iloc[-1]

            sma_20 = self.ti.sma(close, 20).iloc[-1]
            sma_50 = self.ti.sma(close, 50).iloc[-1]
            rsi = self.ti.rsi(close).iloc[-1]

            above_20 = current_price > sma_20
            above_50 = current_price > sma_50
            rsi_ok = 40 <= rsi <= 70

            return {
                "Symbol": symbol,
                "Price": round(current_price, 2),
                "SMA20": round(sma_20, 2),
                "SMA50": round(sma_50, 2),
                "RSI": round(rsi, 1),
                "Above 20": "PASS" if above_20 else "FAIL",
                "Above 50": "PASS" if above_50 else "FAIL",
                "RSI OK": "PASS" if rsi_ok else "FAIL",
                "Grade": "PASS" if (above_20 and above_50 and rsi_ok) else "FAIL"
            }
        except Exception as e:
            return None

    def screen(self, symbols: List[str]) -> pd.DataFrame:
        """Run screen with parallel processing"""
        results = []
        progress = st.progress(0)
        status = st.empty()

        completed = 0
        total = len(symbols)

        status.text(f"Starting parallel scan of {total} stocks...")

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            future_to_symbol = {
                executor.submit(self._process_single, symbol): symbol
                for symbol in symbols
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                completed += 1

                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except:
                    pass

                progress.progress(completed / total)
                status.text(f"Processed {symbol} ({completed}/{total})")

        progress.empty()
        status.empty()

        df_results = pd.DataFrame(results)
        if not df_results.empty:
            df_results = df_results.sort_values(by="Grade", ascending=True)
        return df_results


def main():
    st.title("Technical Analysis Screen - Test 9")
    st.write("Testing StockScreener with parallel processing")

    fetcher = DataFetcher()
    screener = StockScreener(fetcher)

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Select", ["Chart", "Screener"])

    if page == "Chart":
        st.header("Chart")
        symbol = st.text_input("Symbol", value="AAPL").upper()
        if st.button("Load"):
            df = fetcher.get_historical_data(symbol, "3mo")
            if df is not None:
                st.success(f"Loaded {len(df)} rows")
                fig = go.Figure(go.Candlestick(x=df.index, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"]))
                st.plotly_chart(fig)

    elif page == "Screener":
        st.header("Stock Screener - Parallel Processing Test")

        test_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ"]

        st.write(f"Test symbols: {', '.join(test_symbols)}")

        if st.button("Run Parallel Screen", type="primary"):
            with st.spinner("Running parallel screen..."):
                results = screener.screen(test_symbols)

                if not results.empty:
                    st.success(f"Screened {len(results)} stocks")
                    st.dataframe(results)
                else:
                    st.warning("No results")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Test 9** - parallel processing")


if __name__ == "__main__":
    main()
