"""
Technical Analysis Screen - Test Version 6
Adding TechnicalIndicators and SignalScanner classes
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
from concurrent.futures import ThreadPoolExecutor, as_completed

# Page config MUST be first
st.set_page_config(
    page_title="Technical Screen Test 6",
    page_icon="📈",
    layout="wide"
)

# Load environment variables
load_dotenv()

def get_secret(key: str):
    """Get secret from Streamlit secrets or environment"""
    try:
        return st.secrets.get(key)
    except:
        return os.getenv(key)

FMP_API_KEY = get_secret("FMP_API_KEY")

class DataFetcher:
    """Multi-source data fetcher"""

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
                    adapter = requests.adapters.HTTPAdapter(
                        pool_connections=25,
                        pool_maxsize=25,
                        max_retries=2
                    )
                    self._session.mount('https://', adapter)
                    self._session.mount('http://', adapter)
        return self._session

    def get_historical_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        # Try yfinance first
        try:
            stock = yf.Ticker(symbol)
            df = stock.history(period=period)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            st.warning(f"yfinance failed: {e}")

        # Fallback to FMP
        try:
            days_map = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730}
            days = days_map.get(period, 365)
            url = f"{self.fmp_base}/historical-price-full/{symbol}?apikey={FMP_API_KEY}"
            response = self.session.get(url, timeout=10)
            data = response.json()

            if "historical" in data:
                df = pd.DataFrame(data["historical"])
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date")
                df = df.set_index("date")
                cutoff = datetime.now() - timedelta(days=days)
                df = df[df.index >= cutoff]
                df = df.rename(columns={
                    "open": "Open", "high": "High", "low": "Low",
                    "close": "Close", "volume": "Volume"
                })
                return df[["Open", "High", "Low", "Close", "Volume"]]
        except Exception as e:
            st.warning(f"FMP failed: {e}")

        return None


class TechnicalIndicators:
    """Calculate technical indicators"""

    @staticmethod
    def sma(data: pd.Series, period: int) -> pd.Series:
        return data.rolling(window=period).mean()

    @staticmethod
    def ema(data: pd.Series, period: int) -> pd.Series:
        return data.ewm(span=period, adjust=False).mean()

    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def macd(data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        ema_fast = data.ewm(span=fast, adjust=False).mean()
        ema_slow = data.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def bollinger_bands(data: pd.Series, period: int = 20, std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        sma = data.rolling(window=period).mean()
        std = data.rolling(window=period).std()
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        return upper, sma, lower

    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()

    @staticmethod
    def stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
                   k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        lowest_low = low.rolling(window=k_period).min()
        highest_high = high.rolling(window=k_period).max()
        k = 100 * (close - lowest_low) / (highest_high - lowest_low)
        d = k.rolling(window=d_period).mean()
        return k, d

    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        plus_dm = high.diff()
        minus_dm = -low.diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0

        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.rolling(window=period).mean()
        plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)

        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(window=period).mean()
        return adx


class SignalScanner:
    """Scan for trading signals"""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.ti = TechnicalIndicators()
        self._calculate_indicators()

    def _calculate_indicators(self):
        close = self.df["Close"]
        high = self.df["High"]
        low = self.df["Low"]
        volume = self.df["Volume"]

        self.df["SMA_20"] = self.ti.sma(close, 20)
        self.df["SMA_50"] = self.ti.sma(close, 50)
        self.df["SMA_200"] = self.ti.sma(close, 200)
        self.df["RSI"] = self.ti.rsi(close)
        self.df["MACD"], self.df["MACD_Signal"], self.df["MACD_Hist"] = self.ti.macd(close)
        self.df["BB_Upper"], self.df["BB_Middle"], self.df["BB_Lower"] = self.ti.bollinger_bands(close)

    def get_signals(self) -> Dict[str, str]:
        if len(self.df) < 2:
            return {}

        signals = {}
        current = self.df.iloc[-1]

        # RSI
        if current["RSI"] > 70:
            signals["RSI"] = "OVERBOUGHT"
        elif current["RSI"] < 30:
            signals["RSI"] = "OVERSOLD"
        else:
            signals["RSI"] = f"Neutral ({current['RSI']:.1f})"

        # MACD
        if current["MACD"] > current["MACD_Signal"]:
            signals["MACD"] = "Bullish"
        else:
            signals["MACD"] = "Bearish"

        return signals


st.title("Technical Analysis Screen - Test 6")
st.write("Testing with TechnicalIndicators and SignalScanner classes")

# Show API key status
if FMP_API_KEY:
    st.success(f"FMP API Key loaded: {FMP_API_KEY[:8]}...")
else:
    st.warning("FMP API Key not found - check secrets/env")

fetcher = DataFetcher()

symbol = st.text_input("Enter symbol", value="AAPL")
if st.button("Analyze"):
    with st.spinner("Loading..."):
        df = fetcher.get_historical_data(symbol, "1y")
        if df is not None and not df.empty:
            st.success(f"Loaded {len(df)} rows for {symbol}")

            # Run signal scanner
            scanner = SignalScanner(df)
            signals = scanner.get_signals()

            st.subheader("Signals")
            for key, value in signals.items():
                st.write(f"**{key}:** {value}")

            # Chart
            fig = go.Figure()
            fig.add_trace(go.Candlestick(
                x=df.index,
                open=df['Open'],
                high=df['High'],
                low=df['Low'],
                close=df['Close'],
                name=symbol
            ))
            fig.update_layout(title=f"{symbol} Chart", height=500)
            st.plotly_chart(fig)
        else:
            st.error("Could not fetch data")

st.sidebar.title("Navigation")
page = st.sidebar.radio("Select", ["Chart", "Test"])
