"""
Technical Analysis Screen - Test Version 7
Adding main() function structure with navigation
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
    page_title="Technical Screen Test 7",
    page_icon="📈",
    layout="wide"
)

# Load environment variables
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
        # Try yfinance
        try:
            stock = yf.Ticker(symbol)
            df = stock.history(period=period)
            if df is not None and not df.empty:
                return df
        except:
            pass

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
                df = df.sort_values("date").set_index("date")
                cutoff = datetime.now() - timedelta(days=days)
                df = df[df.index >= cutoff]
                df = df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
                return df[["Open", "High", "Low", "Close", "Volume"]]
        except:
            pass
        return None

    def get_quote(self, symbol: str) -> Optional[Dict]:
        try:
            url = f"{self.fmp_base}/quote/{symbol}?apikey={FMP_API_KEY}"
            response = self.session.get(url, timeout=10)
            data = response.json()
            if data and len(data) > 0:
                return data[0]
        except:
            pass
        return None


class TechnicalIndicators:
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
    def macd(data: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
        ema_fast = data.ewm(span=12, adjust=False).mean()
        ema_slow = data.ewm(span=26, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def bollinger_bands(data: pd.Series, period: int = 20) -> Tuple[pd.Series, pd.Series, pd.Series]:
        sma = data.rolling(window=period).mean()
        std = data.rolling(window=period).std()
        return sma + (std * 2), sma, sma - (std * 2)


def create_chart(df: pd.DataFrame, symbol: str, show_indicators: List[str]) -> go.Figure:
    num_subplots = 1
    subplot_titles = [symbol]
    row_heights = [0.6]

    if "Volume" in show_indicators:
        num_subplots += 1
        subplot_titles.append("Volume")
        row_heights.append(0.15)

    if "RSI" in show_indicators:
        num_subplots += 1
        subplot_titles.append("RSI")
        row_heights.append(0.15)

    if "MACD" in show_indicators:
        num_subplots += 1
        subplot_titles.append("MACD")
        row_heights.append(0.15)

    total = sum(row_heights)
    row_heights = [h/total for h in row_heights]

    fig = make_subplots(
        rows=num_subplots, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=subplot_titles,
        row_heights=row_heights
    )

    fig.add_trace(go.Candlestick(
        x=df.index, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"], name="Price"
    ), row=1, col=1)

    ti = TechnicalIndicators()

    if "SMA 20" in show_indicators:
        fig.add_trace(go.Scatter(x=df.index, y=ti.sma(df["Close"], 20), name="SMA 20",
                                  line=dict(color="blue", width=1)), row=1, col=1)

    if "SMA 50" in show_indicators:
        fig.add_trace(go.Scatter(x=df.index, y=ti.sma(df["Close"], 50), name="SMA 50",
                                  line=dict(color="orange", width=1)), row=1, col=1)

    if "Bollinger Bands" in show_indicators:
        upper, middle, lower = ti.bollinger_bands(df["Close"])
        fig.add_trace(go.Scatter(x=df.index, y=upper, name="BB Upper",
                                  line=dict(color="gray", dash="dash")), row=1, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=lower, name="BB Lower",
                                  line=dict(color="gray", dash="dash")), row=1, col=1)

    current_row = 2

    if "Volume" in show_indicators:
        colors = ["red" if df["Close"].iloc[i] < df["Open"].iloc[i] else "green" for i in range(len(df))]
        fig.add_trace(go.Bar(x=df.index, y=df["Volume"], name="Volume", marker_color=colors), row=current_row, col=1)
        current_row += 1

    if "RSI" in show_indicators:
        rsi = ti.rsi(df["Close"])
        fig.add_trace(go.Scatter(x=df.index, y=rsi, name="RSI", line=dict(color="purple")), row=current_row, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=current_row, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=current_row, col=1)
        current_row += 1

    if "MACD" in show_indicators:
        macd, signal, hist = ti.macd(df["Close"])
        fig.add_trace(go.Scatter(x=df.index, y=macd, name="MACD", line=dict(color="blue")), row=current_row, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=signal, name="Signal", line=dict(color="orange")), row=current_row, col=1)

    fig.update_layout(height=800, xaxis_rangeslider_visible=False, template="plotly_dark", showlegend=True)
    return fig


def main():
    st.title("Technical Analysis Screen - Test 7")

    fetcher = DataFetcher()

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Select Module", ["Chart Dashboard", "Stock Screener", "Signal Scanner"])

    if st.sidebar.button("Clear Cache"):
        st.cache_data.clear()
        st.sidebar.success("Cache cleared!")

    if page == "Chart Dashboard":
        st.header("Interactive Chart Dashboard")

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            symbol = st.text_input("Enter Stock Symbol", value="AAPL").upper()
        with col2:
            period = st.selectbox("Time Period", ["1mo", "3mo", "6mo", "1y", "2y"], index=3)
        with col3:
            st.write("")
            fetch_btn = st.button("Load Data", type="primary")

        st.subheader("Select Indicators")
        indicator_cols = st.columns(4)
        with indicator_cols[0]:
            show_sma_20 = st.checkbox("SMA 20", value=True)
            show_sma_50 = st.checkbox("SMA 50", value=True)
        with indicator_cols[1]:
            show_bb = st.checkbox("Bollinger Bands", value=True)
        with indicator_cols[2]:
            show_volume = st.checkbox("Volume", value=True)
            show_rsi = st.checkbox("RSI", value=True)
        with indicator_cols[3]:
            show_macd = st.checkbox("MACD", value=True)

        indicators = []
        if show_sma_20: indicators.append("SMA 20")
        if show_sma_50: indicators.append("SMA 50")
        if show_bb: indicators.append("Bollinger Bands")
        if show_volume: indicators.append("Volume")
        if show_rsi: indicators.append("RSI")
        if show_macd: indicators.append("MACD")

        if fetch_btn or symbol:
            with st.spinner(f"Loading {symbol}..."):
                df = fetcher.get_historical_data(symbol, period)
                if df is not None and not df.empty:
                    quote = fetcher.get_quote(symbol)
                    if quote:
                        metric_cols = st.columns(4)
                        with metric_cols[0]:
                            st.metric("Price", f"${quote.get('price', 0):.2f}")
                        with metric_cols[1]:
                            st.metric("Change", f"{quote.get('changesPercentage', 0):.2f}%")
                        with metric_cols[2]:
                            st.metric("Volume", f"{df['Volume'].iloc[-1]:,.0f}")
                        with metric_cols[3]:
                            mc = quote.get('marketCap', 0)
                            st.metric("Market Cap", f"${mc/1e9:.1f}B" if mc else "N/A")

                    fig = create_chart(df, symbol, indicators)
                    st.plotly_chart(fig)
                else:
                    st.error(f"Could not fetch data for {symbol}")

    elif page == "Stock Screener":
        st.header("Stock Screener")
        st.info("Screener functionality - Test 7")

    elif page == "Signal Scanner":
        st.header("Signal Scanner")
        st.info("Scanner functionality - Test 7")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Test 7** - main() structure")


if __name__ == "__main__":
    main()
