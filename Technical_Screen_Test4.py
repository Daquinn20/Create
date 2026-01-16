"""
Technical Analysis Screen - Test Version 4
Adding DataFetcher class and threading
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
    page_title="Technical Screen Test 4",
    page_icon="📈",
    layout="wide"
)

# Load environment variables
load_dotenv()

# API Keys
FMP_API_KEY = os.getenv("FMP_API_KEY")

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
        return self._session

    def get_historical_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        """Fetch historical data"""
        try:
            stock = yf.Ticker(symbol)
            df = stock.history(period=period)
            return df
        except:
            return None

st.title("Technical Analysis Screen - Test 4")
st.write("Testing with DataFetcher class and threading")

fetcher = DataFetcher()

symbol = st.text_input("Enter symbol", value="AAPL")
if st.button("Fetch Data"):
    with st.spinner("Loading..."):
        df = fetcher.get_historical_data(symbol, "3mo")
        if df is not None and not df.empty:
            st.success(f"Loaded {len(df)} rows for {symbol}")

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
