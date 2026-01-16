"""
Technical Analysis Screen - Test Version 5
Adding email imports and full DataFetcher with HTTPAdapter
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
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import io

# Page config MUST be first
st.set_page_config(
    page_title="Technical Screen Test 5",
    page_icon="📈",
    layout="wide"
)

# Load environment variables
load_dotenv()

# API Keys
FMP_API_KEY = os.getenv("FMP_API_KEY")

# File paths
INDEX_FILE = Path(__file__).parent / "Index_Broad_US.xlsx"
SP500_FILE = Path(__file__).parent / "SP500_list_with_sectors.xlsx"

@st.cache_data(ttl=3600)
def load_sp500() -> pd.DataFrame:
    """Load S&P 500 list"""
    try:
        df = pd.read_excel(SP500_FILE)
        return df
    except Exception as e:
        return pd.DataFrame()

class DataFetcher:
    """Multi-source data fetcher with connection pooling"""

    def __init__(self):
        self.fmp_base = "https://financialmodelingprep.com/api/v3"
        self._session = None
        self._session_lock = threading.Lock()

    @property
    def session(self) -> requests.Session:
        """Thread-safe session getter with connection pooling"""
        if self._session is None:
            with self._session_lock:
                if self._session is None:
                    self._session = requests.Session()
                    # Connection pooling
                    adapter = requests.adapters.HTTPAdapter(
                        pool_connections=25,
                        pool_maxsize=25,
                        max_retries=2
                    )
                    self._session.mount('https://', adapter)
                    self._session.mount('http://', adapter)
        return self._session

    def get_historical_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        """Fetch historical data"""
        try:
            stock = yf.Ticker(symbol)
            df = stock.history(period=period)
            return df
        except:
            return None

st.title("Technical Analysis Screen - Test 5")
st.write("Testing with email imports and HTTPAdapter")

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
