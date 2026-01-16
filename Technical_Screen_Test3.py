"""
Technical Analysis Screen - Test Version 3
Adding cache decorators and file loading
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
from typing import Optional, Dict, List

# Page config MUST be first
st.set_page_config(
    page_title="Technical Screen Test 3",
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
def load_stock_index() -> pd.DataFrame:
    """Load stock universe from Excel file"""
    try:
        df = pd.read_excel(INDEX_FILE)
        return df
    except Exception as e:
        st.error(f"Could not load index file: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_sp500() -> pd.DataFrame:
    """Load S&P 500 list"""
    try:
        df = pd.read_excel(SP500_FILE)
        return df
    except Exception as e:
        return pd.DataFrame()

st.title("Technical Analysis Screen - Test 3")
st.write("Testing with cache decorators and file loading")

# Test loading files
if st.button("Load SP500"):
    df = load_sp500()
    if not df.empty:
        st.success(f"Loaded {len(df)} stocks")
        st.dataframe(df.head(10))
    else:
        st.warning("Could not load file")

# Test plotly chart
st.subheader("Plotly Test")
fig = go.Figure()
fig.add_trace(go.Scatter(x=[1,2,3,4], y=[10,11,12,13], mode='lines', name='Test'))
fig.update_layout(title="Test Chart", height=400)
st.plotly_chart(fig)

st.sidebar.title("Navigation")
page = st.sidebar.radio("Select", ["Page 1", "Page 2"])
