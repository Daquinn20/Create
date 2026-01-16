"""
Technical Analysis Screen - Test Version 2
Adding imports and basic structure from main file
"""
import streamlit as st
import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Page config MUST be first
st.set_page_config(
    page_title="Technical Screen Test 2",
    page_icon="📈",
    layout="wide"
)

# Load environment variables
load_dotenv()

# API Keys
FMP_API_KEY = os.getenv("FMP_API_KEY")

st.title("Technical Analysis Screen - Test 2")
st.write("Testing with imports and basic structure")

# Test if we can show a simple dataframe
data = {'Stock': ['AAPL', 'MSFT', 'GOOGL'], 'Price': [150, 350, 140]}
df = pd.DataFrame(data)
st.dataframe(df)

st.sidebar.title("Navigation")
page = st.sidebar.radio("Select", ["Page 1", "Page 2"])

if page == "Page 1":
    st.header("Page 1 - DataFrame test passed!")
elif page == "Page 2":
    st.header("Page 2")
