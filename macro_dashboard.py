import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from fredapi import Fred
from datetime import datetime, timedelta
import os
import requests
import io
import uuid
import hashlib
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Macro Dashboard", page_icon="📊", layout="wide")

# Initialize API keys
FMP_API_KEY = os.getenv("FMP_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")

# Initialize FRED API
@st.cache_resource
def get_fred():
    if not FRED_API_KEY:
        st.error("FRED_API_KEY not found in .env file")
        st.stop()
    return Fred(api_key=FRED_API_KEY)

fred = get_fred()

# FMP API Functions
@st.cache_data(ttl=3600)
def get_fmp_economic_indicator(indicator_name, from_date=None, to_date=None):
    """Fetch economic indicator from FMP API"""
    try:
        url = f"https://financialmodelingprep.com/stable/economic-indicators"
        params = {"name": indicator_name, "apikey": FMP_API_KEY}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                df.set_index('date', inplace=True)
                return df['value']
        return pd.Series()
    except Exception as e:
        st.warning(f"Could not fetch FMP {indicator_name}: {e}")
        return pd.Series()

@st.cache_data(ttl=3600)
def get_fmp_treasury_rates(from_date=None, to_date=None):
    """Fetch Treasury rates from FMP API"""
    try:
        url = f"https://financialmodelingprep.com/stable/treasury-rates"
        params = {"apikey": FMP_API_KEY}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                df.set_index('date', inplace=True)
                return df
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not fetch FMP Treasury rates: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_fmp_market_risk_premium():
    """Fetch Market Risk Premium from FMP API"""
    try:
        url = f"https://financialmodelingprep.com/stable/market-risk-premium"
        params = {"apikey": FMP_API_KEY}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return data
        return None
    except Exception as e:
        st.warning(f"Could not fetch Market Risk Premium: {e}")
        return None

@st.cache_data(ttl=3600)
def get_fmp_index_historical(symbol, from_date=None):
    """Fetch historical index data from FMP API (e.g., ^GSPC for S&P 500)"""
    try:
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
        params = {"apikey": FMP_API_KEY}
        if from_date:
            params["from"] = from_date

        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'historical' in data:
                df = pd.DataFrame(data['historical'])
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                df.set_index('date', inplace=True)
                return df['close']
        return pd.Series()
    except Exception as e:
        st.warning(f"Could not fetch FMP {symbol}: {e}")
        return pd.Series()

# FMP Economic Indicators mapping
FMP_INDICATORS = {
    "GDP": "GDP",
    "Real GDP": "realGDP",
    "Real GDP Per Capita": "realGDPPerCapita",
    "Federal Funds Rate": "federalFunds",
    "CPI": "CPI",
    "Inflation Rate": "inflationRate",
    "Retail Sales": "retailSales",
    "Consumer Sentiment": "consumerSentiment",
    "Durable Goods": "durableGoods",
    "Unemployment Rate": "unemploymentRate",
    "Nonfarm Payrolls": "totalNonfarmPayroll",
    "Initial Claims": "initialClaims",
    "Industrial Production": "industrialProductionTotalIndex",
    "Housing Starts": "newPrivatelyOwnedHousingUnitsStartedTotalUnits",
    "Vehicle Sales": "totalVehicleSales",
    "Recession Probability": "smoothedUSRecessionProbabilities",
    "30Y Mortgage Rate": "30YearFixedRateMortgageAverage",
    "15Y Mortgage Rate": "15YearFixedRateMortgageAverage",
    "Trade Balance": "tradeBalanceGoodsAndServices",
}

# Cache FRED data fetching - shorter TTL to avoid stale cache issues
@st.cache_data(ttl=1800, show_spinner=False)
def get_fred_series(series_id, start_date=None):
    try:
        if start_date:
            data = fred.get_series(series_id, observation_start=start_date)
        else:
            data = fred.get_series(series_id)
        if data is None:
            return pd.Series()
        result = data.dropna()
        return result
    except Exception as e:
        # Return empty series but don't cache errors
        return pd.Series()

def get_series_data(series_id, start_date=None):
    """Unified function to get data from either FRED or FMP based on series_id prefix"""
    try:
        if series_id.startswith("FMP:"):
            # FMP data source
            symbol = series_id.replace("FMP:", "")
            data = get_fmp_index_historical(symbol, start_date)
            # If no data for requested range, try getting all available data
            if data is None or data.empty:
                if start_date:
                    data = get_fmp_index_historical(symbol, None)
            return data if data is not None else pd.Series()
        else:
            # FRED data source
            data = get_fred_series(series_id, start_date)
            # If no data for requested range, try getting all available data
            if data is None or data.empty:
                if start_date:
                    data = get_fred_series(series_id, None)
            return data if data is not None else pd.Series()
    except Exception as e:
        st.warning(f"Error fetching {series_id}: {e}")
        return pd.Series()

# Get recession data for shading
@st.cache_data(ttl=86400)
def get_recession_periods(start_date=None):
    """Fetch NBER recession dates and return as list of (start, end) tuples"""
    try:
        if start_date:
            rec_data = fred.get_series('USREC', observation_start=start_date)
        else:
            rec_data = fred.get_series('USREC')

        rec_data = rec_data.dropna()

        # Find recession periods (where USREC = 1)
        recession_periods = []
        in_recession = False
        start = None

        for date, value in rec_data.items():
            if value == 1 and not in_recession:
                start = date
                in_recession = True
            elif value == 0 and in_recession:
                recession_periods.append((start, date))
                in_recession = False

        # If still in recession at end of data
        if in_recession:
            recession_periods.append((start, rec_data.index[-1]))

        return recession_periods
    except Exception as e:
        return []

@st.cache_data(ttl=3600)
def get_series_info(series_id):
    try:
        return fred.get_series_info(series_id)
    except:
        return None

# Define macro indicators
MACRO_SERIES = {
    "Fiscal & Debt": {
        "U.S. GDP": "GDP",
        "Real GDP": "GDPC1",
        "Total Public Debt": "GFDEBTN",
        "Federal Debt % of GDP": "GFDEGDQ188S",
        "Federal Tax Revenue": "FGRECPT",
        "Federal Deficit/Surplus": "FYFSD",
        "Interest Payments (Quarterly)": "A091RC1Q027SBEA",
        "Interest as % of GDP": "FYOIGDA188S",
        "NIIP (Net Intl Investment)": "IIPUSNETIQ",
    },
    "Liquidity & Fed": {
        "Fed Balance Sheet (Total Assets)": "WALCL",
        "Treasury General Account (TGA)": "WTREGEN",
        "Reverse Repo (ON RRP)": "RRPONTSYD",
        "Bank Reserves": "TOTRESNS",
        "Household Deposits & Currency": "BOGZ1FL193020005Q",
        "M2 Money Supply": "M2SL",
    },
    "Employment Detailed": {
        "Government Payrolls": "USGOVT",
        "Private Payrolls": "USPRIV",
        "Total Nonfarm Payrolls": "PAYEMS",
        "Unemployment Rate": "UNRATE",
        "Labor Force Participation": "CIVPART",
        "Job Openings Rate": "JTSJOR",
        "Job Openings Level": "JTSJOL",
        "Avg Weekly Hours": "AWHAETP",
        "Temp Help Services": "TEMPHELPS",
    },
    "Jobless Claims": {
        "Initial Claims": "ICSA",
        "Initial Claims (4W MA)": "IC4WSA",
        "Continued Claims": "CCSA",
        "Insured Unemployment Rate": "IURSA",
    },
    "Consumer & Retail": {
        "Retail Sales": "RSAFS",
        "Retail Sales ex Autos": "RSFSXMV",
        "Auto Sales (Total)": "TOTALSA",
        "Real Disposable Income": "DSPIC96",
        "Personal Savings Rate": "PSAVERT",
        "Consumer Sentiment (UMich)": "UMCSENT",
        "Consumer Confidence": "CSCICP03USM665S",
    },
    "Credit & Delinquency": {
        "Credit Card Delinquency 90+": "DRCCLACBS",
        "Consumer Loans Delinquency": "DRCLACBS",
        "Household Debt Service Ratio": "TDSP",
        "Consumer Credit Outstanding": "TOTALSL",
        "Revolving Credit": "REVOLSL",
    },
    "Housing": {
        "Housing Inventory": "ACTLISCOUUS",
        "Case-Shiller Home Price": "CSUSHPINSA",
        "Median Home Price": "MSPUS",
        "Housing Starts": "HOUST",
        "Building Permits": "PERMIT",
        "New Home Sales": "HSN1F",
        "Existing Home Sales": "EXHOSLUSM495S",
        "30Y Mortgage Rate": "MORTGAGE30US",
    },
    "ISM & Manufacturing": {
        "ISM Manufacturing PMI": "NAPM",
        "ISM New Orders": "NAPMNOI",
        "ISM Employment": "NAPMEI",
        "ISM Prices": "NAPMPRI",
        "Industrial Production": "INDPRO",
        "Capacity Utilization": "TCU",
        "Durable Goods Orders": "DGORDER",
    },
    "ISM Services": {
        "ISM Services PMI": "NMFBAI",
        "ISM Services Employment": "NMFEI",
        "ISM Services New Orders": "NMFNOI",
        "ISM Services Prices": "NMFPI",
    },
    "Regional Fed Surveys": {
        "Philadelphia Fed": "GACDFSA066MSFRBPHI",
        "Empire State (NY Fed)": "GACDISA066MSFRBNY",
        "Dallas Fed": "BACTSAMFRBDAL",
        "Chicago Fed National Activity": "CFNAI",
        "Richmond Fed": "RICFRBMFG",
        "Kansas City Fed": "FRBKCLMCIM",
    },
    "Corporate Health": {
        "Corporate Profits": "CP",
        "Corporate Profits After Tax": "CPATAX",
        "Nonfinancial Corp Debt/GDP": "NCBCMDPMVCE",
        "Business Inventories": "BUSINV",
        "Mfg Inventories/Sales Ratio": "MNFCTRIRSA",
    },
    "Inflation": {
        "CPI (All Items)": "CPIAUCSL",
        "CPI YoY": "CPIAUCSL",
        "Core CPI": "CPILFESL",
        "PCE Price Index": "PCEPI",
        "Core PCE": "PCEPILFE",
        "5Y Breakeven Inflation": "T5YIE",
        "10Y Breakeven Inflation": "T10YIE",
    },
    "Interest Rates": {
        "Fed Funds Rate": "DFF",
        "2-Year Treasury": "DGS2",
        "10-Year Treasury": "DGS10",
        "30-Year Treasury": "DGS30",
        "10Y-2Y Spread": "T10Y2Y",
        "10Y-3M Spread": "T10Y3M",
        "High Yield Spread": "BAMLH0A0HYM2",
    },
    "Markets": {
        "S&P 500 (FMP)": "FMP:^GSPC",
        "WTI Crude Oil": "DCOILWTICO",
        "3-Month T-Bill": "DTB3",
        "10-Year Treasury Yield": "DGS10",
        "30-Year Treasury Yield": "DGS30",
        "Gold Price": "GOLDAMGBD228NLBM",
        "US Dollar Index": "DTWEXBGS",
        "Bitcoin": "CBBTCUSD",
        "VIX": "VIXCLS",
    },
}

# Sidebar
st.sidebar.title("📊 Macro Dashboard")
st.sidebar.markdown("Data from **FRED** & **FMP** APIs")

# Clear cache button
if st.sidebar.button("🔄 Clear Cache & Reload"):
    st.cache_data.clear()
    st.rerun()

# Open Excel file button
if st.sidebar.button("📊 Open Macro Data Excel"):
    import subprocess
    excel_path = r"C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\Macro\Macro Data.xlsx"
    try:
        subprocess.Popen(['start', '', excel_path], shell=True)
        st.sidebar.success("Opening Excel file...")
    except Exception as e:
        st.sidebar.error(f"Could not open file: {e}")

# Data source selector
data_source = st.sidebar.radio("Data Source", ["FRED", "FMP", "Both"])

# Time range selector
time_options = {
    "1 Year": 365,
    "2 Years": 730,
    "5 Years": 1825,
    "10 Years": 3650,
    "20 Years": 7300,
    "30 Years": 10950,
    "40 Years": 14600,
    "50 Years": 18250,
    "All Time": None,
}
selected_time = st.sidebar.selectbox("Time Range", list(time_options.keys()), index=2)
days = time_options[selected_time]
start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d") if days else None

# Category selector based on data source
if data_source == "FMP":
    fmp_categories = ["FMP Economic Indicators", "FMP Treasury Rates", "FMP Market Risk Premium"]
    selected_category = st.sidebar.selectbox("Category", fmp_categories)
else:
    category_options = ["All Categories"] + list(MACRO_SERIES.keys())
    selected_category = st.sidebar.selectbox("Category", category_options)

# Main content
st.title("📊 Macro Economic Dashboard")
source_text = "FRED & FMP" if data_source == "Both" else data_source
st.markdown(f"**Data Source:** {source_text} | **Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# Display selected category
st.header(f"{selected_category}")

# Get recession periods once for the session
recession_periods = get_recession_periods(start_date)

# Helper function to add recession shading to a figure
def add_recession_shading(fig, data_index):
    """Add gray recession shading to a plotly figure"""
    for rec_start, rec_end in recession_periods:
        # Only add shading if it overlaps with data range
        if rec_end >= data_index.min() and rec_start <= data_index.max():
            fig.add_vrect(
                x0=rec_start,
                x1=rec_end,
                fillcolor="rgba(128, 128, 128, 0.3)",
                layer="below",
                line_width=0,
            )
    return fig

# Helper function to display metrics with chart
def display_metric_with_chart(name, data, is_percent=False, is_index=False, series_id=None):
    # Validate data
    if data is None or not isinstance(data, pd.Series) or data.empty or len(data) == 0:
        st.warning(f"No data available for {name}")
        return

    try:
        current = data.iloc[-1]
        prev = data.iloc[-2] if len(data) > 1 else current
        change = current - prev
        pct_change = (change / prev * 100) if prev != 0 else 0

        if is_percent:
            formatted = f"{current:.2f}%"
        elif is_index:
            formatted = f"{current:.1f}"
        elif abs(current) > 1000:
            formatted = f"{current:,.0f}"
        else:
            formatted = f"{current:.2f}"

        delta_str = f"{change:+.2f} ({pct_change:+.1f}%)"
        st.metric(label=name, value=formatted, delta=delta_str)

        fig = px.line(x=data.index, y=data.values)

        # Add recession shading
        fig = add_recession_shading(fig, data.index)

        fig.update_layout(
            height=250,
            margin=dict(l=40, r=20, t=20, b=40),
            xaxis=dict(
                visible=True,
                showgrid=True,
                gridcolor='rgba(128,128,128,0.2)',
                tickformat='%Y',
                dtick="M24",
            ),
            yaxis=dict(
                visible=True,
                showgrid=True,
                gridcolor='rgba(128,128,128,0.2)',
                tickformat=',.0f' if abs(data.max()) > 1000 else '.2f',
            ),
            showlegend=False,
            hovermode='x unified',
            autosize=True,
        )
        fig.update_traces(line_color="#1f77b4")

        # Use hash-based key for stability
        key_base = f"{name}_{series_id}_{selected_time}"
        chart_key = f"chart_{hashlib.md5(key_base.encode()).hexdigest()[:12]}"

        st.plotly_chart(fig, key=chart_key, use_container_width=True)
    except Exception as e:
        st.error(f"Error displaying {name}: {str(e)}")

# FMP Data Display
if data_source == "FMP":
    if selected_category == "FMP Economic Indicators":
        fmp_display = {
            "GDP": ("GDP", False, False),
            "Real GDP": ("realGDP", False, False),
            "CPI": ("CPI", False, True),
            "Inflation Rate": ("inflationRate", True, False),
            "Unemployment Rate": ("unemploymentRate", True, False),
            "Federal Funds Rate": ("federalFunds", True, False),
            "Consumer Sentiment": ("consumerSentiment", False, True),
            "Industrial Production": ("industrialProductionTotalIndex", False, True),
            "Retail Sales": ("retailSales", False, False),
            "Durable Goods": ("durableGoods", False, False),
            "Nonfarm Payrolls": ("totalNonfarmPayroll", False, False),
            "Initial Claims": ("initialClaims", False, False),
            "Housing Starts": ("newPrivatelyOwnedHousingUnitsStartedTotalUnits", False, False),
            "30Y Mortgage Rate": ("30YearFixedRateMortgageAverage", True, False),
            "Recession Probability": ("smoothedUSRecessionProbabilities", True, False),
        }

        cols = st.columns(3)
        for idx, (name, (indicator, is_pct, is_idx)) in enumerate(fmp_display.items()):
            with cols[idx % 3]:
                data = get_fmp_economic_indicator(indicator, start_date)
                display_metric_with_chart(name, data, is_pct, is_idx)

    elif selected_category == "FMP Treasury Rates":
        treasury_df = get_fmp_treasury_rates(start_date)
        if not treasury_df.empty:
            rate_cols = [c for c in treasury_df.columns if c != 'date']
            cols = st.columns(3)
            for idx, col_name in enumerate(rate_cols[:9]):
                with cols[idx % 3]:
                    data = treasury_df[col_name].dropna()
                    display_name = col_name.replace("month", "M").replace("year", "Y")
                    display_metric_with_chart(display_name, data, is_percent=True)

    elif selected_category == "FMP Market Risk Premium":
        mrp_data = get_fmp_market_risk_premium()
        if mrp_data:
            st.subheader("Market Risk Premium by Country")
            df_mrp = pd.DataFrame(mrp_data)

            # Display US Market Risk Premium prominently
            us_data = df_mrp[df_mrp['country'] == 'United States']
            if not us_data.empty:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("US Market Risk Premium", f"{us_data['marketRiskPremium'].values[0]:.2f}%")
                with col2:
                    st.metric("US Total Equity Risk Premium", f"{us_data['totalEquityRiskPremium'].values[0]:.2f}%")
                with col3:
                    st.metric("US Country Risk Premium", f"{us_data['countryRiskPremium'].values[0]:.2f}%")

            st.divider()
            st.subheader("All Countries")
            st.dataframe(df_mrp, use_container_width=True, hide_index=True)

# FRED Data Display
elif data_source == "FRED":
    if selected_category == "All Categories":
        # Display all categories in order
        for category_name, series_dict in MACRO_SERIES.items():
            st.header(category_name)
            cols = st.columns(min(len(series_dict), 3))

            for idx, (name, series_id) in enumerate(series_dict.items()):
                with cols[idx % 3]:
                    data = get_series_data(series_id, start_date)
                    is_pct = series_id in ["UNRATE", "DFF", "DGS2", "DGS10", "DGS30", "DTB3", "MORTGAGE30US", "CIVPART", "TCU", "PSAVERT", "FYOIGDA188S", "GFDEGDQ188S", "JTSJOR", "TDSP", "DRCCLACBS", "DRCLACBS", "IURSA", "T5YIE", "T10YIE", "BAMLH0A0HYM2"]
                    is_idx = series_id in ["T10Y2Y", "T10Y3M", "NAPM", "NAPMNOI", "NAPMEI", "NAPMPRI", "CFNAI", "GACDFSA066MSFRBPHI", "GACDISA066MSFRBNY", "BACTSAMFRBDAL", "RICFRBMFG", "FRBKCLMCIM", "NMFBAI", "NMFEI", "NMFNOI", "NMFPI", "CSUSHPINSA", "UMCSENT", "CSCICP03USM665S", "MNFCTRIRSA"]
                    display_metric_with_chart(name, data, is_pct, is_idx, series_id)

            st.divider()
    else:
        series_dict = MACRO_SERIES[selected_category]
        cols = st.columns(min(len(series_dict), 3))

        for idx, (name, series_id) in enumerate(series_dict.items()):
            with cols[idx % 3]:
                data = get_series_data(series_id, start_date)
                is_pct = series_id in ["UNRATE", "DFF", "DGS2", "DGS10", "DGS30", "DTB3", "MORTGAGE30US", "CIVPART", "TCU", "PSAVERT", "FYOIGDA188S", "GFDEGDQ188S", "JTSJOR", "TDSP", "DRCCLACBS", "DRCLACBS", "IURSA", "T5YIE", "T10YIE", "BAMLH0A0HYM2"]
                is_idx = series_id in ["T10Y2Y", "T10Y3M", "NAPM", "NAPMNOI", "NAPMEI", "NAPMPRI", "CFNAI", "GACDFSA066MSFRBPHI", "GACDISA066MSFRBNY", "BACTSAMFRBDAL", "RICFRBMFG", "FRBKCLMCIM", "NMFBAI", "NMFEI", "NMFNOI", "NMFPI", "CSUSHPINSA", "UMCSENT", "CSCICP03USM665S", "MNFCTRIRSA"]
                display_metric_with_chart(name, data, is_pct, is_idx, series_id)

# Both Sources - Side by Side Comparison
else:
    if selected_category == "All Categories":
        # Display all FRED categories
        for category_name, cat_series_dict in MACRO_SERIES.items():
            st.header(f"FRED: {category_name}")
            cols = st.columns(min(len(cat_series_dict), 3))
            for idx, (name, series_id) in enumerate(cat_series_dict.items()):
                with cols[idx % 3]:
                    data = get_series_data(series_id, start_date)
                    is_pct = series_id in ["UNRATE", "DFF", "DGS2", "DGS10", "DGS30", "DTB3", "MORTGAGE30US", "CIVPART", "TCU", "PSAVERT", "FYOIGDA188S", "GFDEGDQ188S", "JTSJOR", "TDSP", "DRCCLACBS", "DRCLACBS", "IURSA", "T5YIE", "T10YIE", "BAMLH0A0HYM2"]
                    is_idx = series_id in ["T10Y2Y", "T10Y3M", "NAPM", "NAPMNOI", "NAPMEI", "NAPMPRI", "CFNAI", "GACDFSA066MSFRBPHI", "GACDISA066MSFRBNY", "BACTSAMFRBDAL", "RICFRBMFG", "FRBKCLMCIM", "NMFBAI", "NMFEI", "NMFNOI", "NMFPI", "CSUSHPINSA", "UMCSENT", "CSCICP03USM665S", "MNFCTRIRSA"]
                    display_metric_with_chart(name, data, is_pct, is_idx, series_id)
            st.divider()
    else:
        st.subheader("FRED Data")
        series_dict = MACRO_SERIES[selected_category]
        cols = st.columns(min(len(series_dict), 3))
        for idx, (name, series_id) in enumerate(series_dict.items()):
            with cols[idx % 3]:
                data = get_series_data(series_id, start_date)
                is_pct = series_id in ["UNRATE", "DFF", "DGS2", "DGS10", "DGS30", "DTB3", "MORTGAGE30US", "CIVPART", "TCU", "PSAVERT", "FYOIGDA188S", "GFDEGDQ188S", "JTSJOR", "TDSP", "DRCCLACBS", "DRCLACBS", "IURSA", "T5YIE", "T10YIE", "BAMLH0A0HYM2"]
                is_idx = series_id in ["T10Y2Y", "T10Y3M", "NAPM", "NAPMNOI", "NAPMEI", "NAPMPRI", "CFNAI", "GACDFSA066MSFRBPHI", "GACDISA066MSFRBNY", "BACTSAMFRBDAL", "RICFRBMFG", "FRBKCLMCIM", "NMFBAI", "NMFEI", "NMFNOI", "NMFPI", "CSUSHPINSA", "UMCSENT", "CSCICP03USM665S", "MNFCTRIRSA"]
                display_metric_with_chart(name, data, is_pct, is_idx, series_id)

    st.divider()
    st.subheader("FMP Economic Data")
    fmp_key_indicators = {
        "GDP": ("GDP", False, False),
        "Real GDP": ("realGDP", False, False),
        "Unemployment Rate": ("unemploymentRate", True, False),
        "CPI": ("CPI", False, True),
        "Fed Funds Rate": ("federalFunds", True, False),
        "Industrial Production": ("industrialProductionTotalIndex", False, True),
    }
    cols = st.columns(3)
    for idx, (name, (indicator, is_pct, is_idx)) in enumerate(fmp_key_indicators.items()):
        with cols[idx % 3]:
            data = get_fmp_economic_indicator(indicator, start_date)
            display_metric_with_chart(name, data, is_pct, is_idx)

st.divider()

# Detailed chart section
st.header("Detailed View")

# Build all series options for detailed view
if data_source == "FRED" and selected_category == "All Categories":
    all_series_options = {}
    for cat_name, cat_series in MACRO_SERIES.items():
        for name, sid in cat_series.items():
            all_series_options[f"{cat_name}: {name}"] = sid
    series_dict = all_series_options
elif data_source == "FRED":
    series_dict = MACRO_SERIES[selected_category]
elif data_source == "Both":
    series_dict = MACRO_SERIES.get(selected_category, {})

if series_dict:
    selected_series = st.selectbox(
        "Select Series",
        list(series_dict.keys()),
        key="detailed_series"
    )
    series_id = series_dict[selected_series]
    data = get_series_data(series_id, start_date)

    if not data.empty:
        # Only get series info for FRED series (not FMP)
        if not series_id.startswith("FMP:"):
            info = get_series_info(series_id)
            if info is not None:
                st.caption(f"**{info.get('title', selected_series)}** | Frequency: {info.get('frequency', 'N/A')} | Units: {info.get('units', 'N/A')}")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            mode='lines',
            name=selected_series,
            fill='tozeroy',
            fillcolor='rgba(31, 119, 180, 0.2)',
            line=dict(color='#1f77b4', width=2)
        ))

        # Add recession shading
        fig = add_recession_shading(fig, data.index)

        fig.update_layout(
            title=selected_series,
            xaxis_title="Date",
            yaxis_title="Value",
            height=400,
            hovermode='x unified'
        )

        st.plotly_chart(fig, key=f"detailed_view_{uuid.uuid4().hex[:8]}")

        # Stats
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Current", f"{data.iloc[-1]:.2f}")
        col2.metric("Period High", f"{data.max():.2f}")
        col3.metric("Period Low", f"{data.min():.2f}")
        col4.metric("Period Avg", f"{data.mean():.2f}")

st.divider()

# Key Economic Overview
st.header("Key Liquidity & Manufacturing Overview")

overview_series = {
    "Fed Balance Sheet": "WALCL",
    "Treasury General Account": "WTREGEN",
    "Interest Payments": "A091RC1Q027SBEA",
    "ISM Manufacturing": "NAPM",
    "Philly Fed": "GACDFSA066MSFRBPHI",
    "Empire State": "GACDISA066MSFRBNY",
}

overview_data = {}
for name, sid in overview_series.items():
    data = get_series_data(sid, start_date)
    if not data.empty:
        overview_data[name] = data

if overview_data:
    fig = make_subplots(
        rows=2, cols=3,
        subplot_titles=list(overview_data.keys()),
        vertical_spacing=0.15,
        horizontal_spacing=0.08
    )

    for idx, (name, data) in enumerate(overview_data.items()):
        row = idx // 3 + 1
        col = idx % 3 + 1

        fig.add_trace(
            go.Scatter(x=data.index, y=data.values, mode='lines', name=name, showlegend=False),
            row=row, col=col
        )

    # Add recession shading to all subplots
    for rec_start, rec_end in recession_periods:
        fig.add_vrect(
            x0=rec_start,
            x1=rec_end,
            fillcolor="rgba(128, 128, 128, 0.3)",
            layer="below",
            line_width=0,
            row="all",
            col="all",
        )

    fig.update_layout(
        height=600,
        title_text=f"Key Indicators - {selected_time}"
    )
    fig.update_xaxes(tickformat='%Y', dtick="M24")
    fig.update_yaxes(tickformat=',.0f')
    st.plotly_chart(fig, key=f"overview_chart_{uuid.uuid4().hex[:8]}")

# CSV Download Section
st.divider()
st.header("Download Raw Data (CSV)")

def convert_series_to_csv(series_data, name):
    """Convert a pandas Series to CSV string"""
    df = pd.DataFrame(series_data)
    df.columns = [name]
    df.index.name = 'Date'
    return df.to_csv()

def convert_df_to_csv(df):
    """Convert a pandas DataFrame to CSV string"""
    return df.to_csv()

# Download options based on data source
if data_source == "FMP":
    if selected_category == "FMP Economic Indicators":
        st.subheader("FMP Economic Indicators")
        fmp_download_data = {}
        for name, indicator in FMP_INDICATORS.items():
            data = get_fmp_economic_indicator(indicator, start_date)
            if not data.empty:
                fmp_download_data[name] = data

        if fmp_download_data:
            # Combined CSV
            combined_df = pd.DataFrame(fmp_download_data)
            combined_df.index.name = 'Date'
            st.download_button(
                label="Download All FMP Indicators (CSV)",
                data=convert_df_to_csv(combined_df),
                file_name=f"fmp_economic_indicators_{selected_time.replace(' ', '_')}.csv",
                mime="text/csv"
            )

            # Individual downloads
            st.write("**Individual Series:**")
            cols = st.columns(4)
            for idx, (name, data) in enumerate(fmp_download_data.items()):
                with cols[idx % 4]:
                    st.download_button(
                        label=f"{name}",
                        data=convert_series_to_csv(data, name),
                        file_name=f"fmp_{name.replace(' ', '_').lower()}.csv",
                        mime="text/csv",
                        key=f"fmp_{name}"
                    )

    elif selected_category == "FMP Treasury Rates":
        treasury_df = get_fmp_treasury_rates(start_date)
        if not treasury_df.empty:
            st.download_button(
                label="Download Treasury Rates (CSV)",
                data=convert_df_to_csv(treasury_df),
                file_name=f"fmp_treasury_rates_{selected_time.replace(' ', '_')}.csv",
                mime="text/csv"
            )

    elif selected_category == "FMP Market Risk Premium":
        mrp_data = get_fmp_market_risk_premium()
        if mrp_data:
            df_mrp = pd.DataFrame(mrp_data)
            st.download_button(
                label="Download Market Risk Premium (CSV)",
                data=convert_df_to_csv(df_mrp),
                file_name="fmp_market_risk_premium.csv",
                mime="text/csv"
            )

elif data_source == "FRED":
    if selected_category == "All Categories":
        st.subheader("FRED - All Categories")
        # Prepare all data button
        if st.button("Prepare All FRED Data"):
            with st.spinner("Fetching all data..."):
                all_fred_data = {}
                for category, series in MACRO_SERIES.items():
                    for name, series_id in series.items():
                        data = get_series_data(series_id, start_date)
                        if not data.empty:
                            all_fred_data[f"{category} - {name}"] = data

                if all_fred_data:
                    all_df = pd.DataFrame(all_fred_data)
                    all_df.index.name = 'Date'
                    st.download_button(
                        label="Download All FRED Data (CSV)",
                        data=convert_df_to_csv(all_df),
                        file_name=f"fred_all_categories_{selected_time.replace(' ', '_')}.csv",
                        mime="text/csv",
                        key="all_fred_csv"
                    )
                    st.success(f"Ready! {len(all_fred_data)} series prepared.")
    else:
        st.subheader(f"FRED - {selected_category}")
        fred_download_data = {}
        dl_series_dict = MACRO_SERIES[selected_category]

        for name, series_id in dl_series_dict.items():
            data = get_series_data(series_id, start_date)
            if not data.empty:
                fred_download_data[name] = data

        if fred_download_data:
            # Combined CSV
            combined_df = pd.DataFrame(fred_download_data)
            combined_df.index.name = 'Date'
            st.download_button(
                label=f"Download All {selected_category} Data (CSV)",
                data=convert_df_to_csv(combined_df),
                file_name=f"fred_{selected_category.replace(' ', '_').replace('&', 'and').lower()}_{selected_time.replace(' ', '_')}.csv",
                mime="text/csv"
            )

            # Individual downloads
            st.write("**Individual Series:**")
            cols = st.columns(4)
            for idx, (name, data) in enumerate(fred_download_data.items()):
                with cols[idx % 4]:
                    series_id = dl_series_dict[name]
                    st.download_button(
                        label=f"{name}",
                        data=convert_series_to_csv(data, name),
                        file_name=f"fred_{series_id}.csv",
                        mime="text/csv",
                        key=f"fred_{series_id}"
                    )

else:  # Both
    st.subheader("Download All Data")

    # FRED data for selected category
    col1, col2 = st.columns(2)

    with col1:
        st.write("**FRED Data:**")
        fred_download_data = {}

        if selected_category == "All Categories":
            for category, series in MACRO_SERIES.items():
                for name, series_id in series.items():
                    data = get_series_data(series_id, start_date)
                    if not data.empty:
                        fred_download_data[f"{category} - {name}"] = data
            filename_cat = "all_categories"
        else:
            dl_series_dict = MACRO_SERIES[selected_category]
            for name, series_id in dl_series_dict.items():
                data = get_series_data(series_id, start_date)
                if not data.empty:
                    fred_download_data[name] = data
            filename_cat = selected_category.replace(' ', '_').replace('&', 'and').lower()

        if fred_download_data:
            combined_df = pd.DataFrame(fred_download_data)
            combined_df.index.name = 'Date'
            st.download_button(
                label=f"Download FRED Data (CSV)",
                data=convert_df_to_csv(combined_df),
                file_name=f"fred_{filename_cat}_{selected_time.replace(' ', '_')}.csv",
                mime="text/csv",
                key="fred_combined"
            )

    with col2:
        st.write("**FMP Data:**")
        fmp_download_data = {}
        fmp_key_indicators = {
            "GDP": "GDP",
            "Real GDP": "realGDP",
            "Unemployment Rate": "unemploymentRate",
            "CPI": "CPI",
            "Fed Funds Rate": "federalFunds",
            "Industrial Production": "industrialProductionTotalIndex",
        }
        for name, indicator in fmp_key_indicators.items():
            data = get_fmp_economic_indicator(indicator, start_date)
            if not data.empty:
                fmp_download_data[name] = data

        if fmp_download_data:
            combined_df = pd.DataFrame(fmp_download_data)
            combined_df.index.name = 'Date'
            st.download_button(
                label="Download FMP Economic Data (CSV)",
                data=convert_df_to_csv(combined_df),
                file_name=f"fmp_economic_indicators_{selected_time.replace(' ', '_')}.csv",
                mime="text/csv",
                key="fmp_combined"
            )

# Download ALL FRED data option
st.divider()
st.subheader("Download Complete Dataset")

if st.button("Prepare All FRED Data for Download"):
    with st.spinner("Fetching all FRED data..."):
        all_fred_data = {}
        for category, series in MACRO_SERIES.items():
            for name, series_id in series.items():
                data = get_series_data(series_id, start_date)
                if not data.empty:
                    all_fred_data[f"{category} - {name}"] = data

        if all_fred_data:
            all_df = pd.DataFrame(all_fred_data)
            all_df.index.name = 'Date'
            st.download_button(
                label="Download Complete FRED Dataset (CSV)",
                data=convert_df_to_csv(all_df),
                file_name=f"fred_complete_macro_data_{selected_time.replace(' ', '_')}.csv",
                mime="text/csv",
                key="all_fred"
            )
            st.success(f"Ready! {len(all_fred_data)} series prepared for download.")

# Footer
st.divider()
st.caption("Data provided by FRED (Federal Reserve Bank of St. Louis), FMP (Financial Modeling Prep), and includes Atlanta Fed GDPNow. Updated regularly.")
