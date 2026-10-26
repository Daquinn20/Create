"""
Earnings Transcript Analysis Dashboard
Interactive Streamlit dashboard for analyzing earnings call transcripts
"""
import streamlit as st
import os
import json
import re
import requests
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path
import anthropic
import openai
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from docx import Document
from docx.shared import Pt, RGBColor, Inches, Twips
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.platypus.doctemplate import PageTemplate, BaseDocTemplate
from reportlab.platypus.frames import Frame
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Earnings Transcript Analyzer",
    page_icon="📈",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .big-font {
        font-size:30px !important;
        font-weight: bold;
    }
    .stTextArea textarea {
        font-family: monospace;
    }
    </style>
    """, unsafe_allow_html=True)

# API Keys from environment or Streamlit secrets
def get_api_key(key_name):
    """Get API key from environment or Streamlit secrets"""
    # Try environment first
    key = os.getenv(key_name)
    if key:
        return key
    # Try Streamlit secrets
    try:
        return st.secrets.get(key_name)
    except Exception:
        return None

FMP_API_KEY = get_api_key("FMP_API_KEY")
ANTHROPIC_API_KEY = get_api_key("ANTHROPIC_API_KEY")
OPENAI_API_KEY = get_api_key("OPENAI_API_KEY")
EMAIL_ADDRESS = get_api_key("EMAIL_ADDRESS")
EMAIL_PASSWORD = get_api_key("EMAIL_PASSWORD")


# Sector-specific KPIs the analyzer must force the LLM to look for in the transcript.
# Keyed by FMP sector; some sectors get refined further by industry substring.
# If a KPI is not disclosed, the model is instructed to say "not disclosed" rather than skip.
SECTOR_KPIS: Dict[str, str] = {
    "Technology": (
        "Billings / calculated billings (YoY growth, vs prior quarter trend), "
        "cRPO and total RPO (YoY growth), Net Retention Rate / DBNR, "
        "Net new ARR, new logo adds, gross retention, "
        "cloud / subscription revenue mix, FCF margin"
    ),
    "Communication Services": (
        "Subscriber / user net adds, ARPU, churn, engagement (DAU/MAU), "
        "ad revenue growth, content / programming spend, FCF"
    ),
    "Financial Services": (
        "Net interest margin (NIM), net interest income (NII), loan growth, "
        "deposit growth and deposit beta, net charge-offs, provision for credit losses, "
        "CET1 ratio, efficiency ratio, fee income trends"
    ),
    "Financials": (
        "Net interest margin (NIM), net interest income (NII), loan growth, "
        "deposit growth and deposit beta, net charge-offs, provision for credit losses, "
        "CET1 ratio, efficiency ratio, fee income trends"
    ),
    "Healthcare": (
        "Product-level revenue (top drugs / devices), volume vs price, "
        "pipeline / trial readouts, R&D spend, patent cliff exposure, "
        "gross-to-net pricing pressure, payer mix"
    ),
    "Consumer Cyclical": (
        "Same-store / comparable sales (comps), traffic vs ticket, "
        "gross margin and promotional intensity, inventory levels and turns, "
        "store count / unit growth, e-commerce mix"
    ),
    "Consumer Defensive": (
        "Organic revenue growth (volume vs price/mix), gross margin, "
        "input cost trends, market share, advertising spend, inventory weeks on hand"
    ),
    "Energy": (
        "Production volumes (BOE/d), realized prices, F&D and lifting costs, "
        "reserve replacement ratio, capex vs FCF, breakeven oil price, hedging position"
    ),
    "Industrials": (
        "Organic revenue growth, book-to-bill, backlog, order trends by segment, "
        "price/cost spread, operating margin by segment, free cash flow conversion"
    ),
    "Basic Materials": (
        "Volume vs price, capacity utilization, input cost pass-through, "
        "inventory destock/restock, end-market demand commentary, EBITDA per ton"
    ),
    "Utilities": (
        "Rate base growth, allowed ROE, capex plan, load growth, "
        "regulatory lag, fuel cost recovery, dividend coverage"
    ),
    "Real Estate": (
        "FFO / AFFO per share, same-store NOI growth, occupancy, "
        "leasing spreads (re-leasing rent change), debt maturities and cost of debt, "
        "cap rates on acquisitions/dispositions"
    ),
}

# Industry-level refinements (substring match on `industry` field, applied if matched).
# Use these when the sector default is too broad (e.g. Tech covers SaaS AND semis).
INDUSTRY_KPIS: Dict[str, str] = {
    "Semiconductor": (
        "Book-to-bill, inventory days and channel inventory, lead times, "
        "data center vs other end-market mix, gross margin trajectory, "
        "utilization, wafer / unit ASPs, capex intensity"
    ),
    "Software": (
        "Billings / calculated billings (YoY growth, vs prior quarter trend), "
        "cRPO and total RPO (YoY growth), Net Retention Rate / DBNR, "
        "Net new ARR, new logo adds, gross retention, "
        "subscription revenue mix, FCF margin, magic number / sales efficiency"
    ),
    "Insurance": (
        "Combined ratio (loss + expense), premium growth (NPW), "
        "reserve development (favorable/unfavorable), investment yield, "
        "catastrophe losses, book value per share growth"
    ),
    "Bank": (
        "Net interest margin (NIM), NII, loan growth, deposit growth and beta, "
        "net charge-offs, NPL/non-accruals, ACL build/release, "
        "CET1, efficiency ratio, fee income"
    ),
    "Biotechnology": (
        "Pipeline readouts and trial timelines, R&D spend, cash runway, "
        "lead asset commercial trajectory, partnership / licensing milestones"
    ),
    "Telecom": (
        "Postpaid phone net adds, churn, ARPU, broadband net adds, "
        "capex intensity, FCF, fiber / 5G build progress"
    ),
    "Auto": (
        "Unit volumes by segment, ASP, mix shift (EV / trim), incentives, "
        "production / inventory, backlog, gross margin per unit, warranty trends"
    ),
    "Airline": (
        "RASM, CASM ex-fuel, load factor, ASMs (capacity), "
        "fuel cost, unit revenue by entity, FCF and net debt"
    ),
    "Restaurant": (
        "Comparable sales (traffic vs check), unit growth, "
        "restaurant-level margin, commodity / labor inflation, digital mix"
    ),
}


def get_sector_kpis(company_info: Optional[Dict]) -> Optional[str]:
    """Resolve the most specific KPI list for the company.

    Industry substring match wins over sector default — e.g. a Tech company in
    "Semiconductors" gets the semi KPI list, not the generic Tech/SaaS one.
    """
    if not company_info:
        return None
    industry = (company_info.get('industry') or '').strip()
    sector = (company_info.get('sector') or '').strip()
    for key, kpis in INDUSTRY_KPIS.items():
        if key.lower() in industry.lower():
            return kpis
    return SECTOR_KPIS.get(sector)


def build_market_context(symbol: str,
                         price_data: Optional[pd.DataFrame],
                         surprises: Optional[pd.DataFrame]) -> Optional[str]:
    """Build a 'market context' text block summarizing beat/miss + post-earnings move.

    This is the data the model needs to reconcile narrative with the tape:
    a bullish-sounding transcript that the market rejected almost always means
    a soft guide or KPI deceleration the model didn't flag without context.
    """
    if (price_data is None or price_data.empty) and (surprises is None or surprises.empty):
        return None

    lines = [f"Ticker: {symbol}"]

    # Beat/miss for the most recent quarter (and one prior, for trajectory)
    if surprises is not None and not surprises.empty:
        recent = surprises.head(2)
        lines.append("")
        lines.append("EPS BEAT/MISS (most recent quarters):")
        for _, row in recent.iterrows():
            actual = row.get('Actual EPS', 0)
            est = row.get('Estimated EPS', 0)
            pct = row.get('Surprise %', 0)
            label = row.get('Beat/Miss', '')
            lines.append(
                f"  - {row.get('Date', '')}: {label} — actual ${actual:.2f} vs estimate ${est:.2f} ({pct:+.1f}%)"
            )

    # Stock reaction around the most recent earnings date
    if (surprises is not None and not surprises.empty
            and price_data is not None and not price_data.empty):
        try:
            most_recent_earnings = pd.to_datetime(surprises.iloc[0].get('Date'))
            prices = price_data.copy()
            prices['Date'] = pd.to_datetime(prices['Date'])
            prices = prices.sort_values('Date').reset_index(drop=True)

            # Price the day before earnings (or closest prior trading day)
            pre = prices[prices['Date'] < most_recent_earnings].tail(1)
            # Price 1 trading day after earnings
            post_1d = prices[prices['Date'] > most_recent_earnings].head(1)
            # Price 5 trading days after earnings
            post_5d = prices[prices['Date'] > most_recent_earnings].head(5).tail(1)
            # Latest available close
            latest = prices.tail(1)

            if not pre.empty and not post_1d.empty:
                pre_px = pre.iloc[0]['Close']
                d1_px = post_1d.iloc[0]['Close']
                d1_move = ((d1_px - pre_px) / pre_px) * 100
                lines.append("")
                lines.append("STOCK REACTION TO MOST RECENT EARNINGS:")
                lines.append(f"  - Pre-earnings close ({pre.iloc[0]['Date'].date()}): ${pre_px:.2f}")
                lines.append(f"  - 1-day reaction ({post_1d.iloc[0]['Date'].date()}): ${d1_px:.2f} ({d1_move:+.1f}%)")
                if not post_5d.empty and post_5d.iloc[0]['Date'] != post_1d.iloc[0]['Date']:
                    d5_px = post_5d.iloc[0]['Close']
                    d5_move = ((d5_px - pre_px) / pre_px) * 100
                    lines.append(f"  - 5-day reaction ({post_5d.iloc[0]['Date'].date()}): ${d5_px:.2f} ({d5_move:+.1f}%)")
                if not latest.empty:
                    cur_px = latest.iloc[0]['Close']
                    cur_move = ((cur_px - pre_px) / pre_px) * 100
                    lines.append(f"  - Current ({latest.iloc[0]['Date'].date()}): ${cur_px:.2f} ({cur_move:+.1f}% vs pre-earnings)")
        except Exception:
            pass

    if len(lines) == 1:  # only the ticker line
        return None
    return "\n".join(lines)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_transcripts(symbol: str, num_quarters: int = 4) -> List[Dict]:
    """Fetch earnings transcripts from FMP API"""
    url = f"https://financialmodelingprep.com/api/v4/batch_earning_call_transcript/{symbol.upper()}"
    transcripts = []
    current_year = datetime.now().year

    try:
        # Fetch from current year and previous years until we have enough transcripts
        for year in range(current_year, current_year - 3, -1):
            if len(transcripts) >= num_quarters:
                break

            params = {'year': year, 'apikey': FMP_API_KEY}
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and 'Error Message' in data:
                continue

            if not isinstance(data, list) or len(data) == 0:
                continue

            for item in data:
                if len(transcripts) >= num_quarters:
                    break
                content_text = item.get('content', '')
                if content_text:
                    transcripts.append({
                        'symbol': symbol,
                        'year': item.get('year'),
                        'quarter': item.get('quarter'),
                        'date': item.get('date', 'Unknown'),
                        'content': content_text,
                        'word_count': len(content_text.split())
                    })

        return transcripts
    except Exception as e:
        st.error(f"Error fetching transcripts: {e}")
        return []


@st.cache_data(ttl=3600, show_spinner=False)
def get_company_profile(symbol: str) -> Optional[Dict]:
    """Get company profile from FMP"""
    url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}"
    params = {'apikey': FMP_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data[0] if isinstance(data, list) and len(data) > 0 else None
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_quarterly_financials(symbol: str, num_quarters: int = 8) -> Optional[pd.DataFrame]:
    """Fetch quarterly income statement data for charts"""
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}"
    params = {'period': 'quarter', 'limit': num_quarters, 'apikey': FMP_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data or isinstance(data, dict):  # API error returns dict
            return None

        # Build DataFrame
        records = []
        for item in reversed(data):  # Reverse to get oldest first
            date = item.get('date', '')
            fiscal_year = item.get('calendarYear', '')
            period = item.get('period', '')
            quarter_label = f"{period} {fiscal_year}"

            records.append({
                'Quarter': quarter_label,
                'Date': date,
                'Revenue': item.get('revenue', 0) / 1_000_000,  # Convert to millions
                'Gross Profit': item.get('grossProfit', 0) / 1_000_000,
                'Operating Income': item.get('operatingIncome', 0) / 1_000_000,
                'Net Income': item.get('netIncome', 0) / 1_000_000,
                'EPS': item.get('eps', 0)
            })

        return pd.DataFrame(records)
    except Exception as e:
        st.warning(f"Could not fetch financial data: {e}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_quarterly_cashflow(symbol: str, num_quarters: int = 8) -> Optional[pd.DataFrame]:
    """Fetch quarterly cash flow statement data for charts"""
    url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{symbol}"
    params = {'period': 'quarter', 'limit': num_quarters, 'apikey': FMP_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data or isinstance(data, dict):
            return None

        records = []
        for item in reversed(data):  # Reverse to get oldest first
            fiscal_year = item.get('calendarYear', '')
            period = item.get('period', '')
            quarter_label = f"{period} {fiscal_year}"

            op_cashflow = item.get('operatingCashFlow', 0) / 1_000_000  # Convert to millions
            capex = abs(item.get('capitalExpenditure', 0)) / 1_000_000  # CapEx is usually negative
            fcf = op_cashflow - capex

            records.append({
                'Quarter': quarter_label,
                'Operating Cash Flow': op_cashflow,
                'Capital Expenditures': capex,
                'Free Cash Flow': fcf
            })

        return pd.DataFrame(records)
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_stock_price_history(symbol: str, years: int = 2) -> Optional[pd.DataFrame]:
    """Fetch historical daily stock prices"""
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
    params = {'apikey': FMP_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data or 'historical' not in data:
            return None

        # Filter to last N years
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=years * 365)

        records = []
        for item in data['historical']:
            date = datetime.strptime(item['date'], '%Y-%m-%d')
            if date >= cutoff_date:
                records.append({
                    'Date': date,
                    'Close': item.get('close', 0),
                    'Volume': item.get('volume', 0)
                })

        df = pd.DataFrame(records)
        return df.sort_values('Date') if not df.empty else None
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_earnings_surprises(symbol: str, num_quarters: int = 8) -> Optional[pd.DataFrame]:
    """Fetch earnings surprises (beats/misses) for footnotes"""
    url = f"https://financialmodelingprep.com/api/v3/earnings-surprises/{symbol}"
    params = {'apikey': FMP_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data:
            return None

        records = []
        for item in data[:num_quarters]:
            date = item.get('date', '')
            actual = item.get('actualEarningResult', 0)
            estimated = item.get('estimatedEarning', 0)

            if estimated and estimated != 0:
                surprise_pct = ((actual - estimated) / abs(estimated)) * 100
            else:
                surprise_pct = 0

            records.append({
                'Date': date,
                'Actual EPS': actual,
                'Estimated EPS': estimated,
                'Surprise %': surprise_pct,
                'Beat/Miss': 'Beat' if surprise_pct > 0 else ('Miss' if surprise_pct < 0 else 'Met')
            })

        return pd.DataFrame(records)
    except Exception:
        return None


def create_financial_charts(symbol: str):
    """Create and display financial charts for the symbol"""
    st.markdown("---")
    st.subheader("📊 Financial Performance (Last 8 Quarters)")

    try:
        financials = fetch_quarterly_financials(symbol)
        if financials is None or financials.empty:
            st.warning(f"No quarterly financial data available for {symbol}")
            return
    except Exception as e:
        st.error(f"Error fetching financial data: {e}")
        return

    # Calculate margins upfront
    financials['Gross Margin %'] = (financials['Gross Profit'] / financials['Revenue'] * 100).round(1)
    financials['Operating Margin %'] = (financials['Operating Income'] / financials['Revenue'] * 100).round(1)

    # Calculate sequential growth rates
    growth_data = []
    for i in range(1, len(financials)):
        prev = financials.iloc[i-1]
        curr = financials.iloc[i]

        rev_growth = ((curr['Revenue'] - prev['Revenue']) / prev['Revenue'] * 100) if prev['Revenue'] != 0 else 0
        gp_growth = ((curr['Gross Profit'] - prev['Gross Profit']) / prev['Gross Profit'] * 100) if prev['Gross Profit'] != 0 else 0

        if prev['Operating Income'] != 0:
            op_growth = ((curr['Operating Income'] - prev['Operating Income']) / abs(prev['Operating Income']) * 100)
        else:
            op_growth = 0

        growth_data.append({
            'Quarter': curr['Quarter'],
            'Revenue': f"{rev_growth:+.1f}%",
            'Gross Profit': f"{gp_growth:+.1f}%",
            'Op. Income': f"{op_growth:+.1f}%"
        })

    # Layout: Charts stacked on left, Growth table on right
    col_charts, col_table = st.columns([2, 1])

    with col_charts:
        # Revenue Chart
        fig_rev = go.Figure()
        fig_rev.add_trace(go.Bar(
            x=financials['Quarter'],
            y=financials['Revenue'],
            marker_color='#4472C4',
            name='Revenue',
            text=[f'${v:,.1f}' for v in financials['Revenue']],
            textposition='outside'
        ))
        fig_rev.update_layout(
            title='Revenue ($M)',
            xaxis_tickangle=-45,
            height=280,
            margin=dict(l=40, r=40, t=40, b=60)
        )
        st.plotly_chart(fig_rev, use_container_width=True)

        # Gross Profit & Margin Chart
        fig_gp = make_subplots(specs=[[{"secondary_y": True}]])
        fig_gp.add_trace(
            go.Bar(
                x=financials['Quarter'],
                y=financials['Gross Profit'],
                marker_color='#70AD47',
                name='Gross Profit ($M)',
                text=[f'${v:,.1f}' for v in financials['Gross Profit']],
                textposition='outside'
            ),
            secondary_y=False
        )
        fig_gp.add_trace(
            go.Scatter(
                x=financials['Quarter'],
                y=financials['Gross Margin %'],
                mode='lines+markers',
                name='Gross Margin %',
                line=dict(color='#000000', width=2),
                marker=dict(size=6)
            ),
            secondary_y=True
        )
        fig_gp.update_layout(
            title='Gross Profit & Margin',
            xaxis_tickangle=-45,
            height=280,
            margin=dict(l=40, r=40, t=40, b=60),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        fig_gp.update_yaxes(title_text="$M", secondary_y=False)
        fig_gp.update_yaxes(title_text="%", secondary_y=True)
        st.plotly_chart(fig_gp, use_container_width=True)

        # Operating Income & Margin Chart
        fig_op = make_subplots(specs=[[{"secondary_y": True}]])
        colors_op = ['#ED7D31' if val >= 0 else '#C00000' for val in financials['Operating Income']]
        fig_op.add_trace(
            go.Bar(
                x=financials['Quarter'],
                y=financials['Operating Income'],
                marker_color=colors_op,
                name='Operating Income ($M)',
                text=[f'${v:,.1f}' for v in financials['Operating Income']],
                textposition='outside'
            ),
            secondary_y=False
        )
        fig_op.add_trace(
            go.Scatter(
                x=financials['Quarter'],
                y=financials['Operating Margin %'],
                mode='lines+markers',
                name='Operating Margin %',
                line=dict(color='#000000', width=2),
                marker=dict(size=6)
            ),
            secondary_y=True
        )
        fig_op.update_layout(
            title='Operating Income & Margin',
            xaxis_tickangle=-45,
            height=280,
            margin=dict(l=40, r=40, t=40, b=60),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        fig_op.update_yaxes(title_text="$M", secondary_y=False)
        fig_op.update_yaxes(title_text="%", secondary_y=True)
        st.plotly_chart(fig_op, use_container_width=True)

    with col_table:
        st.markdown("### Sequential Growth (QoQ)")
        if growth_data:
            growth_df = pd.DataFrame(growth_data)
            st.dataframe(growth_df, use_container_width=True, hide_index=True, height=750)

    # Cash Flow Section
    cashflow_data = fetch_quarterly_cashflow(symbol)
    if cashflow_data is not None and not cashflow_data.empty:
        st.subheader("💰 Cash Flow Analysis")

        # Two charts side by side
        cf_col1, cf_col2 = st.columns(2)

        with cf_col1:
            # Operating Cash Flow chart
            fig_ocf = go.Figure()
            colors_ocf = ['#70AD47' if val >= 0 else '#C00000' for val in cashflow_data['Operating Cash Flow']]
            fig_ocf.add_trace(go.Bar(
                x=cashflow_data['Quarter'],
                y=cashflow_data['Operating Cash Flow'],
                marker_color=colors_ocf,
                name='Operating Cash Flow',
                text=[f'${v:,.1f}' for v in cashflow_data['Operating Cash Flow']],
                textposition='outside'
            ))
            fig_ocf.update_layout(
                title='Operating Cash Flow ($M)',
                xaxis_tickangle=-45,
                height=300,
                margin=dict(l=40, r=40, t=40, b=60)
            )
            st.plotly_chart(fig_ocf, use_container_width=True)

        with cf_col2:
            # Capital Expenditures chart
            fig_capex = go.Figure()
            fig_capex.add_trace(go.Bar(
                x=cashflow_data['Quarter'],
                y=cashflow_data['Capital Expenditures'],
                marker_color='#ED7D31',
                name='Capital Expenditures',
                text=[f'${v:,.1f}' for v in cashflow_data['Capital Expenditures']],
                textposition='outside'
            ))
            fig_capex.update_layout(
                title='Capital Expenditures ($M)',
                xaxis_tickangle=-45,
                height=300,
                margin=dict(l=40, r=40, t=40, b=60)
            )
            st.plotly_chart(fig_capex, use_container_width=True)

        # Cash Flow Table with Free Cash Flow
        st.markdown("### Cash Flow Summary")
        cf_table = cashflow_data[['Quarter', 'Operating Cash Flow', 'Capital Expenditures', 'Free Cash Flow']].copy()
        cf_table['Operating Cash Flow'] = cf_table['Operating Cash Flow'].apply(lambda x: f"${x:,.1f}M")
        cf_table['Capital Expenditures'] = cf_table['Capital Expenditures'].apply(lambda x: f"${x:,.1f}M")
        cf_table['Free Cash Flow'] = cf_table['Free Cash Flow'].apply(lambda x: f"${x:,.1f}M")
        st.dataframe(cf_table, use_container_width=True, hide_index=True)

    # Stock Price Chart (2 years) - Full width with earnings dates
    st.subheader("📉 Stock Price (2 Years)")
    price_data = fetch_stock_price_history(symbol, years=2)
    if price_data is not None and not price_data.empty:
        fig_price = go.Figure()
        fig_price.add_trace(go.Scatter(
            x=price_data['Date'],
            y=price_data['Close'],
            mode='lines',
            name='Close Price',
            line=dict(color='#4472C4', width=2),
            fill='tozeroy',
            fillcolor='rgba(68, 114, 196, 0.1)'
        ))

        # Add yellow dots at earnings dates
        earnings_dates = fetch_earnings_surprises(symbol, num_quarters=8)
        if earnings_dates is not None and not earnings_dates.empty:
            # Find stock prices on or near earnings dates
            earnings_points_x = []
            earnings_points_y = []
            for _, row in earnings_dates.iterrows():
                try:
                    earnings_date = pd.to_datetime(row['Date'])
                    # Find closest price date
                    price_data['DateDiff'] = abs(price_data['Date'] - earnings_date)
                    closest_idx = price_data['DateDiff'].idxmin()
                    closest_row = price_data.loc[closest_idx]
                    if closest_row['DateDiff'].days <= 5:  # Within 5 days
                        earnings_points_x.append(closest_row['Date'])
                        earnings_points_y.append(closest_row['Close'])
                except Exception:
                    pass

            if earnings_points_x:
                fig_price.add_trace(go.Scatter(
                    x=earnings_points_x,
                    y=earnings_points_y,
                    mode='markers',
                    name='Earnings Date',
                    marker=dict(color='yellow', size=12, symbol='circle',
                               line=dict(color='black', width=1))
                ))

        fig_price.update_layout(
            title=f'{symbol} Daily Close Price',
            xaxis_title='Date',
            yaxis_title='Price ($)',
            height=350,
            hovermode='x unified',
            margin=dict(l=40, r=40, t=40, b=40)
        )
        st.plotly_chart(fig_price, use_container_width=True)
    else:
        st.info("Stock price data not available")

    # Earnings Surprises Footnotes
    surprises = fetch_earnings_surprises(symbol)
    if surprises is not None and not surprises.empty:
        st.subheader("📋 Earnings Performance Notes")

        beats = surprises[surprises['Surprise %'] > 0]
        misses = surprises[surprises['Surprise %'] < 0]

        col_beat, col_miss = st.columns(2)

        with col_beat:
            if not beats.empty:
                st.markdown("**✅ Beats:**")
                for _, row in beats.iterrows():
                    st.markdown(f"- {row['Date']}: Beat by {row['Surprise %']:.1f}% (${row['Actual EPS']:.2f} vs ${row['Estimated EPS']:.2f} est)")

        with col_miss:
            if not misses.empty:
                st.markdown("**❌ Misses:**")
                for _, row in misses.iterrows():
                    st.markdown(f"- {row['Date']}: Missed by {abs(row['Surprise %']):.1f}% (${row['Actual EPS']:.2f} vs ${row['Estimated EPS']:.2f} est)")


def load_prior_analysis(symbol: str) -> Optional[str]:
    """Load the most recent prior analysis from OneDrive reports folder or output directory"""
    symbol_upper = symbol.upper()

    # Primary: OneDrive earnings reports folder
    onedrive_dir = Path.home() / 'OneDrive' / 'Documents' / 'Targeted Equity Consulting Group' / 'TECG Earnings Report Analysis'
    if onedrive_dir.exists():
        matches = sorted(
            [f for f in onedrive_dir.glob('*') if f.name.upper().startswith(symbol_upper + '_') and f.suffix in ['.docx', '.txt']],
            key=lambda f: f.stat().st_mtime,
            reverse=True
        )
        if matches:
            latest = matches[0]
            try:
                if latest.suffix == '.docx':
                    doc = Document(latest)
                    content = '\n'.join([para.text for para in doc.paragraphs if para.text.strip()])
                else:
                    with open(latest, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                if content:
                    return content, latest.name
            except Exception:
                pass

    # Fallback: local output directory
    output_dir = Path(__file__).parent / 'output'
    prior_file = output_dir / f'{symbol_upper}_claude_summary.txt'
    if prior_file.exists():
        try:
            with open(prior_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            if content:
                return content, prior_file.name
        except Exception:
            pass

    return None, None


def read_uploaded_notes(uploaded_file) -> Optional[str]:
    """Read notes from an uploaded file (txt, md, or docx)"""
    if uploaded_file is None:
        return None

    try:
        file_name = uploaded_file.name.lower()

        if file_name.endswith('.docx'):
            # Read Word document
            doc = Document(io.BytesIO(uploaded_file.read()))
            content = '\n'.join([para.text for para in doc.paragraphs if para.text.strip()])
            return content if content.strip() else None
        else:
            # Read text or markdown file
            content = uploaded_file.read().decode('utf-8')
            return content if content.strip() else None
    except Exception as e:
        st.warning(f"Could not read uploaded file: {e}")
        return None


def create_analysis_prompt(symbol, transcripts, company_info=None,
                           prior_analysis=None, user_notes=None,
                           sector_kpis=None, market_context=None) -> str:
    header = f"COMPANY: {symbol}\n"
    if company_info:
        header += f"Name: {company_info.get('companyName', 'N/A')}\n"
        header += f"Industry: {company_info.get('industry', 'N/A')}\n"
        header += f"Sector: {company_info.get('sector', 'N/A')}\n"

    context = ""
    if market_context:
        context += f"\nMARKET CONTEXT (beat/miss and post-earnings reaction):\n{market_context}\n"
    if sector_kpis:
        context += f"\nSECTOR KPIs FOR THE QUARTER TABLE: {sector_kpis}\n"
    if prior_analysis:
        context += ("\nA prior analysis exists. Do not summarize it. Use it only "
                    "to note, in one bullet in section 2, if the outlook changed.\n")
    if user_notes:
        context += f"\nUSER NOTES — answer any question in them inside the relevant section:\n{user_notes}\n"

    combined_text = ""
    for i, t in enumerate(transcripts, 1):
        combined_text += (f"\n\n--- TRANSCRIPT {i}: Q{t['quarter']} {t['year']} "
                          f"({t['date']}) ---\n{t['content']}")

    return f"""{header}{context}
Analyze the {len(transcripts)} earnings transcript(s) for {symbol} below.
The reader cares most about what management is saying about the FUTURE,
second about what the Q&A revealed, and only briefly about the quarter
itself. The most recent quarter is the focus; older transcripts are
context for whether the outlook is changing.

STEP 1 — FIGURE OUT WHAT MATTERS FOR THIS COMPANY (do this first;
it decides what the whole report covers). Identify the 4-6 drivers
that actually move this company, using only evidence in the transcripts:
  a) EMPHASIS: what management spent the most time on, repeated, or
     led with — in prepared remarks and in guidance.
  b) Q&A PRESSURE: what analysts asked about repeatedly or followed
     up on — that is what the market cares about for this name.
  c) GUIDANCE: what management chose to guide, raise, cut, or stop
     guiding — guided metrics are, by definition, what matters.
  d) THE SECTOR KPIs ABOVE, but only where the transcript actually
     engages them.
The drivers are different for every company: units / shipments and
product-cycle commentary for a hardware name, AI commentary where
management is making AI a strategy, subscribers and churn for a media
name, backlog / RPO / billings for software, production and realized
prices for energy, comps and traffic for retail, NIM and credit for a
bank. These are examples of the METHOD, not a checklist — pick what
this transcript supports, name the drivers in the company's own terms,
and drop everything else. If a normally key metric was NOT discussed,
that silence is itself a finding: report it as "not discussed".

OUTPUT RULES — FOLLOW EXACTLY:
- Output markdown only, using ONLY the six headings below, in order.
  No other headings, no introduction, no conclusion, no separators.
- Total length: 1,100 words maximum for the whole report.
- Sections 2, 3 and 4 together must be at least two-thirds of the report.
- Bullets in sections 2 and 4: one line, 25 words maximum.
  Bullets in section 3: up to 45 words — facts plus management's words.
- Do not repeat any fact, quote, or judgment in more than one section.
- Forward statements: give the timeframe management gave (next quarter,
  full year, long term). If they gave none, say "no timeframe given".
- Use specific numbers from the transcripts. Not disclosed = say so.
  Never estimate, never report a metric the transcript does not support.

## Verdict
First line: **POSITIVE**, **NEGATIVE**, or **NEUTRAL**, plus one sentence
on the single most important reason — forward-looking if possible.
Then "Bottom line:" and one sentence only.

## What They're Saying About the Future
First, a markdown guidance table — columns: Metric | Prior Guide |
New Guide / Outlook | Change. One row per metric guided for a future
period. If a metric was discussed for the future without numbers,
include it with the wording used.
Then maximum 5 bullets on the forward commentary that has no numbers:
demand outlook, product cycle, capacity, cost headwinds or tailwinds,
pricing, capital spending, buybacks — only what management actually
said about future periods, in their timeframe.

## What Matters — What Management Said
One bullet per driver identified in Step 1 (4-6 bullets), in order of
importance. Format: **Driver name:** then, in up to 45 words, the
facts — what management actually said, with the numbers or short
quotes that prove it, how it changed versus prior quarters, and what
it means going forward. This section is commentary and facts, not
opinion: if management gave a metric (units, shipments, subscribers,
orders, production — whatever fits this company), report it here;
if they talked around it, say what they said instead and note the gap.

## Q&A Tidbits
Maximum 6 bullets, most recent quarter only. The unscripted reveals:
- Forward information that came out ONLY because an analyst asked
- Evasive or vague answers about the future — these are signals
- Topics multiple analysts pressed on, and what that pressure produced
- Anything that softened, contradicted, or added to the prepared remarks
Each bullet: the topic, what was actually revealed, and one short
quote if the wording itself is the signal. Do not repeat drivers
already covered above — add only what the Q&A changed or revealed.

## The Quarter
A markdown table only, plus at most 2 bullets if something material
is not in the table. Columns: KPI | This Quarter | Prior Quarter |
YoY | Direction. One row per driver from Step 1 that has a reported
number, plus revenue, gross margin, operating margin, EPS, free
cash flow, and capex.

## Risks and Watch
**Bull case:** 1-2 sentences. **Bear case:** 1-2 sentences.
**Watch:** 3 numbered items — the specific future metrics, guideposts,
or dates that will prove or break the outlook above.

GUIDANCE CHART DATA — after the report, output ONLY this fenced block
(it is stripped before printing; the app charts it in the appendix):
```guidance-chart-data
{{"next_quarter": {{"label": "next quarter label, e.g. Q1 FY27",
  "revenue_low": null, "revenue_mid": null, "revenue_high": null,
  "revenue_yoy_low_pct": null, "revenue_yoy_mid_pct": null, "revenue_yoy_high_pct": null,
  "gross_margin_low_pct": null, "gross_margin_mid_pct": null, "gross_margin_high_pct": null,
  "capex": null}},
 "annual": {{"label": "fiscal year label, e.g. FY27",
  "revenue_low": null, "revenue_mid": null, "revenue_high": null,
  "gross_margin_floor_pct": null, "capex": null, "capex_is_minimum": false}}}}
```
Fill every field management actually guided (dollars in billions,
percents as numbers); leave the rest null. Quarterly guides go in
next_quarter, full-year guides in annual — the charts show quarterly
guides as a hatched guided bar/point with its range, and annual
guides as a floor/level line and annotation. Never invent a guide
to fill the block.
{combined_text}
"""


def analyze_with_claude(prompt: str) -> str:
    """Analyze using Claude"""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8000,
        system="You are an expert equity research analyst. When analyzing earnings transcripts, you MUST always include a dedicated Q&A Session Deep Dive section. The Q&A is where management gives unscripted answers — it reveals more than prepared remarks. Never omit this section.",
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


def analyze_with_chatgpt(symbol: str, transcripts: List[Dict], company_info: Optional[Dict] = None,
                         prior_analysis: Optional[str] = None, user_notes: Optional[str] = None,
                         sector_kpis: Optional[str] = None, market_context: Optional[str] = None) -> tuple:
    """Analyze using ChatGPT with automatic fallback for rate limits"""
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    # Try with all transcripts first
    quarters_to_try = len(transcripts)

    while quarters_to_try >= 1:
        try:
            prompt = create_analysis_prompt(symbol, transcripts[:quarters_to_try], company_info,
                                            prior_analysis, user_notes,
                                            sector_kpis=sector_kpis, market_context=market_context)
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert equity research analyst. When analyzing earnings transcripts, you MUST always include a dedicated Q&A Session Deep Dive section. The Q&A is where management gives unscripted answers — it reveals more than prepared remarks. Never omit this section."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=8000
            )
            result = response.choices[0].message.content
            if quarters_to_try < len(transcripts):
                return result, f"(Analyzed {quarters_to_try} quarters due to rate limits)"
            return result, None
        except openai.RateLimitError as e:
            if quarters_to_try > 1:
                quarters_to_try -= 1
                st.warning(f"Rate limit hit. Retrying with {quarters_to_try} quarters...")
            else:
                raise e

    raise Exception("Could not complete analysis due to rate limits")


def add_page_border(doc):
    """Add a black rectangular border around the page"""
    sections = doc.sections
    for section in sections:
        sectPr = section._sectPr
        pgBorders = OxmlElement('w:pgBorders')
        pgBorders.set(qn('w:offsetFrom'), 'page')

        for border_name in ['top', 'left', 'bottom', 'right']:
            border = OxmlElement(f'w:{border_name}')
            border.set(qn('w:val'), 'single')
            border.set(qn('w:sz'), '12')  # 1.5pt = 12 eighths of a point
            border.set(qn('w:space'), '24')
            border.set(qn('w:color'), '000000')  # Black
            pgBorders.append(border)

        sectPr.append(pgBorders)


import html as _html


def parse_markdown(content: str):
    """Parse the limited markdown the analysis prompt is allowed to emit.

    Returns a list of blocks:
      ('h1'|'h2'|'h3', text)
      ('p', text)
      ('bullets', [text, ...])
      ('numbers', [text, ...])
      ('table', headers, rows)
      ('quote', text)
      ('hr', None)
    Wrapped paragraph lines are joined. Blank lines separate blocks.
    """
    blocks = []
    lines = content.replace('\r\n', '\n').split('\n')
    i = 0
    para = []

    def flush_para():
        if para:
            blocks.append(('p', ' '.join(para).strip()))
            para.clear()

    while i < len(lines):
        raw = lines[i]
        line = raw.strip()

        if not line:
            flush_para()
            i += 1
            continue

        # Table: consecutive |...| lines, second line is the --- separator
        if line.startswith('|') and line.endswith('|'):
            flush_para()
            table_lines = []
            while i < len(lines) and lines[i].strip().startswith('|'):
                table_lines.append(lines[i].strip())
                i += 1
            def cells(row):
                return [c.strip() for c in row.strip('|').split('|')]
            headers = cells(table_lines[0])
            rows = [cells(r) for r in table_lines[2:]] if len(table_lines) > 2 else []
            blocks.append(('table', headers, rows))
            continue

        # Horizontal rule / old-style separators: drop, spacing does the job
        if re.fullmatch(r'(-{3,}|\*{3,}|={3,}|_{3,})', line):
            flush_para()
            i += 1
            continue

        m = re.match(r'^(#{1,3})\s+(.*)$', line)
        if m:
            flush_para()
            blocks.append((f'h{len(m.group(1))}', m.group(2).strip()))
            i += 1
            continue

        if re.match(r'^[-*•]\s+', line):
            flush_para()
            items = []
            while i < len(lines) and re.match(r'^\s*[-*•]\s+', lines[i]):
                items.append(re.sub(r'^\s*[-*•]\s+', '', lines[i]).strip())
                i += 1
            blocks.append(('bullets', items))
            continue

        if re.match(r'^\d+[.)]\s+', line):
            flush_para()
            items = []
            while i < len(lines) and re.match(r'^\s*\d+[.)]\s+', lines[i]):
                items.append(re.sub(r'^\s*\d+[.)]\s+', '', lines[i]).strip())
                i += 1
            blocks.append(('numbers', items))
            continue

        if line.startswith('>'):
            flush_para()
            blocks.append(('quote', line.lstrip('>').strip()))
            i += 1
            continue

        para.append(line)
        i += 1

    flush_para()
    return blocks


def _split_inline(text):
    """Yield (text, bold, italic, code) segments for **bold**, *italic*, `code`."""
    parts = re.split(r'(\*\*.*?\*\*|\*[^*]+?\*|`[^`]+?`)', text)
    for part in parts:
        if not part:
            continue
        if part.startswith('**') and part.endswith('**'):
            yield part[2:-2], True, False, False
        elif part.startswith('*') and part.endswith('*') and len(part) > 2:
            yield part[1:-1], False, True, False
        elif part.startswith('`') and part.endswith('`'):
            yield part[1:-1], False, False, True
        else:
            yield part, False, False, False




def _pdf_escape(text):
    return _html.escape(text, quote=False)


def _pdf_inline(text):
    out = []
    for seg, bold, italic, code in _split_inline(text):
        seg = _pdf_escape(seg)
        if bold: out.append(f"<b>{seg}</b>")
        elif italic: out.append(f"<i>{seg}</i>")
        elif code: out.append(f"<font face=\"Courier\">{seg}</font>")
        else: out.append(seg)
    return "".join(out)


SIGNATURE_LINES = [
    "David Quinn",
    "Targeted Equity Consulting Group",
    "daquinn@targetedequityconsulting.com | 617-905-7415",
]


def extract_guidance(content: str):
    """Pull the ```guidance-chart-data JSON block out of the analysis.

    Returns (content_without_block, guidance_dict_or_None). The block is
    produced by the prompt (see create_analysis_prompt) and is charted,
    never printed.
    """
    m = re.search(r"```guidance-chart-data\s*(\{.*?\})\s*```", content, re.S)
    if not m:
        return content, None
    try:
        guidance = json.loads(m.group(1))
    except Exception:
        guidance = None
    return (content[:m.start()] + content[m.end():]).strip(), guidance


def create_report_charts(symbol: str, guidance: dict = None) -> list:
    """Appendix charts, built once and shared by Word and PDF.

    Chart set (David, 2026-10-26): revenue growth YoY AND sequential,
    margins (gross / operating / FCF), and free cash flow with capex.
    If management offered a guidance range, it is charted too: a
    quarterly guide becomes a hatched bar/point with range whiskers at
    the guided quarter; an annual guide becomes a floor line (margins)
    and an annotation (capex) — quarterly or annual, whichever was given.
    Data: the FMP income + cashflow statements the app already fetches.
    Returns [(caption, BytesIO png), ...] for the last 8 quarters.
    """
    guidance = guidance or {}
    nq = guidance.get("next_quarter") or {}
    annual = guidance.get("annual") or {}
    charts = []
    try:
        fin = fetch_quarterly_financials(symbol, num_quarters=12)
        cf = fetch_quarterly_cashflow(symbol, num_quarters=12)
        if fin is None or fin.empty or len(fin) < 5:
            return charts
        fin = fin.reset_index(drop=True)
        fin["Gross Margin %"] = fin["Gross Profit"] / fin["Revenue"] * 100
        fin["Operating Margin %"] = fin["Operating Income"] / fin["Revenue"] * 100
        fin["Revenue YoY %"] = fin["Revenue"].pct_change(4) * 100
        fin["Revenue QoQ %"] = fin["Revenue"].pct_change(1) * 100
        if cf is not None and not cf.empty:
            fin = fin.merge(cf[["Quarter", "Free Cash Flow"]], on="Quarter", how="left")
        else:
            fin["Free Cash Flow"] = None
        fin["FCF Margin %"] = fin["Free Cash Flow"] / fin["Revenue"] * 100
        d = fin.tail(8).reset_index(drop=True)
        labels = d["Quarter"].tolist()
        gx = len(d)
        guide_label = nq.get("label") or "Guide"
        all_labels = labels + [guide_label]
        has_guide = any(v is not None for v in nq.values())
        navy, blue, grey, green, orange = "#34495E", "#4472C4", "#A6A6A6", "#70AD47", "#ED7D31"

        def new_fig(title):
            fig, ax = plt.subplots(figsize=(7, 3.2))
            ax.set_title(title, fontsize=11, fontweight="bold", color=navy)
            ax.grid(axis="y", color="#E7E7E7", linewidth=0.7)
            ax.set_axisbelow(True)
            for s in ("top", "right"):
                ax.spines[s].set_visible(False)
            ax.tick_params(labelsize=8)
            return fig, ax

        def save(fig, caption):
            plt.tight_layout()
            buf = io.BytesIO()
            plt.savefig(buf, format="png", dpi=200, bbox_inches="tight")
            buf.seek(0)
            charts.append((caption, buf))
            plt.close(fig)

        fig, ax = new_fig("Revenue Growth - YoY vs Sequential" + (", incl. Guidance" if has_guide else ""))
        x = range(len(d)); w = 0.38
        ax.bar([i - w / 2 for i in x], d["Revenue YoY %"], width=w, color=blue, label="YoY %")
        ax.bar([i + w / 2 for i in x], d["Revenue QoQ %"], width=w, color=grey, label="Sequential (QoQ) %")
        if has_guide and (nq.get("revenue_mid") is not None or nq.get("revenue_yoy_mid_pct") is not None):
            last_rev = float(d["Revenue"].iloc[-1]); yoy_base = float(fin["Revenue"].iloc[-4])
            if nq.get("revenue_yoy_mid_pct") is not None:
                yoy_mid = float(nq["revenue_yoy_mid_pct"])
                yoy_lo = float(nq.get("revenue_yoy_low_pct") or yoy_mid); yoy_hi = float(nq.get("revenue_yoy_high_pct") or yoy_mid)
                rev_mid = yoy_base * (1 + yoy_mid / 100)
            else:
                rev_mid = float(nq["revenue_mid"]) * 1000  # guide is in $B, FMP data in $M
                rev_lo = float(nq.get("revenue_low") or nq["revenue_mid"]) * 1000
                rev_hi = float(nq.get("revenue_high") or nq["revenue_mid"]) * 1000
                yoy_mid = (rev_mid / yoy_base - 1) * 100; yoy_lo = (rev_lo / yoy_base - 1) * 100; yoy_hi = (rev_hi / yoy_base - 1) * 100
            qoq_mid = (rev_mid / last_rev - 1) * 100
            qoq_lo = qoq_mid if nq.get("revenue_yoy_mid_pct") is not None else (float(nq.get("revenue_low") or nq["revenue_mid"]) * 1000 / last_rev - 1) * 100
            qoq_hi = qoq_mid if nq.get("revenue_yoy_mid_pct") is not None else (float(nq.get("revenue_high") or nq["revenue_mid"]) * 1000 / last_rev - 1) * 100
            ax.bar([gx - w / 2], [yoy_mid], width=w, color=blue, hatch="///", edgecolor="white", label="Guided (hatched)")
            ax.bar([gx + w / 2], [qoq_mid], width=w, color=grey, hatch="///", edgecolor="white")
            ax.errorbar([gx - w / 2], [yoy_mid], yerr=[[yoy_mid - yoy_lo], [yoy_hi - yoy_mid]], color=navy, capsize=3, linewidth=1)
            ax.errorbar([gx + w / 2], [qoq_mid], yerr=[[qoq_mid - qoq_lo], [qoq_hi - qoq_mid]], color=navy, capsize=3, linewidth=1)
            ax.set_xticks(list(x) + [gx]); ax.set_xticklabels(all_labels, fontsize=8)
        else:
            ax.set_xticks(list(x)); ax.set_xticklabels(labels, fontsize=8)
        ax.axhline(0, color=navy, linewidth=0.8)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, p: f"{v:.0f}%"))
        ax.legend(fontsize=8, frameon=False)
        save(fig, "Revenue growth: YoY vs sequential (QoQ), with the guided quarter hatched and whiskers showing the guide range." if has_guide else "Revenue growth: YoY vs sequential (QoQ). Seasonal businesses swing QoQ - YoY is the trend.")

        fig, ax = new_fig("Margins - Gross, Operating, Free Cash Flow" + (", incl. Guidance" if has_guide else ""))
        xs = list(range(len(d)))
        ax.plot(xs, d["Gross Margin %"], marker="o", color=blue, linewidth=2, label="Gross margin %")
        ax.plot(xs, d["Operating Margin %"], marker="o", color=orange, linewidth=2, label="Operating margin %")
        ax.plot(xs, d["FCF Margin %"], marker="o", color=green, linewidth=2, label="FCF margin %")
        if has_guide and nq.get("gross_margin_mid_pct") is not None:
            gm_mid = float(nq["gross_margin_mid_pct"])
            gm_lo = float(nq.get("gross_margin_low_pct") or gm_mid); gm_hi = float(nq.get("gross_margin_high_pct") or gm_mid)
            ax.plot([xs[-1], gx], [float(d["Gross Margin %"].iloc[-1]), gm_mid], color=blue, linewidth=1.5, linestyle="--")
            ax.errorbar([gx], [gm_mid], yerr=[[gm_mid - gm_lo], [gm_hi - gm_mid]], marker="D", color=blue, capsize=3, label="Guided gross margin")
            ax.set_xticks(xs + [gx]); ax.set_xticklabels(all_labels, fontsize=8)
        else:
            ax.set_xticks(xs); ax.set_xticklabels(labels, fontsize=8)
        if annual.get("gross_margin_floor_pct") is not None:
            floor = float(annual["gross_margin_floor_pct"])
            ax.axhline(floor, color=navy, linewidth=1, linestyle=":", alpha=0.7)
            ax.annotate(f"{annual.get('label') or 'Annual'} GM floor (guide) {floor:.2f}%", xy=(0, floor),
                        xytext=(2, 3), textcoords="offset points", fontsize=7.5, color=navy)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, p: f"{v:.0f}%"))
        ax.legend(fontsize=8, frameon=False)
        save(fig, "Margins: gross, operating and FCF margin, % of revenue, with guided gross margin and any annual margin floor.")

        if d["Free Cash Flow"].notna().any():
            cfd = cf.tail(8).reset_index(drop=True) if cf is not None else None
            fig, ax = new_fig("Free Cash Flow & Capex ($M)" + (", incl. Guidance" if has_guide else ""))
            x = range(len(d)); w = 0.38
            ax.bar([i - w / 2 for i in x], d["Free Cash Flow"], width=w, color=green, label="Free cash flow")
            if cfd is not None and len(cfd) == len(d):
                ax.bar([i + w / 2 for i in x], cfd["Capital Expenditures"], width=w, color=orange, label="Capex")
            if has_guide and nq.get("capex") is not None:
                ax.bar([gx + w / 2], [float(nq["capex"]) * 1000], width=w, color=orange, hatch="///", edgecolor="white", label="Guided capex (hatched)")
                ax.set_xticks(list(x) + [gx]); ax.set_xticklabels(all_labels, fontsize=8)
            else:
                ax.set_xticks(list(x)); ax.set_xticklabels(labels, fontsize=8)
            if annual.get("capex") is not None:
                prefix = ">" if annual.get("capex_is_minimum") else "~"
                ax.annotate(f"{annual.get('label') or 'Annual'} capex guide {prefix}${float(annual['capex']):,.0f}B",
                            xy=(0.02, 0.94), xycoords="axes fraction", fontsize=8, color=navy)
            ax.set_ylabel("$M")
            ax.legend(fontsize=8, frameon=False)
            save(fig, "Free cash flow (operating cash flow less capex) and capex, per quarter, with guided capex where offered.")
    except Exception as e:
        print(f"Error creating report charts: {e}")
    return charts



def create_word_document(content: str, symbol: str, ai_model: str) -> io.BytesIO:
    """Create Word document from the analysis markdown and return as bytes."""
    from docx.enum.text import WD_ALIGN_PARAGRAPH as _ALIGN
    from docx.oxml.ns import qn as _qn
    from docx.oxml import OxmlElement as _Ox

    NAVY = RGBColor(0x2C, 0x3E, 0x50)
    doc = Document()
    add_page_border(doc)
    for section in doc.sections:
        section.top_margin = Inches(0.7); section.bottom_margin = Inches(0.7)
        section.left_margin = Inches(0.8); section.right_margin = Inches(0.8)
    normal = doc.styles["Normal"]
    normal.font.name = "Calibri"; normal.font.size = Pt(10.5)
    normal.paragraph_format.space_after = Pt(6)
    normal.paragraph_format.line_spacing = 1.07
    for name, size, before in (("Heading 1", 15, 14), ("Heading 2", 12.5, 12), ("Heading 3", 11, 10)):
        stl = doc.styles[name]
        stl.font.name = "Calibri"; stl.font.size = Pt(size); stl.font.color.rgb = NAVY
        stl.font.bold = True
        stl.paragraph_format.space_before = Pt(before); stl.paragraph_format.space_after = Pt(4)
        stl.paragraph_format.keep_with_next = True

    logo_path = Path(__file__).parent / "company_logo.png"
    if logo_path.exists():
        try:
            p = doc.add_paragraph(); p.alignment = _ALIGN.CENTER
            p.add_run().add_picture(str(logo_path), width=Inches(2.6))
        except Exception as e:
            print(f"Error adding logo: {e}")

    title = doc.add_paragraph(f"{symbol} Earnings Transcript Analysis")
    title.style = doc.styles["Heading 1"]; title.alignment = _ALIGN.CENTER
    sub = doc.add_paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y')}  |  Model: {ai_model}")
    sub.alignment = _ALIGN.CENTER

    def add_runs(paragraph, text):
        for seg, bold, italic, code in _split_inline(text):
            run = paragraph.add_run(seg); run.bold = bold; run.italic = italic
            if code: run.font.name = "Consolas"

    content, guidance = extract_guidance(content)
    for block in parse_markdown(content):
        kind = block[0]
        if kind in ("h1", "h2", "h3"):
            p = doc.add_paragraph(style=f"Heading {kind[1]}"); add_runs(p, block[1])
        elif kind == "p":
            p = doc.add_paragraph(); add_runs(p, block[1])
        elif kind in ("bullets", "numbers"):
            for item in block[1]:
                p = doc.add_paragraph(style="List Bullet" if kind == "bullets" else "List Number")
                add_runs(p, item)
        elif kind == "quote":
            p = doc.add_paragraph(); p.paragraph_format.left_indent = Inches(0.3)
            add_runs(p, block[1])
            for run in p.runs: run.italic = True
        elif kind == "table":
            _, headers, rows = block
            table = doc.add_table(rows=1 + len(rows), cols=len(headers)); table.style = "Table Grid"
            for j, htxt in enumerate(headers):
                cell = table.rows[0].cells[j]; cell.text = ""
                add_runs(cell.paragraphs[0], htxt)
                for run in cell.paragraphs[0].runs:
                    run.bold = True; run.font.size = Pt(9); run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
                shd = _Ox("w:shd"); shd.set(_qn("w:fill"), "34495E")
                cell._tc.get_or_add_tcPr().append(shd)
            for r, row in enumerate(rows):
                for j, val in enumerate(row):
                    cell = table.rows[r + 1].cells[j]; cell.text = ""
                    add_runs(cell.paragraphs[0], val)
                    for run in cell.paragraphs[0].runs: run.font.size = Pt(9)
            doc.add_paragraph()

    charts = create_report_charts(symbol, guidance)
    if charts:
        doc.add_page_break()
        h = doc.add_paragraph("Appendix - Charts (last 8 quarters)"); h.style = doc.styles["Heading 1"]
        for caption, chart_buf in charts:
            chart_buf.seek(0)
            p = doc.add_paragraph(); p.alignment = _ALIGN.CENTER
            p.add_run().add_picture(chart_buf, width=Inches(6.4))
            cap = doc.add_paragraph(caption); cap.alignment = _ALIGN.CENTER
            for run in cap.runs: run.font.size = Pt(9); run.italic = True

    doc.add_paragraph()
    for line in SIGNATURE_LINES:
        doc.add_paragraph(line)
    buffer = io.BytesIO(); doc.save(buffer); buffer.seek(0)
    return buffer



def create_pdf_document(content: str, symbol: str, ai_model: str) -> io.BytesIO:
    """Create PDF document from the analysis markdown and return as bytes."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                            rightMargin=0.75 * inch, leftMargin=0.75 * inch,
                            topMargin=0.7 * inch, bottomMargin=0.7 * inch)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("T", parent=styles["Heading1"], fontSize=16,
                                 textColor=colors.HexColor("#2c3e50"), alignment=TA_CENTER, spaceAfter=4)
    subtitle_style = ParagraphStyle("Sub", parent=styles["Normal"], fontSize=10,
                                    textColor=colors.HexColor("#555555"), alignment=TA_CENTER, spaceAfter=14)
    h1 = ParagraphStyle("H1x", parent=styles["Heading1"], fontSize=14, leading=17,
                        textColor=colors.HexColor("#2c3e50"), spaceBefore=14, spaceAfter=5)
    h2 = ParagraphStyle("H2x", parent=styles["Heading2"], fontSize=12, leading=15,
                        textColor=colors.HexColor("#2c3e50"), spaceBefore=12, spaceAfter=4)
    h3 = ParagraphStyle("H3x", parent=styles["Heading3"], fontSize=11, leading=14,
                        textColor=colors.HexColor("#2c3e50"), spaceBefore=10, spaceAfter=3)
    body = ParagraphStyle("Bx", parent=styles["Normal"], fontSize=10.5, leading=15, spaceAfter=6)
    bullet = ParagraphStyle("Bul", parent=body, leftIndent=16, spaceAfter=3)
    quote = ParagraphStyle("Qx", parent=body, leftIndent=20, rightIndent=20, fontName="Helvetica-Oblique")
    sig = ParagraphStyle("Sigx", parent=styles["Normal"], fontSize=10, leading=14, spaceAfter=2)
    heading_for = {"h1": h1, "h2": h2, "h3": h3}

    story = []
    logo_path = Path(__file__).parent / "company_logo.png"
    if logo_path.exists():
        try:
            logo = Image(str(logo_path), width=2.6 * inch, height=2.6 * inch, kind="proportional")
            logo.hAlign = "CENTER"; story.append(logo); story.append(Spacer(1, 0.1 * inch))
        except Exception:
            pass
    story.append(Paragraph(f"{_html.escape(symbol)} Earnings Transcript Analysis", title_style))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y')}  |  Model: {_html.escape(ai_model)}", subtitle_style))

    content, guidance = extract_guidance(content)
    for block in parse_markdown(content):
        kind = block[0]
        if kind in heading_for:
            story.append(Paragraph(_pdf_inline(block[1]), heading_for[kind]))
        elif kind == "p":
            story.append(Paragraph(_pdf_inline(block[1]), body))
        elif kind == "bullets":
            for item in block[1]:
                story.append(Paragraph(_pdf_inline(item), bullet, bulletText="\u2022"))
        elif kind == "numbers":
            for n, item in enumerate(block[1], 1):
                story.append(Paragraph(_pdf_inline(item), bullet, bulletText=f"{n}."))
        elif kind == "quote":
            story.append(Paragraph(_pdf_inline(block[1]), quote))
        elif kind == "table":
            _, headers, rows = block
            cell = ParagraphStyle("Cx", parent=styles["Normal"], fontSize=8.5, leading=11)
            cell_h = ParagraphStyle("CHx", parent=cell, fontName="Helvetica-Bold", textColor=colors.white)
            data = [[Paragraph(_pdf_inline(h), cell_h) for h in headers]]
            data += [[Paragraph(_pdf_inline(v), cell) for v in row] for row in rows]
            col_w = (letter[0] - 1.5 * inch) / len(headers)
            t = Table(data, colWidths=[col_w] * len(headers), repeatRows=1)
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#34495e")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f2f4f5")]),
            ]))
            story.append(t); story.append(Spacer(1, 0.12 * inch))

    charts = create_report_charts(symbol, guidance)
    if charts:
        from reportlab.platypus import PageBreak
        story.append(PageBreak())
        story.append(Paragraph("Appendix - Charts (last 8 quarters)", h1))
        cap = ParagraphStyle("Capx", parent=styles["Normal"], fontSize=9, alignment=TA_CENTER,
                             fontName="Helvetica-Oblique", spaceAfter=14)
        for caption, chart_buf in charts:
            chart_buf.seek(0)
            img = Image(chart_buf, width=6.6 * inch, height=3.0 * inch, kind="proportional")
            img.hAlign = "CENTER"; story.append(img)
            story.append(Paragraph(_html.escape(caption), cap))

    story.append(Spacer(1, 0.3 * inch))
    for line in SIGNATURE_LINES:
        story.append(Paragraph(_html.escape(line), sig))
    doc.build(story, onFirstPage=draw_page_border, onLaterPages=draw_page_border)
    buffer.seek(0)
    return buffer


def send_email_with_attachments(recipient_email: str, symbol: str, ai_model: str,
                                 pdf_buffer: io.BytesIO = None, word_buffer: io.BytesIO = None) -> tuple:
    """Send email with PDF and/or Word attachments."""
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        return False, "Email credentials not configured"

    try:
        # Clean credentials
        clean_email = EMAIL_ADDRESS.strip().replace('\xa0', '').encode('ascii', 'ignore').decode('ascii')
        clean_password = EMAIL_PASSWORD.strip().replace('\xa0', '').encode('ascii', 'ignore').decode('ascii')

        # Create message
        msg = MIMEMultipart()
        msg['From'] = clean_email
        msg['To'] = recipient_email
        msg['Subject'] = f"{symbol} Earnings Transcript Analysis - {datetime.now().strftime('%B %d, %Y')}"

        # Email body
        body_text = f"""Your {symbol} Earnings Transcript Analysis ({ai_model}) is attached.

Generated: {datetime.now().strftime('%B %d, %Y at %H:%M')}

Targeted Equity Consulting Group
Precision Analysis for Informed Investment Decisions
"""
        msg.attach(MIMEText(body_text, 'plain'))

        # Attach PDF if provided
        if pdf_buffer:
            pdf_buffer.seek(0)
            pdf_attachment = MIMEBase('application', 'octet-stream')
            pdf_attachment.set_payload(pdf_buffer.read())
            encoders.encode_base64(pdf_attachment)
            pdf_attachment.add_header('Content-Disposition', 'attachment',
                                     filename=f"{symbol}_transcript_analysis.pdf")
            msg.attach(pdf_attachment)

        # Attach Word if provided
        if word_buffer:
            word_buffer.seek(0)
            word_attachment = MIMEBase('application', 'octet-stream')
            word_attachment.set_payload(word_buffer.read())
            encoders.encode_base64(word_attachment)
            word_attachment.add_header('Content-Disposition', 'attachment',
                                      filename=f"{symbol}_transcript_analysis.docx")
            msg.attach(word_attachment)

        # Send via Gmail SMTP
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(clean_email, clean_password)
        server.sendmail(clean_email, recipient_email, msg.as_string())
        server.quit()

        return True, "Email sent successfully!"
    except Exception as e:
        return False, f"Email error: {str(e)}"


# Main App
st.title("📈 Earnings Transcript Analyzer")
st.markdown("Analyze earnings call transcripts with AI (Claude & ChatGPT)")

# Check API keys
missing_keys = []
if not FMP_API_KEY:
    missing_keys.append("FMP_API_KEY")
if not ANTHROPIC_API_KEY:
    missing_keys.append("ANTHROPIC_API_KEY")
if not OPENAI_API_KEY:
    missing_keys.append("OPENAI_API_KEY")

if missing_keys:
    st.error(f"Missing API keys: {', '.join(missing_keys)}")
    st.info("Add them to your .env file or Streamlit secrets.")
    st.stop()

# Sidebar
st.sidebar.header("Settings")
symbol = st.sidebar.text_input("Stock Ticker", value="AAPL", max_chars=10).upper()
num_quarters = st.sidebar.slider("Number of Quarters", 1, 8, 4)
ai_choice = st.sidebar.selectbox("AI Model", ["Both", "Claude Only", "ChatGPT Only"])

# Sector KPI override — auto-detect from FMP by default, but let the user force a
# specific KPI checklist if FMP misclassifies or returns no sector.
# Industry-level entries (more specific) are listed first.
SECTOR_OVERRIDE_CHOICES = ["Auto-detect (FMP)"] + \
    [f"Industry: {k}" for k in INDUSTRY_KPIS.keys()] + \
    [f"Sector: {k}" for k in SECTOR_KPIS.keys()]
sector_override = st.sidebar.selectbox(
    "Sector KPI checklist",
    SECTOR_OVERRIDE_CHOICES,
    index=0,
    help="Forces which KPI checklist the model must address. Use Auto-detect to rely on FMP's classification; override when FMP returns no sector or a misleading one (e.g. a SaaS company classified as Industrials)."
)

# Notes section
st.sidebar.markdown("---")
st.sidebar.header("📝 Research Notes")
st.sidebar.markdown("Add your own notes to be analyzed with the transcripts")

# File uploader for notes
uploaded_notes = st.sidebar.file_uploader(
    "Upload Notes File",
    type=['txt', 'md', 'docx'],
    help="Upload a .txt, .md, or .docx file with your research notes"
)

# Text area for manual notes input
manual_notes = st.sidebar.text_area(
    "Or paste notes here",
    height=150,
    placeholder="Enter your research notes, observations, or questions about the company..."
)

# Main content
if st.sidebar.button("🔍 Analyze Earnings", type="primary"):

    # Validate ticker symbol
    if not re.match(r'^[A-Z]{1,5}$', symbol):
        st.error(f"Invalid ticker symbol: '{symbol}' (expected 1-5 letters, e.g. AAPL)")
        st.stop()

    # Fetch company info
    with st.spinner(f"Fetching {symbol} company info..."):
        company_info = get_company_profile(symbol)

    if company_info:
        col1, col2, col3 = st.columns(3)
        col1.metric("Company", company_info.get('companyName', symbol))
        col2.metric("Sector", company_info.get('sector', 'N/A'))
        col3.metric("Industry", company_info.get('industry', 'N/A'))

    # Fetch transcripts
    with st.spinner(f"Fetching {num_quarters} quarters of transcripts..."):
        transcripts = fetch_transcripts(symbol, num_quarters)

    if not transcripts:
        st.error(f"No transcripts found for {symbol}")
        st.stop()

    # Show transcript info
    st.success(f"Found {len(transcripts)} transcripts")
    total_words = sum(t['word_count'] for t in transcripts)

    with st.expander("📄 Transcript Details"):
        for t in transcripts:
            st.write(f"**Q{t['quarter']} {t['year']}** - {t['date']} ({t['word_count']:,} words)")

    st.info(f"Total words to analyze: {total_words:,}")

    # Load prior analysis if available
    prior_analysis, prior_filename = load_prior_analysis(symbol)
    if prior_analysis:
        st.info(f"📋 Loaded prior analysis from: {prior_filename}")

    # Process user notes (combine uploaded file + manual input)
    user_notes = None
    notes_parts = []

    # Read from uploaded file
    if uploaded_notes:
        uploaded_content = read_uploaded_notes(uploaded_notes)
        if uploaded_content:
            notes_parts.append(f"[From uploaded file: {uploaded_notes.name}]\n{uploaded_content}")
            st.info(f"📝 Loaded notes from: {uploaded_notes.name}")

    # Add manual notes
    if manual_notes and manual_notes.strip():
        notes_parts.append(f"[Manual notes]\n{manual_notes.strip()}")
        st.info("📝 Including manual notes in analysis")

    # Combine all notes
    if notes_parts:
        user_notes = "\n\n".join(notes_parts)

    # Resolve sector-specific KPIs the model must explicitly address.
    # Manual override (industry / sector pick from the sidebar) takes precedence
    # over FMP's auto-classification.
    sector_kpis = None
    if sector_override.startswith("Industry: "):
        key = sector_override[len("Industry: "):]
        sector_kpis = INDUSTRY_KPIS.get(key)
        if sector_kpis:
            st.info(f"📌 Sector KPI checklist (manual override → industry: {key})")
    elif sector_override.startswith("Sector: "):
        key = sector_override[len("Sector: "):]
        sector_kpis = SECTOR_KPIS.get(key)
        if sector_kpis:
            st.info(f"📌 Sector KPI checklist (manual override → sector: {key})")
    else:
        sector_kpis = get_sector_kpis(company_info)
        if sector_kpis:
            st.info(f"📌 Sector KPI checklist (auto-detected from FMP): {sector_kpis[:120]}…")
        else:
            st.warning("⚠️ No sector KPI checklist matched. Pick one manually in the sidebar to force it.")

    # Build market context (beat/miss + post-earnings price reaction) so the model
    # can reconcile a bullish-sounding transcript with the actual tape.
    market_context = None
    try:
        mc_prices = fetch_stock_price_history(symbol, years=2)
        mc_surprises = fetch_earnings_surprises(symbol, num_quarters=8)
        market_context = build_market_context(symbol, mc_prices, mc_surprises)
        if market_context:
            st.info("📊 Market context (beat/miss + price reaction) injected into prompt")
    except Exception as e:
        st.warning(f"Could not build market context: {e}")

    # Create prompt
    prompt = create_analysis_prompt(symbol, transcripts, company_info, prior_analysis, user_notes,
                                    sector_kpis=sector_kpis, market_context=market_context)

    # Analyze
    claude_result = None
    chatgpt_result = None

    if ai_choice in ["Both", "Claude Only"]:
        with st.spinner("🤖 Claude is analyzing..."):
            try:
                claude_result = analyze_with_claude(prompt)
            except Exception as e:
                st.error(f"Claude error: {e}")

    chatgpt_note = None
    if ai_choice in ["Both", "ChatGPT Only"]:
        with st.spinner("🤖 ChatGPT is analyzing..."):
            try:
                chatgpt_result, chatgpt_note = analyze_with_chatgpt(symbol, transcripts, company_info,
                                                                    prior_analysis, user_notes,
                                                                    sector_kpis=sector_kpis,
                                                                    market_context=market_context)
                if chatgpt_note:
                    st.info(f"ChatGPT {chatgpt_note}")
            except Exception as e:
                if "rate" in str(e).lower():
                    st.warning(f"ChatGPT rate limit exceeded. Using Claude only.")
                    if not claude_result and ai_choice == "ChatGPT Only":
                        st.info("Falling back to Claude...")
                        try:
                            claude_result = analyze_with_claude(prompt)
                        except Exception as ce:
                            st.error(f"Claude fallback error: {ce}")
                else:
                    st.error(f"ChatGPT error: {e}")

    # Store results in session state for persistence across button clicks
    st.session_state['claude_result'] = claude_result
    st.session_state['chatgpt_result'] = chatgpt_result
    st.session_state['analysis_symbol'] = symbol

    st.success("Analysis complete! See results below.")

    # Automatically send email with results
    if claude_result:
        with st.spinner("Sending email..."):
            word_doc = create_word_document(claude_result, symbol, "Claude")
            pdf_doc = create_pdf_document(claude_result, symbol, "Claude")
            success, message = send_email_with_attachments(
                "daquinn@targetedequityconsulting.com",
                symbol, "Claude", pdf_doc, word_doc
            )
            if success:
                st.success(f"📧 {message}")
            else:
                st.error(f"Email failed: {message}")

# Display results from session state (persists across button clicks)
claude_result = st.session_state.get('claude_result')
chatgpt_result = st.session_state.get('chatgpt_result')
analysis_symbol = st.session_state.get('analysis_symbol', symbol)

if claude_result or chatgpt_result:
    st.header("📊 Analysis Results")

    if claude_result and chatgpt_result:
        tab1, tab2 = st.tabs(["Claude Analysis", "ChatGPT Analysis"])

        with tab1:
            st.markdown(claude_result)
            word_doc = create_word_document(claude_result, analysis_symbol, "Claude")
            pdf_doc = create_pdf_document(claude_result, analysis_symbol, "Claude")

            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "📥 Download Word",
                    word_doc,
                    file_name=f"{analysis_symbol}_claude_analysis.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    key="dl_word_claude"
                )
            with col2:
                st.download_button(
                    "📥 Download PDF",
                    pdf_doc,
                    file_name=f"{analysis_symbol}_claude_analysis.pdf",
                    mime="application/pdf",
                    key="dl_pdf_claude"
                )

        with tab2:
            st.markdown(chatgpt_result)
            word_doc = create_word_document(chatgpt_result, analysis_symbol, "ChatGPT")
            pdf_doc = create_pdf_document(chatgpt_result, analysis_symbol, "ChatGPT")

            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "📥 Download Word",
                    word_doc,
                    file_name=f"{analysis_symbol}_chatgpt_analysis.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    key="dl_word_chatgpt"
                )
            with col2:
                st.download_button(
                    "📥 Download PDF",
                    pdf_doc,
                    file_name=f"{analysis_symbol}_chatgpt_analysis.pdf",
                    mime="application/pdf",
                    key="dl_pdf_chatgpt"
                )

    elif claude_result:
        st.markdown(claude_result)
        word_doc = create_word_document(claude_result, analysis_symbol, "Claude")
        pdf_doc = create_pdf_document(claude_result, analysis_symbol, "Claude")

        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "📥 Download Word",
                word_doc,
                file_name=f"{analysis_symbol}_claude_analysis.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key="dl_word_single"
            )
        with col2:
            st.download_button(
                "📥 Download PDF",
                pdf_doc,
                file_name=f"{analysis_symbol}_claude_analysis.pdf",
                mime="application/pdf",
                key="dl_pdf_single"
            )

    elif chatgpt_result:
        st.markdown(chatgpt_result)
        word_doc = create_word_document(chatgpt_result, analysis_symbol, "ChatGPT")
        pdf_doc = create_pdf_document(chatgpt_result, analysis_symbol, "ChatGPT")

        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "📥 Download Word",
                word_doc,
                file_name=f"{analysis_symbol}_chatgpt_analysis.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key="dl_word_gpt"
            )
        with col2:
            st.download_button(
                "📥 Download PDF",
                pdf_doc,
                file_name=f"{analysis_symbol}_chatgpt_analysis.pdf",
                mime="application/pdf",
                key="dl_pdf_gpt"
            )

    # Display financial charts at the bottom
    create_financial_charts(analysis_symbol)

else:
    st.info("👈 Enter a stock ticker and click 'Analyze Earnings' to start")

    # Show example
    st.markdown("""
    ### How it works:
    1. Enter any stock ticker (e.g., AAPL, NVDA, MSFT)
    2. Select number of quarters to analyze
    3. Choose AI model (Claude, ChatGPT, or both)
    4. Click Analyze and get detailed investment insights
    5. Download PDF or Word report, or email directly
    """)
