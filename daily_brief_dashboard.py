"""
Daily Brief Dashboard
Interactive Streamlit dashboard for comprehensive market overview
Matches the output format of daily_note_generator.py
"""
import streamlit as st
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import anthropic
from openai import OpenAI
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.enums import TA_CENTER
import io
from pathlib import Path

load_dotenv()

st.set_page_config(
    page_title="Daily Brief",
    page_icon="📋",
    layout="wide"
)

# API Keys
def get_api_key(key_name):
    key = os.getenv(key_name)
    if key:
        return key
    try:
        return st.secrets.get(key_name)
    except:
        return None

FMP_API_KEY = get_api_key("FMP_API_KEY")
ANTHROPIC_API_KEY = get_api_key("ANTHROPIC_API_KEY")
OPENAI_API_KEY = get_api_key("OPENAI_API_KEY")


# Market Data Symbols
INDEX_FUTURES = {
    "S&P 500": "ES=F",
    "NASDAQ": "NQ=F",
    "Russell 2000": "RTY=F",
    "Nikkei 225": "NIY=F"
}

TREASURY_TICKERS = {
    "US 30Y": "^TYX",
    "US 10Y": "^TNX",
    "US 2Y": "^IRX"
}

FX_SYMBOLS = {
    "EUR/USD": "EURUSD",
    "USD/JPY": "USDJPY",
    "USD/CNY": "USDCNY"
}

COMMODITY_SYMBOLS = {
    "WTI Crude": "CLUSD",
    "Gold": "GCUSD",
    "Copper": "HGUSD"
}

CRYPTO_SYMBOLS = {
    "Bitcoin": "BTCUSD",
    "Ethereum": "ETHUSD",
    "Solana": "SOLUSD"
}


@st.cache_data(ttl=300)
def fetch_yfinance_data(symbol):
    """Fetch data from yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d")
        if len(hist) >= 2:
            current = hist['Close'].iloc[-1]
            prev = hist['Close'].iloc[-2]
            change = current - prev
            change_pct = (change / prev) * 100
            return {"price": current, "change": change, "change_pct": change_pct}
        elif len(hist) == 1:
            return {"price": hist['Close'].iloc[-1], "change": 0, "change_pct": 0}
    except:
        pass
    return None


@st.cache_data(ttl=300)
def fetch_fmp_quote(symbol):
    """Fetch quote from FMP API."""
    if not FMP_API_KEY:
        return None
    try:
        url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                quote = data[0]
                return {
                    "price": quote.get("price", 0),
                    "change": quote.get("change", 0),
                    "change_pct": quote.get("changesPercentage", 0)
                }
    except:
        pass
    return None


@st.cache_data(ttl=300)
def fetch_sector_performance():
    """Fetch sector performance from FMP."""
    if not FMP_API_KEY:
        return None
    try:
        url = f"https://financialmodelingprep.com/api/v3/sectors-performance?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=10)
        data = response.json()
        return data if isinstance(data, list) else None
    except:
        return None


@st.cache_data(ttl=300)
def fetch_market_news():
    """Fetch market news from FMP."""
    if not FMP_API_KEY:
        return []
    try:
        url = f"https://financialmodelingprep.com/api/v3/stock_news?limit=15&apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=10)
        return response.json() if response.status_code == 200 else []
    except:
        return []


@st.cache_data(ttl=300)
def fetch_economic_calendar():
    """Fetch economic calendar from FMP."""
    if not FMP_API_KEY:
        return []
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        week_later = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        url = f"https://financialmodelingprep.com/api/v3/economic_calendar?from={today}&to={week_later}&apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=10)
        return response.json()[:15] if response.status_code == 200 else []
    except:
        return []


@st.cache_data(ttl=300)
def fetch_premarket_movers():
    """Fetch pre-market gainers from FMP."""
    if not FMP_API_KEY:
        return []
    try:
        url = f"https://financialmodelingprep.com/api/v3/stock_market/gainers?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=10)
        data = response.json()
        return data[:10] if isinstance(data, list) else []
    except:
        return []


def generate_ai_summary(news_items):
    """Generate AI summary of market news."""
    if not news_items:
        return "No market news available."

    news_text = "\n".join([f"• {n.get('title', '')}" for n in news_items[:10]])
    prompt = f"""Summarize the key market-moving news in 3-5 bullet points. Focus on what matters for investors today.

Headlines:
{news_text}

Provide concise bullet points starting with •"""

    if ANTHROPIC_API_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            response = client.messages.create(
                model="claude-3-5-haiku-latest",
                max_tokens=400,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except:
            pass

    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=400
            )
            return response.choices[0].message.content.strip()
        except:
            pass

    return "AI summary unavailable."


def create_pdf_report(index_data, treasury_data, fx_data, commodity_data, crypto_data,
                      sector_data, news, economic_calendar, ai_summary, premarket_movers):
    """Create professional PDF matching original daily_note_generator output."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                           rightMargin=0.5*inch, leftMargin=0.5*inch,
                           topMargin=0.5*inch, bottomMargin=0.5*inch)

    story = []
    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'],
                                 fontSize=24, textColor=colors.HexColor('#2c3e50'),
                                 spaceAfter=12, alignment=TA_CENTER)

    heading_style = ParagraphStyle('CustomHeading', parent=styles['Heading2'],
                                   fontSize=14, textColor=colors.HexColor('#34495e'),
                                   spaceAfter=10, spaceBefore=15)

    tagline_style = ParagraphStyle('Tagline', parent=styles['Normal'],
                                   fontSize=10, textColor=colors.HexColor('#555555'),
                                   alignment=TA_CENTER, fontName='Helvetica-Oblique',
                                   spaceAfter=20)

    # Add logo if exists
    logo_path = Path(__file__).parent / "company_logo.png"
    if logo_path.exists():
        try:
            logo = Image(str(logo_path), width=6.0*inch, height=2.0*inch, kind='proportional')
            logo.hAlign = 'CENTER'
            story.append(logo)
            story.append(Spacer(1, 0.08*inch))
        except:
            pass

    # Tagline and Title
    story.append(Paragraph("Precision Analysis for Informed Investment Decisions", tagline_style))
    story.append(Paragraph(f"Daily Brief - {datetime.now().strftime('%B %d, %Y')}", title_style))
    story.append(Spacer(1, 0.12*inch))

    # Table style
    compact_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (1, -1), 'LEFT'),
        ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
    ])

    # GLOBAL MARKETS TABLE
    story.append(Paragraph("Global Markets", heading_style))

    main_table_data = [['Category', 'Item', 'Price/Rate', 'Yield', 'Δ%']]

    # Index Futures
    first = True
    for name, data in index_data.items():
        if data:
            cat = "Index Futures" if first else ""
            main_table_data.append([cat, name, f"{data['price']:,.2f}", "-", f"{data['change_pct']:+.2f}%"])
            first = False

    # Fixed Income
    first = True
    for name, data in treasury_data.items():
        if data:
            cat = "Fixed Income" if first else ""
            main_table_data.append([cat, name, "-", f"{data['price']:.2f}%", f"{data['change_pct']:+.2f}%"])
            first = False

    # Commodities
    first = True
    for name, data in commodity_data.items():
        if data:
            cat = "Commodities" if first else ""
            main_table_data.append([cat, name, f"${data['price']:,.2f}", "-", f"{data['change_pct']:+.2f}%"])
            first = False

    main_table = Table(main_table_data, colWidths=[1.1*inch, 1.1*inch, 1.0*inch, 0.7*inch, 0.8*inch])
    main_table.setStyle(compact_style)
    main_table.hAlign = 'CENTER'
    story.append(main_table)
    story.append(Spacer(1, 0.1*inch))

    # FX & CRYPTO TABLE
    fx_crypto_data = [['Category', 'Item', 'Price/Rate', 'Δ%']]

    first = True
    for name, data in fx_data.items():
        if data:
            cat = "Foreign Exchange" if first else ""
            fx_crypto_data.append([cat, name, f"{data['price']:.4f}", f"{data['change_pct']:+.2f}%"])
            first = False

    first = True
    for name, data in crypto_data.items():
        if data:
            cat = "Crypto Currencies" if first else ""
            fx_crypto_data.append([cat, name, f"${data['price']:,.2f}", f"{data['change_pct']:+.2f}%"])
            first = False

    fx_table = Table(fx_crypto_data, colWidths=[1.1*inch, 1.1*inch, 1.5*inch, 1.0*inch])
    fx_table.setStyle(compact_style)
    fx_table.hAlign = 'CENTER'
    story.append(fx_table)
    story.append(Spacer(1, 0.15*inch))

    # SECTOR PERFORMANCE
    if sector_data:
        story.append(Paragraph("Sector Performance", heading_style))
        sector_table_data = [['Sector', 'Change']]
        for item in sector_data[:11]:
            change = item.get('changesPercentage', '0%')
            sector_table_data.append([item['sector'], change])

        sector_table = Table(sector_table_data, colWidths=[4*inch, 1.5*inch])
        sector_table.setStyle(compact_style)
        story.append(sector_table)
        story.append(Spacer(1, 0.15*inch))

    # AI MARKET SUMMARY
    if ai_summary:
        story.append(Paragraph("Market-Moving News", heading_style))
        bullet_style = ParagraphStyle('BulletPoint', parent=styles['Normal'],
                                     fontSize=10, leftIndent=10, spaceAfter=8, leading=14)
        for line in ai_summary.split('\n'):
            if line.strip():
                story.append(Paragraph(line.strip(), bullet_style))
        story.append(Spacer(1, 0.15*inch))

    # PRE-MARKET MOVERS
    if premarket_movers:
        story.append(Paragraph("Pre-Market Movers", heading_style))
        movers_data = [['Symbol', 'Company', 'Price', 'Change', '% Change']]
        for m in premarket_movers[:8]:
            change = m.get('change', 0)
            pct = m.get('changesPercentage', 0)
            movers_data.append([
                m.get('symbol', ''),
                m.get('name', '')[:25],
                f"${m.get('price', 0):.2f}",
                f"{change:+.2f}",
                f"{pct:+.2f}%"
            ])

        movers_table = Table(movers_data, colWidths=[0.8*inch, 2.5*inch, 1*inch, 1*inch, 1*inch])
        movers_table.setStyle(compact_style)
        story.append(movers_table)
        story.append(Spacer(1, 0.15*inch))

    # ECONOMIC CALENDAR
    if economic_calendar:
        story.append(Paragraph("US Economic Calendar", heading_style))
        cal_data = [['Date', 'Event', 'Actual', 'Estimate', 'Previous']]
        for event in economic_calendar[:10]:
            date = event.get('date', '')[:10]
            cal_data.append([
                date,
                event.get('event', '')[:40],
                str(event.get('actual', '-')),
                str(event.get('estimate', '-')),
                str(event.get('previous', '-'))
            ])

        cal_table = Table(cal_data, colWidths=[0.9*inch, 3*inch, 0.8*inch, 0.8*inch, 0.8*inch])
        cal_table.setStyle(compact_style)
        story.append(cal_table)

    doc.build(story)
    buffer.seek(0)
    return buffer


# ============ MAIN APP ============
st.title("📋 Daily Brief Dashboard")
st.markdown("*Precision Analysis for Informed Investment Decisions*")

# Check API keys
if not FMP_API_KEY:
    st.warning("FMP_API_KEY not found. Some features will be limited.")

if st.sidebar.button("🔄 Refresh Data", type="primary"):
    st.cache_data.clear()

# Fetch all data
with st.spinner("Fetching market data..."):
    # Index Futures
    index_data = {name: fetch_yfinance_data(symbol) for name, symbol in INDEX_FUTURES.items()}

    # Treasuries
    treasury_data = {name: fetch_yfinance_data(symbol) for name, symbol in TREASURY_TICKERS.items()}

    # FX
    fx_data = {name: fetch_fmp_quote(symbol) for name, symbol in FX_SYMBOLS.items()}

    # Commodities
    commodity_data = {name: fetch_fmp_quote(symbol) for name, symbol in COMMODITY_SYMBOLS.items()}

    # Crypto
    crypto_data = {name: fetch_fmp_quote(symbol) for name, symbol in CRYPTO_SYMBOLS.items()}

    # Other data
    sector_data = fetch_sector_performance()
    news = fetch_market_news()
    economic_calendar = fetch_economic_calendar()
    premarket_movers = fetch_premarket_movers()

# ===== GLOBAL MARKETS =====
st.header("🌍 Global Markets")

# Index Futures
st.subheader("Index Futures")
cols = st.columns(4)
for i, (name, data) in enumerate(index_data.items()):
    if data:
        with cols[i]:
            st.metric(name, f"{data['price']:,.2f}", f"{data['change_pct']:+.2f}%")

# Fixed Income
st.subheader("Fixed Income (Yields)")
cols = st.columns(3)
for i, (name, data) in enumerate(treasury_data.items()):
    if data:
        with cols[i]:
            st.metric(name, f"{data['price']:.2f}%", f"{data['change_pct']:+.2f}%", delta_color="inverse")

# Commodities
st.subheader("Commodities")
cols = st.columns(3)
for i, (name, data) in enumerate(commodity_data.items()):
    if data:
        with cols[i]:
            st.metric(name, f"${data['price']:,.2f}", f"{data['change_pct']:+.2f}%")

# FX & Crypto side by side
col1, col2 = st.columns(2)

with col1:
    st.subheader("Foreign Exchange")
    for name, data in fx_data.items():
        if data:
            st.metric(name, f"{data['price']:.4f}", f"{data['change_pct']:+.2f}%")

with col2:
    st.subheader("Crypto Currencies")
    for name, data in crypto_data.items():
        if data:
            st.metric(name, f"${data['price']:,.2f}", f"{data['change_pct']:+.2f}%")

# ===== SECTOR PERFORMANCE =====
if sector_data:
    st.header("📊 Sector Performance")
    cols = st.columns(4)
    for i, sector in enumerate(sector_data[:12]):
        with cols[i % 4]:
            change = sector.get('changesPercentage', '0%')
            st.metric(sector['sector'], change)

# ===== AI SUMMARY =====
st.header("🤖 Market-Moving News Summary")
if st.button("Generate AI Summary"):
    with st.spinner("Generating AI summary..."):
        ai_summary = generate_ai_summary(news)
        st.session_state['ai_summary'] = ai_summary
        st.info(ai_summary)
elif 'ai_summary' in st.session_state:
    st.info(st.session_state['ai_summary'])

# ===== PRE-MARKET MOVERS =====
if premarket_movers:
    st.header("📈 Pre-Market Movers")
    movers_df = pd.DataFrame([{
        'Symbol': m.get('symbol', ''),
        'Company': m.get('name', '')[:30],
        'Price': f"${m.get('price', 0):.2f}",
        'Change': f"{m.get('change', 0):+.2f}",
        '% Change': f"{m.get('changesPercentage', 0):+.2f}%"
    } for m in premarket_movers[:10]])
    st.dataframe(movers_df, use_container_width=True, hide_index=True)

# ===== NEWS =====
st.header("📰 Market News")
for item in news[:8]:
    with st.expander(item.get('title', 'N/A')[:100]):
        st.write(item.get('text', 'No content')[:500])
        st.caption(f"Source: {item.get('site', 'Unknown')} | {item.get('publishedDate', '')[:10]}")

# ===== ECONOMIC CALENDAR =====
if economic_calendar:
    st.header("📅 Economic Calendar (Next 7 Days)")
    cal_df = pd.DataFrame([{
        'Date': e.get('date', '')[:10],
        'Event': e.get('event', '')[:50],
        'Actual': e.get('actual', '-'),
        'Estimate': e.get('estimate', '-'),
        'Previous': e.get('previous', '-')
    } for e in economic_calendar[:12]])
    st.dataframe(cal_df, use_container_width=True, hide_index=True)

# ===== DOWNLOAD PDF =====
st.divider()
st.subheader("📥 Download Daily Brief")

if st.button("Generate PDF Report"):
    with st.spinner("Generating PDF..."):
        ai_summary = st.session_state.get('ai_summary', generate_ai_summary(news))
        pdf = create_pdf_report(
            index_data, treasury_data, fx_data, commodity_data, crypto_data,
            sector_data, news, economic_calendar, ai_summary, premarket_movers
        )
        st.download_button(
            "📄 Download PDF",
            pdf,
            file_name=f"Daily_Brief_{datetime.now().strftime('%Y-%m-%d')}.pdf",
            mime="application/pdf"
        )
