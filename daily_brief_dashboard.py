"""
Daily Brief Dashboard
Interactive Streamlit dashboard for comprehensive market overview
Matches the output format of daily_note_generator.py with email scraping
"""
import streamlit as st
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import imaplib
import email
from email.header import decode_header
import re
from bs4 import BeautifulSoup
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
EMAIL_ADDRESS = get_api_key("EMAIL_ADDRESS")
EMAIL_PASSWORD = get_api_key("EMAIL_PASSWORD")

# Target newsletter senders (email addresses mapped to display names)
TARGET_SENDERS = {
    "reply@email.investors.com": "Investors Business Daily",
    "pharma@endpointsnews.com": "Endpoints News",
    "info@kedm.com": "KEDM",
    "thebarronsdaily@barrons.com": "Barron's Daily",
    "newsletter@biopharmcatalyst.com": "BioPharmCatalyst",
    "yourweekendreading@substack.com": "ERIK - YWR",
    "Newsletter@io-fund.com": "I/O Fund",
    "Premium@io-fund.com": "I/O Fund Premium",
    "davelutz@bloomberg.net": "Dave Lutz",
    "contact@stockanalysis.com": "Stock Analysis"
}

# Disruption/Innovation Index tickers
PORTFOLIO_TICKERS = [
    "NVDA", "TSLA", "PLTR", "AMD", "COIN", "MSTR", "SHOP", "SQ", "ROKU", "CRWD",
    "NET", "DDOG", "SNOW", "ZS", "PANW", "AFRM", "UPST", "HOOD", "SOFI", "U",
    "RBLX", "ABNB", "UBER", "LYFT", "DASH", "RIVN", "LCID", "NIO", "XPEV", "LI"
]

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


# ============ EMAIL SCRAPING ============
def clean_html(html_content):
    """Convert HTML to clean text."""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        for element in soup(["script", "style", "meta", "link", "noscript", "head"]):
            element.decompose()
        text = soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()[:2000]
    except:
        return html_content[:500]


def decode_email_subject(subject):
    """Decode email subject."""
    if subject is None:
        return "No Subject"
    decoded_parts = decode_header(subject)
    subject_text = ""
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            subject_text += part.decode(encoding or 'utf-8', errors='ignore')
        else:
            subject_text += part
    return subject_text


def extract_email_body(msg):
    """Extract email body."""
    text_body = ""
    html_body = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))
            if "attachment" not in content_disposition:
                if content_type == "text/plain":
                    try:
                        text_body = part.get_payload(decode=True).decode('utf-8', errors='ignore').strip()
                    except:
                        pass
                elif content_type == "text/html":
                    try:
                        html_body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                    except:
                        pass
    else:
        content_type = msg.get_content_type()
        try:
            payload = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
            if content_type == "text/plain":
                text_body = payload.strip()
            elif content_type == "text/html":
                html_body = payload
        except:
            pass

    if text_body and len(text_body) > 50:
        return text_body
    elif html_body:
        return clean_html(html_body)
    return text_body or "No content available"


def fetch_emails_from_gmail():
    """Fetch emails from target senders within last 24 hours."""
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        return []

    emails_data = []
    try:
        # Clean credentials - remove any non-ASCII characters
        clean_email = EMAIL_ADDRESS.strip().replace('\xa0', ' ').replace('\u00a0', ' ').strip()
        clean_password = EMAIL_PASSWORD.strip().replace('\xa0', '').replace('\u00a0', '')

        # Ensure ASCII only
        clean_email = clean_email.encode('ascii', 'ignore').decode('ascii')
        clean_password = clean_password.encode('ascii', 'ignore').decode('ascii')

        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(clean_email, clean_password)
        mail.select('inbox')

        # Search last 24 hours (use yesterday's date to catch evening emails)
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")

        for sender_email, display_name in TARGET_SENDERS.items():
            try:
                # Search for emails from this sender since yesterday
                sender_clean = sender_email.encode('ascii', 'ignore').decode('ascii')
                search_criteria = f'(FROM "{sender_clean}" SINCE "{yesterday}")'
                _, message_numbers = mail.search(None, search_criteria.encode('utf-8'))

                if not message_numbers[0]:
                    continue

                for num in message_numbers[0].split()[:2]:  # Limit to 2 per sender
                    try:
                        _, msg_data = mail.fetch(num, '(RFC822)')
                        email_body = msg_data[0][1]
                        msg = email.message_from_bytes(email_body)

                        subject = decode_email_subject(msg['subject'])
                        # Clean non-ASCII characters
                        subject = subject.replace('\xa0', ' ').encode('ascii', 'ignore').decode('ascii')
                        body = extract_email_body(msg)
                        body = body.replace('\xa0', ' ')
                        date = msg['date'] or ''

                        emails_data.append({
                            'sender': display_name,  # Use friendly display name instead of email
                            'subject': subject,
                            'body': body[:1500],
                            'date': date
                        })
                    except Exception:
                        continue
            except Exception:
                continue

        mail.logout()
    except Exception as e:
        st.warning(f"Could not connect to email: {str(e).encode('ascii', 'ignore').decode('ascii')}")

    return emails_data


def summarize_newsletter(subject, body, sender):
    """Summarize newsletter with AI."""
    prompt = f"""Summarize this newsletter in 2-3 bullet points. Focus on key headlines and actionable insights.

Newsletter: {sender}
Subject: {subject}
Content: {body[:1500]}

Provide bullet points starting with •"""

    if ANTHROPIC_API_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            response = client.messages.create(
                model="claude-3-5-haiku-latest",
                max_tokens=200,
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
                max_tokens=200
            )
            return response.choices[0].message.content.strip()
        except:
            pass

    return body[:300] + "..."


# ============ MARKET DATA ============
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


@st.cache_data(ttl=300)
def fetch_portfolio_news(tickers):
    """Fetch news for Disruption/Innovation Index tickers."""
    if not FMP_API_KEY or not tickers:
        return []

    all_news = []
    tickers_str = ",".join(tickers[:20])  # Limit to 20 tickers

    try:
        url = f"https://financialmodelingprep.com/api/v3/stock_news?tickers={tickers_str}&limit=20&apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            all_news = response.json()
    except:
        pass

    return all_news[:15]


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


def generate_portfolio_summary(news_items):
    """Generate AI summary of portfolio/disruption index news."""
    if not news_items:
        return "No significant news for Disruption/Innovation Index holdings."

    news_text = "\n".join([f"• [{n.get('symbol', '')}] {n.get('title', '')}" for n in news_items[:12]])
    prompt = f"""Summarize the key news for these innovation/disruption stocks in 4-6 bullet points. Focus on material events, earnings, and price-moving catalysts.

Headlines:
{news_text}

Provide concise bullet points starting with •"""

    if ANTHROPIC_API_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            response = client.messages.create(
                model="claude-3-5-haiku-latest",
                max_tokens=500,
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
                max_tokens=500
            )
            return response.choices[0].message.content.strip()
        except:
            pass

    return "AI summary unavailable."


def create_pdf_report(index_data, treasury_data, fx_data, commodity_data, crypto_data,
                      sector_data, news, economic_calendar, ai_summary, premarket_movers,
                      portfolio_summary, newsletter_summaries):
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

    bold_style = ParagraphStyle('Bold', parent=styles['Normal'],
                                fontSize=12, fontName='Helvetica-Bold',
                                spaceAfter=8, spaceBefore=12)

    bullet_style = ParagraphStyle('BulletPoint', parent=styles['Normal'],
                                 fontSize=10, leftIndent=10, spaceAfter=6, leading=14)

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

    # SUMMARY SECTION
    story.append(Paragraph("<b>Summary</b>", bold_style))
    story.append(Spacer(1, 0.05*inch))

    # MARKET-MOVING NEWS
    if ai_summary:
        story.append(Paragraph("<b>Market-Moving News</b>", styles['Normal']))
        story.append(Spacer(1, 0.05*inch))
        for line in ai_summary.split('\n'):
            if line.strip():
                story.append(Paragraph(line.strip(), bullet_style))
        story.append(Spacer(1, 0.15*inch))

    # PRE-MARKET MOVERS
    if premarket_movers:
        story.append(Paragraph("<b>Pre-Market Movers</b>", styles['Normal']))
        story.append(Spacer(1, 0.05*inch))
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

    # DISRUPTION/INNOVATION INDEX NEWS
    if portfolio_summary:
        story.append(Paragraph("Disruption/Innovation Index News", heading_style))
        story.append(Spacer(1, 0.05*inch))
        for line in portfolio_summary.split('\n'):
            if line.strip():
                story.append(Paragraph(line.strip(), bullet_style))
        story.append(Spacer(1, 0.15*inch))

    # NEWSLETTER UPDATES
    if newsletter_summaries:
        story.append(Paragraph("Newsletter Updates", heading_style))
        for item in newsletter_summaries:
            story.append(Paragraph(f"<b>{item['sender']}</b>", styles['Normal']))
            story.append(Paragraph(f"<i>{item['subject']}</i>", styles['Normal']))
            story.append(Spacer(1, 0.03*inch))
            story.append(Paragraph(item['summary'], bullet_style))
            story.append(Spacer(1, 0.1*inch))

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
missing_keys = []
if not FMP_API_KEY:
    missing_keys.append("FMP_API_KEY")
if not EMAIL_ADDRESS:
    missing_keys.append("EMAIL_ADDRESS")
if not EMAIL_PASSWORD:
    missing_keys.append("EMAIL_PASSWORD")

if missing_keys:
    st.warning(f"Missing: {', '.join(missing_keys)}. Some features will be limited.")

if st.sidebar.button("🔄 Refresh Data", type="primary"):
    st.cache_data.clear()

# Sidebar options
st.sidebar.header("Settings")
fetch_newsletters = st.sidebar.checkbox("Fetch Email Newsletters", value=bool(EMAIL_ADDRESS and EMAIL_PASSWORD))
include_portfolio = st.sidebar.checkbox("Include Disruption Index News", value=True)

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

    # Portfolio news
    portfolio_news = []
    if include_portfolio:
        portfolio_news = fetch_portfolio_news(PORTFOLIO_TICKERS)

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

# ===== SUMMARY SECTION =====
st.header("📝 Summary")

# Market-Moving News - Auto-generate
st.subheader("Market-Moving News")
if 'ai_summary' not in st.session_state and news:
    with st.spinner("Generating AI summary..."):
        st.session_state['ai_summary'] = generate_ai_summary(news)

if 'ai_summary' in st.session_state:
    st.info(st.session_state['ai_summary'])

# ===== PRE-MARKET MOVERS =====
if premarket_movers:
    st.subheader("Pre-Market Movers")
    movers_df = pd.DataFrame([{
        'Symbol': m.get('symbol', ''),
        'Company': m.get('name', '')[:30],
        'Price': f"${m.get('price', 0):.2f}",
        'Change': f"{m.get('change', 0):+.2f}",
        '% Change': f"{m.get('changesPercentage', 0):+.2f}%"
    } for m in premarket_movers[:10]])
    st.dataframe(movers_df, use_container_width=True, hide_index=True)

# ===== DISRUPTION/INNOVATION INDEX NEWS =====
if include_portfolio and portfolio_news:
    st.header("🚀 Disruption/Innovation Index News")

    # Auto-generate portfolio summary
    if 'portfolio_summary' not in st.session_state:
        with st.spinner("Generating portfolio news summary..."):
            st.session_state['portfolio_summary'] = generate_portfolio_summary(portfolio_news)

    if 'portfolio_summary' in st.session_state:
        st.info(st.session_state['portfolio_summary'])

    with st.expander("View All Portfolio Headlines"):
        for item in portfolio_news[:10]:
            st.write(f"**[{item.get('symbol', '')}]** {item.get('title', '')}")
            st.caption(f"{item.get('publishedDate', '')[:10]}")

# ===== NEWSLETTER UPDATES =====
st.header("📧 Newsletter Updates")
if fetch_newsletters and EMAIL_ADDRESS and EMAIL_PASSWORD:
    # Auto-fetch newsletters
    if 'newsletter_summaries' not in st.session_state:
        with st.spinner("Fetching emails from Gmail..."):
            emails = fetch_emails_from_gmail()
            if emails:
                newsletter_summaries = []
                for em in emails:
                    summary = summarize_newsletter(em['subject'], em['body'], em['sender'])
                    newsletter_summaries.append({
                        'sender': em['sender'],
                        'subject': em['subject'],
                        'summary': summary
                    })
                st.session_state['newsletter_summaries'] = newsletter_summaries

    if 'newsletter_summaries' in st.session_state and st.session_state['newsletter_summaries']:
        for item in st.session_state['newsletter_summaries']:
            with st.expander(f"**{item['sender']}** - {item['subject'][:60]}"):
                st.write(item['summary'])
    else:
        st.info("No newsletters found for today")
else:
    st.info("Add EMAIL_ADDRESS and EMAIL_PASSWORD to secrets to enable newsletter fetching")

# ===== NEWS =====
st.header("📰 Market News Headlines")
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
        portfolio_summary = st.session_state.get('portfolio_summary', '')
        newsletter_summaries = st.session_state.get('newsletter_summaries', [])

        pdf = create_pdf_report(
            index_data, treasury_data, fx_data, commodity_data, crypto_data,
            sector_data, news, economic_calendar, ai_summary, premarket_movers,
            portfolio_summary, newsletter_summaries
        )
        st.download_button(
            "📄 Download PDF",
            pdf,
            file_name=f"Daily_Brief_{datetime.now().strftime('%Y-%m-%d')}.pdf",
            mime="application/pdf"
        )
