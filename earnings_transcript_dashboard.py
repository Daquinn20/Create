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
from docx import Document
from docx.shared import Pt, RGBColor, Inches, Twips
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import io
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
    except:
        return None

FMP_API_KEY = get_api_key("FMP_API_KEY")
ANTHROPIC_API_KEY = get_api_key("ANTHROPIC_API_KEY")
OPENAI_API_KEY = get_api_key("OPENAI_API_KEY")


def fetch_transcripts(symbol: str, num_quarters: int = 4) -> List[Dict]:
    """Fetch earnings transcripts from FMP API"""
    url = f"https://financialmodelingprep.com/api/v4/batch_earning_call_transcript/{symbol.upper()}"
    params = {'year': datetime.now().year, 'apikey': FMP_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict) and 'Error Message' in data:
            return []

        if not isinstance(data, list) or len(data) == 0:
            return []

        transcripts = []
        for item in data[:num_quarters]:
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


def get_company_profile(symbol: str) -> Optional[Dict]:
    """Get company profile from FMP"""
    url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}"
    params = {'apikey': FMP_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data[0] if isinstance(data, list) and len(data) > 0 else None
    except:
        return None


def create_analysis_prompt(symbol: str, transcripts: List[Dict], company_info: Optional[Dict] = None) -> str:
    """Create the analysis prompt"""
    header = f"COMPANY: {symbol}\n"
    if company_info:
        header += f"Name: {company_info.get('companyName', 'N/A')}\n"
        header += f"Industry: {company_info.get('industry', 'N/A')}\n"
        header += f"Sector: {company_info.get('sector', 'N/A')}\n"
    header += "\n"

    combined_text = ""
    for i, transcript in enumerate(transcripts, 1):
        combined_text += f"\n\n{'=' * 80}\n"
        combined_text += f"TRANSCRIPT {i}: Q{transcript['quarter']} {transcript['year']}\n"
        combined_text += f"Date: {transcript['date']}\n"
        combined_text += f"{'=' * 80}\n\n"
        combined_text += transcript['content']

    prompt = f"""{header}
Please analyze these {len(transcripts)} earnings call transcripts for {symbol} and provide a comprehensive investment-focused summary.

Include:

1. GUIDANCE CHANGES (Critical):
   - Revenue guidance changes
   - Margin guidance changes
   - Debt/Capital changes

2. MANAGEMENT & LEADERSHIP:
   - Executive changes
   - Strategic priority shifts

3. TONE ANALYSIS:
   - Overall tone: Bullish or Bearish vs prior quarters?
   - Confidence level changes

4. POSITIVE HIGHLIGHTS:
   - Guidance raises, growth drivers, market share gains

5. NEGATIVE HIGHLIGHTS / RED FLAGS:
   - Guidance cuts, margin compression, competitive pressures

6. QUARTER-OVER-QUARTER CHANGES:
   - New topics this quarter
   - Topics being avoided

7. Investment Implications:
   - Bull/bear case, key debates, what to watch

{combined_text}

Provide detailed, objective analysis for investment decision-making."""

    return prompt


def analyze_with_claude(prompt: str) -> str:
    """Analyze using Claude"""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=8000,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


def analyze_with_chatgpt(symbol: str, transcripts: List[Dict], company_info: Optional[Dict] = None) -> tuple:
    """Analyze using ChatGPT with automatic fallback for rate limits"""
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    # Try with all transcripts first
    quarters_to_try = len(transcripts)

    while quarters_to_try >= 1:
        try:
            prompt = create_analysis_prompt(symbol, transcripts[:quarters_to_try], company_info)
            response = client.chat.completions.create(
                model="gpt-4-turbo-preview",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=4096
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


def create_word_document(content: str, symbol: str, ai_model: str) -> io.BytesIO:
    """Create Word document and return as bytes"""
    doc = Document()

    # Add page border
    add_page_border(doc)

    # Add company logo at top center
    logo_path = Path(__file__).parent / "company_logo.png"
    if logo_path.exists():
        try:
            logo_paragraph = doc.add_paragraph()
            logo_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
            run = logo_paragraph.add_run()
            run.add_picture(str(logo_path), width=Inches(3))
            doc.add_paragraph()  # Spacer after logo
        except Exception as e:
            pass  # Skip logo if there's an error

    # Title
    title = doc.add_paragraph(f"{symbol} Earnings Transcript Analysis")
    title.style = 'Heading 1'
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Subtitle
    subtitle = doc.add_paragraph(f"Analysis by {ai_model}")
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    date_line = doc.add_paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    date_line.alignment = WD_ALIGN_PARAGRAPH.CENTER
    doc.add_paragraph()

    # Content
    for line in content.split('\n'):
        if line.strip():
            # Check if line starts with markdown headers (#, ##, ###, etc.)
            if line.strip().startswith('#'):
                # Remove # symbols and make bold
                clean_text = line.strip().lstrip('#').strip()
                p = doc.add_paragraph()
                run = p.add_run(clean_text)
                run.bold = True
                run.font.size = Pt(12)
            else:
                # Handle **bold** markdown syntax
                p = doc.add_paragraph()
                # Split by **text** pattern
                parts = re.split(r'(\*\*.*?\*\*)', line)
                for part in parts:
                    if part.startswith('**') and part.endswith('**'):
                        # Bold text - remove ** and make bold
                        run = p.add_run(part[2:-2])
                        run.bold = True
                    else:
                        # Regular text
                        if part:
                            p.add_run(part)

    # Save to bytes
    buffer = io.BytesIO()
    doc.save(buffer)
    buffer.seek(0)
    return buffer


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

# Main content
if st.sidebar.button("🔍 Analyze Earnings", type="primary"):

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

    # Create prompt
    prompt = create_analysis_prompt(symbol, transcripts, company_info)

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
                chatgpt_result, chatgpt_note = analyze_with_chatgpt(symbol, transcripts, company_info)
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

    # Display results
    st.header("📊 Analysis Results")

    if ai_choice == "Both" and claude_result and chatgpt_result:
        tab1, tab2 = st.tabs(["Claude Analysis", "ChatGPT Analysis"])

        with tab1:
            st.markdown(claude_result)
            doc = create_word_document(claude_result, symbol, "Claude")
            st.download_button(
                "📥 Download Claude Report (Word)",
                doc,
                file_name=f"{symbol}_claude_analysis.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )

        with tab2:
            st.markdown(chatgpt_result)
            doc = create_word_document(chatgpt_result, symbol, "ChatGPT")
            st.download_button(
                "📥 Download ChatGPT Report (Word)",
                doc,
                file_name=f"{symbol}_chatgpt_analysis.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )

    elif claude_result:
        st.markdown(claude_result)
        doc = create_word_document(claude_result, symbol, "Claude")
        st.download_button(
            "📥 Download Report (Word)",
            doc,
            file_name=f"{symbol}_claude_analysis.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )

    elif chatgpt_result:
        st.markdown(chatgpt_result)
        doc = create_word_document(chatgpt_result, symbol, "ChatGPT")
        st.download_button(
            "📥 Download Report (Word)",
            doc,
            file_name=f"{symbol}_chatgpt_analysis.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )

else:
    st.info("👈 Enter a stock ticker and click 'Analyze Earnings' to start")

    # Show example
    st.markdown("""
    ### How it works:
    1. Enter any stock ticker (e.g., AAPL, NVDA, MSFT)
    2. Select number of quarters to analyze
    3. Choose AI model (Claude, ChatGPT, or both)
    4. Click Analyze and get detailed investment insights
    5. Download Word report for your records
    """)
