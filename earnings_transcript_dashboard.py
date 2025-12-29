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
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.enums import TA_CENTER
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
EMAIL_ADDRESS = get_api_key("EMAIL_ADDRESS")
EMAIL_PASSWORD = get_api_key("EMAIL_PASSWORD")


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


def create_pdf_document(content: str, symbol: str, ai_model: str) -> io.BytesIO:
    """Create PDF document and return as bytes"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                           rightMargin=0.75*inch, leftMargin=0.75*inch,
                           topMargin=0.75*inch, bottomMargin=0.75*inch)

    story = []
    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'],
                                 fontSize=18, textColor=colors.HexColor('#2c3e50'),
                                 spaceAfter=6, alignment=TA_CENTER)

    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'],
                                    fontSize=11, textColor=colors.HexColor('#555555'),
                                    alignment=TA_CENTER, spaceAfter=20)

    heading_style = ParagraphStyle('CustomHeading', parent=styles['Heading2'],
                                   fontSize=12, textColor=colors.HexColor('#34495e'),
                                   spaceAfter=8, spaceBefore=12, fontName='Helvetica-Bold')

    body_style = ParagraphStyle('Body', parent=styles['Normal'],
                                fontSize=10, spaceAfter=6, leading=14)

    # Add logo if exists
    logo_path = Path(__file__).parent / "company_logo.png"
    if logo_path.exists():
        try:
            logo = Image(str(logo_path), width=3*inch, height=1*inch, kind='proportional')
            logo.hAlign = 'CENTER'
            story.append(logo)
            story.append(Spacer(1, 0.2*inch))
        except:
            pass

    # Title
    story.append(Paragraph(f"{symbol} Earnings Transcript Analysis", title_style))
    story.append(Paragraph(f"Analysis by {ai_model}", subtitle_style))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", subtitle_style))
    story.append(Spacer(1, 0.2*inch))

    # Content
    for line in content.split('\n'):
        if line.strip():
            # Check if line starts with markdown headers
            if line.strip().startswith('#'):
                clean_text = line.strip().lstrip('#').strip()
                story.append(Paragraph(clean_text, heading_style))
            else:
                # Handle **bold** markdown - convert to <b> tags for reportlab
                formatted_line = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', line)
                try:
                    story.append(Paragraph(formatted_line, body_style))
                except:
                    # If parsing fails, add as plain text
                    story.append(Paragraph(line.replace('<', '&lt;').replace('>', '&gt;'), body_style))

    doc.build(story)
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

    # Store results in session state for persistence across button clicks
    st.session_state['claude_result'] = claude_result
    st.session_state['chatgpt_result'] = chatgpt_result
    st.session_state['analysis_symbol'] = symbol

    st.success("Analysis complete! See results below.")

# Email callback functions
def send_claude_email():
    st.session_state['send_claude_email'] = True

def send_chatgpt_email():
    st.session_state['send_chatgpt_email'] = True

def send_single_email():
    st.session_state['send_single_email'] = True

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

            # Email button
            st.divider()
            st.button("📧 Email Claude Report", key="email_claude", on_click=send_claude_email)

            # Process email if flag is set
            if st.session_state.get('send_claude_email'):
                word_doc_email = create_word_document(claude_result, analysis_symbol, "Claude")
                pdf_doc_email = create_pdf_document(claude_result, analysis_symbol, "Claude")
                success, message = send_email_with_attachments(
                    "daquinn@targetedequityconsulting.com",
                    analysis_symbol, "Claude", pdf_doc_email, word_doc_email
                )
                if success:
                    st.success(message)
                else:
                    st.error(message)
                st.session_state['send_claude_email'] = False

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

            # Email button
            st.divider()
            st.button("📧 Email ChatGPT Report", key="email_chatgpt", on_click=send_chatgpt_email)

            # Process email if flag is set
            if st.session_state.get('send_chatgpt_email'):
                word_doc_email = create_word_document(chatgpt_result, analysis_symbol, "ChatGPT")
                pdf_doc_email = create_pdf_document(chatgpt_result, analysis_symbol, "ChatGPT")
                success, message = send_email_with_attachments(
                    "daquinn@targetedequityconsulting.com",
                    analysis_symbol, "ChatGPT", pdf_doc_email, word_doc_email
                )
                if success:
                    st.success(message)
                else:
                    st.error(message)
                st.session_state['send_chatgpt_email'] = False

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

        # Email button
        st.divider()
        st.button("📧 Email Report", key="email_report", on_click=send_single_email)

        if st.session_state.get('send_single_email'):
            word_doc_email = create_word_document(claude_result, analysis_symbol, "Claude")
            pdf_doc_email = create_pdf_document(claude_result, analysis_symbol, "Claude")
            success, message = send_email_with_attachments(
                "daquinn@targetedequityconsulting.com",
                analysis_symbol, "Claude", pdf_doc_email, word_doc_email
            )
            if success:
                st.success(message)
            else:
                st.error(message)
            st.session_state['send_single_email'] = False

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

        # Email button
        st.divider()
        st.button("📧 Email Report", key="email_gpt", on_click=send_chatgpt_email)

        if st.session_state.get('send_chatgpt_email'):
            word_doc_email = create_word_document(chatgpt_result, analysis_symbol, "ChatGPT")
            pdf_doc_email = create_pdf_document(chatgpt_result, analysis_symbol, "ChatGPT")
            success, message = send_email_with_attachments(
                "daquinn@targetedequityconsulting.com",
                analysis_symbol, "ChatGPT", pdf_doc_email, word_doc_email
            )
            if success:
                st.success(message)
            else:
                st.error(message)
            st.session_state['send_chatgpt_email'] = False

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
