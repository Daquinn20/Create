"""
Industry Report Dashboard - Streamlit Version
Generates comprehensive industry/sector analysis reports with AI-powered insights
"""
import streamlit as st
import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import json
from io import BytesIO
import anthropic
from openai import OpenAI
from dotenv import load_dotenv
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph,
    Spacer, Image, PageBreak
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.colors import HexColor
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

load_dotenv()

# Load config for email - check Streamlit secrets, env vars, then config.json
def load_config():
    config = {}

    # Try config.json first
    try:
        with open("config.json", 'r') as f:
            config = json.load(f)
    except:
        pass

    # Override with Streamlit secrets if available
    try:
        if hasattr(st, 'secrets'):
            if 'EMAIL_ADDRESS' in st.secrets:
                config['email_address'] = st.secrets['EMAIL_ADDRESS']
            if 'EMAIL_PASSWORD' in st.secrets:
                config['password'] = st.secrets['EMAIL_PASSWORD']
            if 'email_address' in st.secrets:
                config['email_address'] = st.secrets['email_address']
            if 'email_password' in st.secrets:
                config['password'] = st.secrets['email_password']
    except:
        pass

    # Override with environment variables
    if os.getenv('EMAIL_ADDRESS'):
        config['email_address'] = os.getenv('EMAIL_ADDRESS')
    if os.getenv('EMAIL_PASSWORD'):
        config['password'] = os.getenv('EMAIL_PASSWORD')

    return config

CONFIG = load_config()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Initialize AI clients
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Default stock universe path
DEFAULT_UNIVERSE_PATH = r"C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\AI dashboard Data\master_universe.csv"

# Import from industry_report_generator
from industry_report_generator import (
    # Data fetching
    get_sector_performance,
    get_sector_pe_ratios,
    get_stocks_by_sector,
    get_stocks_by_industry,
    get_company_profile,
    # AI Analysis
    generate_industry_analysis,
    identify_winners_losers,
    # Data classes
    ResearchNotes,
    Article,
    WinnersLosersAnalysis,
    CompanyTrendPosition,
    create_research_notes,
    # PDF generation
    generate_industry_pdf,
    # Utilities
    format_currency,
)


# ============================================
# DOCUMENT READING FUNCTIONS
# ============================================

def read_word_document(file_buffer) -> str:
    """Extract text from a Word document."""
    try:
        from docx import Document
        doc = Document(file_buffer)
        full_text = []
        for para in doc.paragraphs:
            if para.text.strip():
                full_text.append(para.text)
        for table in doc.tables:
            for row in table.rows:
                row_text = [cell.text.strip() for cell in row.cells if cell.text.strip()]
                if row_text:
                    full_text.append(" | ".join(row_text))
        return "\n".join(full_text)
    except Exception as e:
        logger.error(f"Error reading Word document: {e}")
        return ""


def read_pdf_document(file_buffer) -> str:
    """Extract text from a PDF document."""
    try:
        import pdfplumber
        full_text = []
        with pdfplumber.open(file_buffer) as pdf:
            for page in pdf.pages[:50]:  # Limit to 50 pages
                text = page.extract_text()
                if text:
                    full_text.append(text)
        return "\n".join(full_text)
    except Exception as e:
        logger.error(f"Error reading PDF document: {e}")
        return ""


def read_text_document(file_buffer) -> str:
    """Read a plain text file."""
    try:
        return file_buffer.read().decode('utf-8')
    except Exception as e:
        logger.error(f"Error reading text file: {e}")
        return ""


def read_uploaded_document(uploaded_file) -> str:
    """Read an uploaded document (Word, PDF, or TXT)."""
    if uploaded_file is None:
        return ""

    filename = uploaded_file.name.lower()
    if filename.endswith('.docx'):
        return read_word_document(uploaded_file)
    elif filename.endswith('.pdf'):
        return read_pdf_document(uploaded_file)
    elif filename.endswith('.txt'):
        return read_text_document(uploaded_file)
    else:
        return ""


# ============================================
# PDF GENERATION FOR WINNERS/LOSERS
# ============================================

def generate_winners_losers_word(
    industry_name: str,
    winners_losers: WinnersLosersAnalysis,
    original_file_buffer: BytesIO = None
) -> BytesIO:
    """Generate Word document by copying original and appending winners/losers."""
    from docx import Document
    from docx.shared import Inches, Pt, RGBColor
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.enum.table import WD_TABLE_ALIGNMENT
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    # If we have original file, copy it; otherwise create new
    if original_file_buffer:
        original_file_buffer.seek(0)
        doc = Document(original_file_buffer)
    else:
        doc = Document()

    # Add page break before Winners/Losers section
    doc.add_page_break()

    # Add Winners/Losers header
    header = doc.add_heading('Winners & Losers Analysis', level=1)
    header.alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Add summary if exists
    if winners_losers.summary:
        summary_para = doc.add_paragraph()
        summary_para.add_run(winners_losers.summary).italic = True
        doc.add_paragraph()

    # WINNERS section
    if winners_losers.winners:
        winners_heading = doc.add_heading('WINNERS', level=2)
        for run in winners_heading.runs:
            run.font.color.rgb = RGBColor(21, 87, 36)  # Dark green

        # Create winners table
        table = doc.add_table(rows=1, cols=3)
        try:
            table.style = 'Table Grid'
        except KeyError:
            pass  # Style not available, use default

        # Header row
        header_cells = table.rows[0].cells
        header_cells[0].text = 'Symbol'
        header_cells[1].text = 'Company'
        header_cells[2].text = 'Rationale'

        # Style header
        for cell in header_cells:
            cell.paragraphs[0].runs[0].bold = True
            shading = OxmlElement('w:shd')
            shading.set(qn('w:fill'), 'd4edda')
            cell._tc.get_or_add_tcPr().append(shading)

        # Add winner rows
        for w in winners_losers.winners:
            row = table.add_row().cells
            row[0].text = w.symbol
            row[1].text = w.company_name
            row[2].text = w.rationale

        doc.add_paragraph()

    # LOSERS section
    if winners_losers.losers:
        losers_heading = doc.add_heading('LOSERS', level=2)
        for run in losers_heading.runs:
            run.font.color.rgb = RGBColor(114, 28, 36)  # Dark red

        # Create losers table
        table = doc.add_table(rows=1, cols=3)
        try:
            table.style = 'Table Grid'
        except KeyError:
            pass  # Style not available, use default

        # Header row
        header_cells = table.rows[0].cells
        header_cells[0].text = 'Symbol'
        header_cells[1].text = 'Company'
        header_cells[2].text = 'Rationale'

        # Style header
        for cell in header_cells:
            cell.paragraphs[0].runs[0].bold = True
            shading = OxmlElement('w:shd')
            shading.set(qn('w:fill'), 'f8d7da')
            cell._tc.get_or_add_tcPr().append(shading)

        # Add loser rows
        for l in winners_losers.losers:
            row = table.add_row().cells
            row[0].text = l.symbol
            row[1].text = l.company_name
            row[2].text = l.rationale

    # Save to buffer
    buffer = BytesIO()
    doc.save(buffer)
    buffer.seek(0)
    return buffer


def generate_winners_losers_pdf(
    industry_name: str,
    trends_data: Dict[str, Any],
    winners_losers: WinnersLosersAnalysis,
    original_note_content: str = None
) -> BytesIO:
    """Generate simple PDF with just winners and losers tables (for appending to original)."""

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch
    )

    # Styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        alignment=TA_CENTER,
        spaceAfter=5,
        textColor=HexColor('#1a1a2e')
    )
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=12,
        spaceBefore=15,
        spaceAfter=8,
        textColor=HexColor('#2E86AB')
    )
    rationale_style = ParagraphStyle(
        'Rationale',
        parent=styles['Normal'],
        fontSize=9,
        textColor=HexColor('#333333'),
        leftIndent=10,
        spaceAfter=6,
        leading=11
    )

    elements = []

    # Add company logo if exists
    logo_path = CONFIG.get('logo_path', 'company_logo.png')
    if os.path.exists(logo_path):
        try:
            img = Image(logo_path, width=4.68*inch, height=1.56*inch)
            img.hAlign = 'CENTER'
            elements.append(img)
            elements.append(Spacer(1, 0.1*inch))
        except Exception as e:
            logger.warning(f"Could not load logo: {e}")

    # Winners Table
    if winners_losers.winners:
        elements.append(Paragraph("WINNERS", heading_style))

        winner_data = [["Symbol", "Company", "Rationale"]]
        for w in winners_losers.winners:
            winner_data.append([
                w.symbol,
                (w.company_name[:30] + "...") if len(w.company_name) > 30 else w.company_name,
                (w.rationale[:80] + "...") if len(w.rationale) > 80 else w.rationale
            ])

        winner_table = Table(winner_data, colWidths=[55, 130, 275])
        winner_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#d4edda')),
            ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#155724')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (1, -1), 'LEFT'),
            ('ALIGN', (2, 0), (2, -1), 'LEFT'),
            ('BOX', (0, 0), (-1, -1), 1.5, HexColor('#155724')),
            ('INNERGRID', (0, 0), (-1, -1), 0.5, HexColor('#c3e6cb')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#f8fff8')]),
        ]))
        elements.append(winner_table)
        elements.append(Spacer(1, 20))

    # Losers Table
    if winners_losers.losers:
        elements.append(Paragraph("LOSERS", heading_style))

        loser_data = [["Symbol", "Company", "Rationale"]]
        for l in winners_losers.losers:
            loser_data.append([
                l.symbol,
                (l.company_name[:30] + "...") if len(l.company_name) > 30 else l.company_name,
                (l.rationale[:80] + "...") if len(l.rationale) > 80 else l.rationale
            ])

        loser_table = Table(loser_data, colWidths=[55, 130, 275])
        loser_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#f8d7da')),
            ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#721c24')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (1, -1), 'LEFT'),
            ('ALIGN', (2, 0), (2, -1), 'LEFT'),
            ('BOX', (0, 0), (-1, -1), 1.5, HexColor('#721c24')),
            ('INNERGRID', (0, 0), (-1, -1), 0.5, HexColor('#f5c6cb')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#fff8f8')]),
        ]))
        elements.append(loser_table)

    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    return buffer


def send_email_with_attachment(
    file_buffer: BytesIO,
    industry_name: str,
    file_type: str = "pdf",  # "pdf" or "docx"
    recipient_email: str = None
) -> Tuple[bool, str]:
    """Send email with PDF or Word attachment."""
    try:
        email_address = CONFIG.get('email_address')
        password = CONFIG.get('password')
        smtp_server = CONFIG.get('smtp_server', 'smtp.gmail.com')
        smtp_port = CONFIG.get('smtp_port', 587)
        recipient = recipient_email or CONFIG.get('email_recipient', email_address)

        if not email_address or not password:
            return False, "Email credentials not configured in config.json"

        # Create message
        msg = MIMEMultipart()
        msg['From'] = email_address
        msg['To'] = recipient
        msg['Subject'] = f"{industry_name} - Winners & Losers Analysis"

        # Email body
        body = f"""
{industry_name} Analysis Report

Please find attached the Winners & Losers analysis report.

Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}

---
Targeted Equity Consulting Group
Precision Analysis for Informed Investment Decisions
        """
        msg.attach(MIMEText(body, 'plain'))

        # Attach file
        file_buffer.seek(0)
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(file_buffer.read())
        encoders.encode_base64(attachment)

        safe_name = industry_name.replace(' ', '_').replace('/', '_')
        date_str = datetime.now().strftime('%Y%m%d')
        extension = "docx" if file_type == "docx" else "pdf"
        attachment.add_header(
            'Content-Disposition',
            'attachment',
            filename=f"{safe_name}_Winners_Losers_{date_str}.{extension}"
        )
        msg.attach(attachment)

        # Send email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(email_address, password)
        server.sendmail(email_address, recipient, msg.as_string())
        server.quit()

        return True, f"Email sent successfully to {recipient}!"

    except Exception as e:
        logger.error(f"Email error: {e}")
        return False, f"Email error: {str(e)}"


def send_email_with_pdf(
    pdf_buffer: BytesIO,
    industry_name: str,
    recipient_email: str = None
) -> Tuple[bool, str]:
    """Send email with PDF attachment (backwards compatible wrapper)."""
    return send_email_with_attachment(pdf_buffer, industry_name, "pdf", recipient_email)


# ============================================
# UNIVERSE SCANNING FUNCTIONS
# ============================================

def load_stock_universe(file_path: str = None, uploaded_file=None) -> pd.DataFrame:
    """Load stock universe from CSV file."""
    try:
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file, header=None)
        elif file_path and os.path.exists(file_path):
            df = pd.read_csv(file_path, header=None)
        else:
            return pd.DataFrame()

        # Check if first row looks like headers
        first_row = df.iloc[0].astype(str)
        has_header = any(col.lower() in ['symbol', 'ticker', 'name', 'company', 'exchange']
                        for col in first_row.values)

        if has_header:
            # Re-read with headers
            if uploaded_file is not None:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file)
            elif file_path:
                df = pd.read_csv(file_path)
        else:
            # Assign default column names: Symbol, Company, Exchange
            if len(df.columns) >= 3:
                df.columns = ['Symbol', 'Company', 'Exchange'] + [f'Col{i}' for i in range(3, len(df.columns))]
            elif len(df.columns) == 2:
                df.columns = ['Symbol', 'Company']
            elif len(df.columns) == 1:
                df.columns = ['Symbol']

        return df
    except Exception as e:
        logger.error(f"Error loading stock universe: {e}")
        return pd.DataFrame()


def extract_trends_from_note(note_content: str, ai_provider: str = "anthropic") -> Dict[str, Any]:
    """Extract key trends and themes from an industry research note using AI."""

    prompt = f"""Analyze this industry research note and extract the following in JSON format:

RESEARCH NOTE:
{note_content[:15000]}  # Limit content length

Please extract and return as JSON:
{{
    "industry": "The main industry or sector discussed",
    "key_trends": ["List of 3-5 key industry trends identified"],
    "bullish_factors": ["Factors that would benefit companies"],
    "bearish_factors": ["Factors that would hurt companies"],
    "key_themes": ["Main investment themes"],
    "summary": "2-3 sentence summary of the note"
}}

Return ONLY valid JSON, no other text."""

    try:
        if ai_provider == "anthropic" and anthropic_client:
            response = anthropic_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            response_text = response.content[0].text
        elif ai_provider == "openai" and openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            response_text = response.choices[0].message.content
        else:
            return {"error": "No AI provider available"}

        # Clean up response
        response_text = response_text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]

        return json.loads(response_text.strip())
    except Exception as e:
        logger.error(f"Error extracting trends: {e}")
        return {"error": str(e)}


# ============================================
# MULTI-AGENT ANALYSIS SYSTEM
# ============================================

ANALYSIS_AGENTS = {
    "industry_analyst": {
        "name": "Industry Analyst",
        "prompt": """You are a Senior Industry Analyst. Read this research report and identify:
1. Key industry trends and shifts
2. Which business models benefit from these trends
3. Which business models are threatened
4. Market dynamics and competitive changes

Focus on structural changes that create lasting winners and losers."""
    },
    "competitive_intel": {
        "name": "Competitive Intelligence",
        "prompt": """You are a Competitive Intelligence Analyst. Based on this report, identify:
1. Companies gaining competitive advantage
2. Companies losing market position
3. New entrants or disruptors benefiting
4. Incumbents being disrupted

Focus on competitive positioning changes."""
    },
    "financial_analyst": {
        "name": "Financial Analyst",
        "prompt": """You are a Financial Analyst. Based on this report, identify:
1. Companies with financial strength to capitalize on trends
2. Companies with balance sheet risk from these changes
3. Revenue/margin implications for different players
4. Capital requirements and who can fund growth

Focus on financial capacity to win or lose."""
    },
    "risk_analyst": {
        "name": "Risk Analyst",
        "prompt": """You are a Risk Analyst. Based on this report, identify:
1. Companies most exposed to negative trends
2. Regulatory or legal risks for specific players
3. Technology disruption risks
4. Business model obsolescence risks

Focus on downside risks and vulnerabilities."""
    },
    "investment_strategist": {
        "name": "Investment Strategist",
        "prompt": """You are a Chief Investment Strategist. Synthesize the analysis and provide:
1. Clear WINNERS list with conviction levels
2. Clear LOSERS list with conviction levels
3. Key catalysts to watch
4. Timeframe for thesis to play out

Be decisive - pick clear winners and losers."""
    }
}


def run_agent_analysis(
    agent_key: str,
    report_content: str,
    universe_stocks: str,
    ai_provider: str = "anthropic"
) -> str:
    """Run a single agent's analysis on the report."""
    agent = ANALYSIS_AGENTS[agent_key]

    prompt = f"""{agent['prompt']}

RESEARCH REPORT:
{report_content[:12000]}

STOCK UNIVERSE TO CONSIDER:
{universe_stocks}

Provide your analysis identifying specific stocks from the universe as potential winners or losers."""

    try:
        if ai_provider == "anthropic" and anthropic_client:
            response = anthropic_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        elif ai_provider == "openai" and openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.choices[0].message.content
        else:
            return "AI provider not available"
    except Exception as e:
        logger.error(f"Agent {agent_key} failed: {e}")
        return f"Error: {str(e)}"


def run_all_agents_and_synthesize(
    report_content: str,
    universe_df: pd.DataFrame = None,
    ai_provider: str = "anthropic",
    progress_callback=None
) -> WinnersLosersAnalysis:
    """Run all agents and synthesize into final winners/losers."""

    # Build stock list if universe provided, otherwise analyze report directly
    if universe_df is not None and not universe_df.empty:
        symbol_col = universe_df.columns[0]
        name_col = universe_df.columns[1] if len(universe_df.columns) > 1 else symbol_col
        stocks_list = []
        for _, row in universe_df.head(150).iterrows():
            stocks_list.append(f"- {row[symbol_col]} ({row[name_col]})")
        universe_stocks = "\n".join(stocks_list)
    else:
        universe_stocks = "(Identify winners/losers directly from the companies mentioned in the report)"

    # Run each agent
    agent_results = {}
    agents = list(ANALYSIS_AGENTS.keys())

    for i, agent_key in enumerate(agents):
        if progress_callback:
            progress_callback(f"Running {ANALYSIS_AGENTS[agent_key]['name']}...", (i + 1) / (len(agents) + 1))
        agent_results[agent_key] = run_agent_analysis(agent_key, report_content, universe_stocks, ai_provider)

    # Final synthesis
    if progress_callback:
        progress_callback("Synthesizing final winners/losers...", 0.95)

    synthesis_prompt = f"""Based on the following multi-agent analysis of the research report, create the final WINNERS and LOSERS list.

INDUSTRY ANALYST VIEW:
{agent_results.get('industry_analyst', 'N/A')}

COMPETITIVE INTELLIGENCE VIEW:
{agent_results.get('competitive_intel', 'N/A')}

FINANCIAL ANALYST VIEW:
{agent_results.get('financial_analyst', 'N/A')}

RISK ANALYST VIEW:
{agent_results.get('risk_analyst', 'N/A')}

INVESTMENT STRATEGIST VIEW:
{agent_results.get('investment_strategist', 'N/A')}

Now synthesize into a final JSON output:
{{
    "summary": "2-3 sentence synthesis of the consensus view",
    "winners": [
        {{"symbol": "TICKER", "company_name": "Name", "trend": "Key trend", "rationale": "Why they win", "confidence": "High/Medium/Low"}}
    ],
    "losers": [
        {{"symbol": "TICKER", "company_name": "Name", "trend": "Key trend", "rationale": "Why they lose", "confidence": "High/Medium/Low"}}
    ]
}}

Include stocks where multiple agents agree. Return ONLY valid JSON."""

    try:
        if ai_provider == "anthropic" and anthropic_client:
            response = anthropic_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=3000,
                messages=[{"role": "user", "content": synthesis_prompt}]
            )
            response_text = response.content[0].text
        elif ai_provider == "openai" and openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=3000,
                messages=[{"role": "user", "content": synthesis_prompt}]
            )
            response_text = response.choices[0].message.content
        else:
            return WinnersLosersAnalysis(summary="No AI provider available")

        # Parse JSON
        response_text = response_text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]

        data = json.loads(response_text.strip())

        winners = [
            CompanyTrendPosition(
                symbol=w.get('symbol', ''),
                company_name=w.get('company_name', ''),
                position='winner',
                trend=w.get('trend', ''),
                rationale=w.get('rationale', ''),
                confidence=w.get('confidence', 'Medium')
            )
            for w in data.get('winners', [])
        ]

        losers = [
            CompanyTrendPosition(
                symbol=l.get('symbol', ''),
                company_name=l.get('company_name', ''),
                position='loser',
                trend=l.get('trend', ''),
                rationale=l.get('rationale', ''),
                confidence=l.get('confidence', 'Medium')
            )
            for l in data.get('losers', [])
        ]

        return WinnersLosersAnalysis(
            winners=winners,
            losers=losers,
            summary=data.get('summary', '')
        )

    except Exception as e:
        logger.error(f"Synthesis failed: {e}")
        return WinnersLosersAnalysis(summary=f"Synthesis error: {str(e)}")


def scan_universe_for_winners_losers(
    universe_df: pd.DataFrame,
    trends_data: Dict[str, Any],
    ai_provider: str = "anthropic"
) -> WinnersLosersAnalysis:
    """Scan stock universe to identify winners and losers based on extracted trends."""

    # Get relevant columns from universe
    symbol_col = None
    name_col = None
    sector_col = None

    for col in universe_df.columns:
        col_lower = str(col).lower()
        if 'symbol' in col_lower or 'ticker' in col_lower or col == 'Symbol':
            symbol_col = col
        elif 'name' in col_lower or 'company' in col_lower or col == 'Company':
            name_col = col
        elif 'sector' in col_lower:
            sector_col = col

    # Fallback: use first column as symbol if not found
    if not symbol_col and len(universe_df.columns) > 0:
        symbol_col = universe_df.columns[0]
        logger.info(f"Using first column '{symbol_col}' as symbol column")

    # Use second column as name if not found
    if not name_col and len(universe_df.columns) > 1:
        name_col = universe_df.columns[1]

    if not symbol_col:
        return WinnersLosersAnalysis(summary="Could not find symbol column in universe file")

    # Build stock list for analysis
    stocks_info = []
    for _, row in universe_df.head(100).iterrows():  # Limit to 100 stocks
        info = f"- {row[symbol_col]}"
        if name_col and pd.notna(row.get(name_col)):
            info += f" ({row[name_col]})"
        if sector_col and pd.notna(row.get(sector_col)):
            info += f" [Sector: {row[sector_col]}]"
        stocks_info.append(info)

    prompt = f"""Based on the following industry trends and themes, analyze the stock universe and identify WINNERS and LOSERS.

INDUSTRY TRENDS & THEMES:
- Industry: {trends_data.get('industry', 'N/A')}
- Key Trends: {', '.join(trends_data.get('key_trends', []))}
- Bullish Factors: {', '.join(trends_data.get('bullish_factors', []))}
- Bearish Factors: {', '.join(trends_data.get('bearish_factors', []))}
- Key Themes: {', '.join(trends_data.get('key_themes', []))}

STOCK UNIVERSE TO ANALYZE:
{chr(10).join(stocks_info)}

Analyze each stock against the trends and categorize as WINNER, LOSER, or skip if not relevant.

Return as JSON:
{{
    "summary": "Brief 2-3 sentence summary of winners/losers dynamics",
    "winners": [
        {{
            "symbol": "TICKER",
            "company_name": "Company Name",
            "trend": "The specific trend they benefit from",
            "rationale": "Why they win (2-3 sentences)",
            "confidence": "High/Medium/Low"
        }}
    ],
    "losers": [
        {{
            "symbol": "TICKER",
            "company_name": "Company Name",
            "trend": "The specific trend hurting them",
            "rationale": "Why they lose (2-3 sentences)",
            "confidence": "High/Medium/Low"
        }}
    ]
}}

Focus on stocks that are CLEARLY positioned as winners or losers. Skip neutral stocks.
Return ONLY valid JSON."""

    try:
        if ai_provider == "anthropic" and anthropic_client:
            response = anthropic_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=3000,
                messages=[{"role": "user", "content": prompt}]
            )
            response_text = response.content[0].text
        elif ai_provider == "openai" and openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=3000,
                messages=[{"role": "user", "content": prompt}]
            )
            response_text = response.choices[0].message.content
        else:
            return WinnersLosersAnalysis(summary="No AI provider available")

        # Clean up response
        response_text = response_text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]

        data = json.loads(response_text.strip())

        winners = [
            CompanyTrendPosition(
                symbol=w.get('symbol', ''),
                company_name=w.get('company_name', ''),
                position='winner',
                trend=w.get('trend', ''),
                rationale=w.get('rationale', ''),
                confidence=w.get('confidence', 'Medium')
            )
            for w in data.get('winners', [])
        ]

        losers = [
            CompanyTrendPosition(
                symbol=l.get('symbol', ''),
                company_name=l.get('company_name', ''),
                position='loser',
                trend=l.get('trend', ''),
                rationale=l.get('rationale', ''),
                confidence=l.get('confidence', 'Medium')
            )
            for l in data.get('losers', [])
        ]

        return WinnersLosersAnalysis(
            winners=winners,
            losers=losers,
            summary=data.get('summary', '')
        )

    except Exception as e:
        logger.error(f"Error scanning universe: {e}")
        return WinnersLosersAnalysis(summary=f"Error: {str(e)}")

# Page configuration
st.set_page_config(
    page_title="Industry Report Generator",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main .block-container {
        border: 1.5px solid black;
        padding: 30px;
        border-radius: 10px;
        background-color: white;
        max-width: 1400px;
        margin: auto;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    .winner-card {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
    }
    .loser-card {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
    }
    .neutral-card {
        background-color: #fff3cd;
        border: 1px solid #ffeeba;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
    }
    .section-header {
        background-color: #2E86AB;
        color: white;
        padding: 10px 15px;
        border-radius: 5px;
        margin: 20px 0 10px 0;
    }
    div[data-testid="stExpander"] {
        border: 1px solid #ddd;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# Available sectors (from FMP)
SECTORS = [
    "Technology",
    "Healthcare",
    "Financial Services",
    "Consumer Cyclical",
    "Communication Services",
    "Industrials",
    "Consumer Defensive",
    "Energy",
    "Basic Materials",
    "Real Estate",
    "Utilities"
]

# Common industries by sector
INDUSTRIES_BY_SECTOR = {
    "Technology": [
        "Software - Application",
        "Software - Infrastructure",
        "Semiconductors",
        "Information Technology Services",
        "Computer Hardware",
        "Electronic Components",
        "Scientific & Technical Instruments",
        "Communication Equipment"
    ],
    "Healthcare": [
        "Biotechnology",
        "Drug Manufacturers - General",
        "Medical Devices",
        "Healthcare Plans",
        "Medical Instruments & Supplies",
        "Diagnostics & Research",
        "Medical Care Facilities"
    ],
    "Financial Services": [
        "Banks - Diversified",
        "Banks - Regional",
        "Asset Management",
        "Insurance - Diversified",
        "Capital Markets",
        "Credit Services",
        "Financial Data & Stock Exchanges"
    ],
    "Consumer Cyclical": [
        "Internet Retail",
        "Auto Manufacturers",
        "Restaurants",
        "Home Improvement Retail",
        "Apparel Retail",
        "Specialty Retail",
        "Travel Services"
    ],
    "Communication Services": [
        "Internet Content & Information",
        "Entertainment",
        "Telecom Services",
        "Electronic Gaming & Multimedia",
        "Advertising Agencies"
    ],
    "Industrials": [
        "Aerospace & Defense",
        "Railroads",
        "Industrial Distribution",
        "Farm & Heavy Construction Machinery",
        "Specialty Industrial Machinery",
        "Consulting Services"
    ],
    "Energy": [
        "Oil & Gas Integrated",
        "Oil & Gas E&P",
        "Oil & Gas Midstream",
        "Oil & Gas Refining & Marketing",
        "Uranium"
    ],
    "Consumer Defensive": [
        "Household & Personal Products",
        "Beverages - Non-Alcoholic",
        "Packaged Foods",
        "Discount Stores",
        "Grocery Stores"
    ],
    "Basic Materials": [
        "Specialty Chemicals",
        "Gold",
        "Copper",
        "Steel",
        "Lumber & Wood Production"
    ],
    "Real Estate": [
        "REIT - Residential",
        "REIT - Industrial",
        "REIT - Retail",
        "REIT - Office",
        "Real Estate Services"
    ],
    "Utilities": [
        "Utilities - Regulated Electric",
        "Utilities - Renewable",
        "Utilities - Diversified",
        "Utilities - Independent Power Producers"
    ]
}


def display_logo():
    """Display company logo at the top, centered."""
    logo_path = "company_logo.png"
    if os.path.exists(logo_path):
        st.markdown(
            """
            <div style="display: flex; justify-content: center; align-items: center; flex-direction: column; margin-bottom: 10px;">
            """,
            unsafe_allow_html=True
        )
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image(logo_path, use_container_width=True)
        st.markdown(
            """
            <p style='text-align: center; color: #666; font-style: italic; margin-top: 5px;'>
                Precision Analysis for Informed Investment Decisions
            </p>
            </div>
            """,
            unsafe_allow_html=True
        )


def display_winners_losers(winners_losers: WinnersLosersAnalysis):
    """Display winners and losers in styled cards."""
    if not winners_losers:
        st.info("No winners/losers analysis available.")
        return

    # Summary
    if winners_losers.summary:
        st.markdown(f"**Summary:** {winners_losers.summary}")
        st.markdown("---")

    col1, col2 = st.columns(2)

    # Winners
    with col1:
        st.markdown("### 🏆 Winners")
        if winners_losers.winners:
            for w in winners_losers.winners:
                confidence_color = {"High": "🟢", "Medium": "🟡", "Low": "🔴"}.get(w.confidence, "⚪")
                st.markdown(f"""
                <div class="winner-card">
                    <h4 style="color: #155724; margin: 0;">{w.symbol} - {w.company_name}</h4>
                    <p style="margin: 5px 0;"><strong>Trend:</strong> {w.trend}</p>
                    <p style="margin: 5px 0; font-size: 0.9em;">{w.rationale}</p>
                    <p style="margin: 0; font-size: 0.85em;"><strong>Confidence:</strong> {confidence_color} {w.confidence}</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No clear winners identified.")

    # Losers
    with col2:
        st.markdown("### 📉 Losers")
        if winners_losers.losers:
            for l in winners_losers.losers:
                confidence_color = {"High": "🟢", "Medium": "🟡", "Low": "🔴"}.get(l.confidence, "⚪")
                st.markdown(f"""
                <div class="loser-card">
                    <h4 style="color: #721c24; margin: 0;">{l.symbol} - {l.company_name}</h4>
                    <p style="margin: 5px 0;"><strong>Trend:</strong> {l.trend}</p>
                    <p style="margin: 5px 0; font-size: 0.9em;">{l.rationale}</p>
                    <p style="margin: 0; font-size: 0.85em;"><strong>Confidence:</strong> {confidence_color} {l.confidence}</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No clear losers identified.")

    # Neutral (if any)
    if winners_losers.neutral:
        st.markdown("### ⚖️ Neutral / Mixed Positioning")
        for n in winners_losers.neutral:
            st.markdown(f"""
            <div class="neutral-card">
                <strong>{n.symbol}</strong> ({n.company_name}): {n.rationale}
            </div>
            """, unsafe_allow_html=True)


def main():
    # Display logo
    display_logo()

    # Title
    st.markdown("<h1 style='text-align: center;'>🏭 Industry Report Generator</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #666;'>Generate comprehensive industry analysis with AI-powered insights and trend-based winners/losers identification</p>", unsafe_allow_html=True)

    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Report Configuration")

        # Mode selection - Upload existing report OR generate new
        report_mode = st.radio(
            "Mode:",
            ["📄 Analyze Existing Report", "🔍 Generate New Report"],
            help="Upload a completed report to scan for winners/losers, or generate a new industry report"
        )

        st.markdown("---")

        # AI Provider (common to both modes)
        ai_provider = st.selectbox(
            "AI Provider",
            ["anthropic", "openai"],
            index=0,
            help="Select AI provider for analysis"
        )

        # Variables for new report mode
        selected_sector = None
        selected_industry = None
        company_limit = 20
        analyst_notes_text = ""
        key_themes_text = ""
        investment_thesis = ""
        articles_data = []

        if report_mode == "📄 Analyze Existing Report":
            # ============================================
            # MODE 1: UPLOAD EXISTING REPORT
            # ============================================
            st.markdown("---")
            st.subheader("📄 Upload Industry Report")
            st.markdown("*Upload your completed report to identify winners & losers*")

            # Custom industry/theme name
            custom_industry_name = st.text_input(
                "Industry/Theme Name",
                placeholder="e.g., Tokenization, AI Infrastructure, Clean Energy...",
                help="Enter any industry or investment theme - not limited to standard sectors"
            )

            # Upload report
            uploaded_industry_note = st.file_uploader(
                "Upload Report",
                type=["docx", "pdf", "txt"],
                help="Upload Word, PDF, or text file",
                key="industry_note_upload"
            )

            st.markdown("---")

            # Scan button
            scan_universe_button = st.button(
                "🔍 Scan for Winners & Losers",
                type="primary",
                use_container_width=True,
                disabled=(uploaded_industry_note is None or not custom_industry_name)
            )

            if uploaded_industry_note is None:
                st.caption("⬆️ Upload a report to enable scanning")
            elif not custom_industry_name:
                st.caption("⬆️ Enter an industry/theme name")

            # Set generate_button to False for this mode
            generate_button = False
            uploaded_notes = None

        else:
            # ============================================
            # MODE 2: GENERATE NEW REPORT
            # ============================================
            st.markdown("---")
            st.subheader("🔍 Industry Selection")

            # Selection mode
            selection_mode = st.radio(
                "Select by:",
                ["Sector", "Industry", "Custom Theme"],
                help="Choose standard sector/industry or enter custom theme"
            )

            if selection_mode == "Sector":
                selected_sector = st.selectbox(
                    "Select Sector",
                    SECTORS,
                    index=0
                )
            elif selection_mode == "Industry":
                sector_for_industry = st.selectbox(
                    "Filter by Sector",
                    SECTORS,
                    index=0
                )
                industries = INDUSTRIES_BY_SECTOR.get(sector_for_industry, [])
                if industries:
                    selected_industry = st.selectbox(
                        "Select Industry",
                        industries
                    )
                else:
                    selected_industry = st.text_input("Custom Industry Name")
            else:
                # Custom theme
                selected_industry = st.text_input(
                    "Custom Theme/Industry",
                    placeholder="e.g., Tokenization, Space Economy, Nuclear Energy..."
                )

            st.markdown("---")

            # Analysis options
            st.subheader("📊 Analysis Options")
            company_limit = st.slider(
                "Max Companies",
                min_value=5,
                max_value=50,
                value=20
            )

            st.markdown("---")

            # Research notes section
            st.subheader("📝 Research Notes (Optional)")

            with st.expander("Add Analyst Notes"):
                analyst_notes_text = st.text_area(
                    "Analyst Notes (one per line)",
                    height=100,
                    placeholder="Enter analyst observations..."
                )

            with st.expander("Add Key Themes"):
                key_themes_text = st.text_area(
                    "Key Investment Themes (one per line)",
                    height=80,
                    placeholder="AI infrastructure buildout..."
                )

            with st.expander("Add Investment Thesis"):
                investment_thesis = st.text_area(
                    "Investment Thesis",
                    height=100,
                    placeholder="Overall investment thesis..."
                )

            with st.expander("Add Articles"):
                st.markdown("**Add research articles:**")
                num_articles = st.number_input("Number of articles", min_value=0, max_value=5, value=0)
                articles_data = []
                for i in range(int(num_articles)):
                    st.markdown(f"**Article {i+1}**")
                    title = st.text_input(f"Title {i+1}", key=f"art_title_{i}")
                    source = st.text_input(f"Source {i+1}", key=f"art_source_{i}")
                    date = st.text_input(f"Date {i+1}", key=f"art_date_{i}", placeholder="YYYY-MM-DD")
                    content = st.text_area(f"Summary {i+1}", key=f"art_content_{i}", height=60)
                    url = st.text_input(f"URL {i+1}", key=f"art_url_{i}")
                    if title:
                        articles_data.append({
                            "title": title,
                            "source": source,
                            "date": date,
                            "content": content,
                            "url": url
                        })

            # Upload JSON notes file
            st.markdown("---")
            uploaded_notes = st.file_uploader(
                "Or upload notes JSON file",
                type=["json"],
                help="Upload a JSON file with analyst notes and articles"
            )

            st.markdown("---")

            # Generate button for new report mode
            generate_button = st.button(
                "🚀 Generate Report",
                type="primary",
                use_container_width=True
            )

            # Initialize variables not used in this mode
            scan_universe_button = False
            uploaded_industry_note = None
            custom_industry_name = None

    # Main content area

    # Handle Scan Universe button (Analyze Existing Report mode)
    if scan_universe_button and uploaded_industry_note:
        try:
            # Read the uploaded industry note
            progress_bar = st.progress(0)
            status_text = st.empty()
            status_text.text("Reading industry report...")

            # Store original file bytes for Word export (preserve formatting)
            uploaded_industry_note.seek(0)
            original_file_bytes = uploaded_industry_note.read()
            uploaded_industry_note.seek(0)
            original_filename = uploaded_industry_note.name

            note_content = read_uploaded_document(uploaded_industry_note)
            progress_bar.progress(10)

            if not note_content:
                st.error("Could not read the uploaded document. Please try a different format.")
            else:
                st.success(f"Read {len(note_content):,} characters from report")

                # Run multi-agent analysis (no stock universe needed)
                def update_progress(msg, pct):
                    status_text.text(msg)
                    progress_bar.progress(int(15 + pct * 85))

                status_text.text("Running multi-agent analysis...")
                st.info("🤖 **5 AI Agents analyzing your report:** Industry Analyst → Competitive Intel → Financial Analyst → Risk Analyst → Investment Strategist")

                winners_losers_result = run_all_agents_and_synthesize(
                    note_content,
                    None,  # No universe - analyze report directly
                    ai_provider,
                    progress_callback=update_progress
                )
                progress_bar.progress(100)

                # Extract trends for display (quick extraction)
                trends_data = extract_trends_from_note(note_content, ai_provider)

                # Store in session state (including original file for Word export)
                st.session_state['universe_scan_results'] = {
                    'industry_name': custom_industry_name,
                    'note_content': note_content,
                    'trends_data': trends_data if "error" not in trends_data else {},
                    'winners_losers': winners_losers_result,
                    'original_file_bytes': original_file_bytes,
                    'original_filename': original_filename
                }

                status_text.text("")
                st.success(f"✅ Multi-agent analysis complete! Found {len(winners_losers_result.winners)} winners and {len(winners_losers_result.losers)} losers.")

        except Exception as e:
            st.error(f"Error during analysis: {str(e)}")
            logger.exception("Universe scan failed")

    # Display universe scan results if available
    if 'universe_scan_results' in st.session_state:
        scan_results = st.session_state['universe_scan_results']
        trends_data = scan_results.get('trends_data', {})
        industry_name = scan_results.get('industry_name', 'Industry')

        st.markdown("---")
        st.markdown(f"<h2 style='text-align: center;'>📊 {industry_name} Analysis</h2>", unsafe_allow_html=True)

        # Display extracted trends
        st.markdown("<div class='section-header'><h3 style='margin:0; color: white;'>📈 Extracted Trends & Themes</h3></div>", unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Industry:** {trends_data.get('industry', industry_name)}")
            st.markdown("**Key Trends:**")
            for trend in trends_data.get('key_trends', []):
                st.markdown(f"• {trend}")

            if trends_data.get('key_themes'):
                st.markdown("**Key Themes:**")
                for theme in trends_data.get('key_themes', []):
                    st.markdown(f"• {theme}")

        with col2:
            st.markdown("**Bullish Factors:**")
            for factor in trends_data.get('bullish_factors', []):
                st.markdown(f"🟢 {factor}")

            st.markdown("**Bearish Factors:**")
            for factor in trends_data.get('bearish_factors', []):
                st.markdown(f"🔴 {factor}")

        if trends_data.get('summary'):
            st.info(f"**Summary:** {trends_data['summary']}")

        # Winners & Losers section
        st.markdown("<div class='section-header'><h3 style='margin:0; color: white;'>🎯 Winners & Losers</h3></div>", unsafe_allow_html=True)
        display_winners_losers(scan_results['winners_losers'])

        # Export options
        st.markdown("---")
        st.markdown("### 📥 Export Results")

        col1, col2, col3 = st.columns(3)

        # Export as text
        with col1:
            wl = scan_results['winners_losers']
            export_text = f"# {industry_name} - Winners & Losers Analysis\n"
            export_text += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
            export_text += f"## Summary\n{wl.summary}\n\n"
            export_text += "## WINNERS\n"
            for w in wl.winners:
                export_text += f"- {w.symbol} ({w.company_name})\n"
                export_text += f"  Trend: {w.trend}\n"
                export_text += f"  Rationale: {w.rationale}\n"
                export_text += f"  Confidence: {w.confidence}\n\n"
            export_text += "## LOSERS\n"
            for l in wl.losers:
                export_text += f"- {l.symbol} ({l.company_name})\n"
                export_text += f"  Trend: {l.trend}\n"
                export_text += f"  Rationale: {l.rationale}\n"
                export_text += f"  Confidence: {l.confidence}\n\n"

            st.download_button(
                "📄 Download as Text",
                data=export_text,
                file_name=f"{industry_name.replace(' ', '_')}_winners_losers.txt",
                mime="text/plain"
            )

        # Export as JSON
        with col2:
            export_json = {
                "industry": industry_name,
                "generated": datetime.now().isoformat(),
                "trends": trends_data,
                "summary": wl.summary,
                "winners": [{"symbol": w.symbol, "company": w.company_name, "trend": w.trend, "rationale": w.rationale, "confidence": w.confidence} for w in wl.winners],
                "losers": [{"symbol": l.symbol, "company": l.company_name, "trend": l.trend, "rationale": l.rationale, "confidence": l.confidence} for l in wl.losers]
            }
            st.download_button(
                "📋 Download as JSON",
                data=json.dumps(export_json, indent=2),
                file_name=f"{industry_name.replace(' ', '_')}_winners_losers.json",
                mime="application/json"
            )

        # Export as CSV
        with col3:
            rows = []
            for w in wl.winners:
                rows.append({"Type": "Winner", "Symbol": w.symbol, "Company": w.company_name, "Trend": w.trend, "Rationale": w.rationale, "Confidence": w.confidence})
            for l in wl.losers:
                rows.append({"Type": "Loser", "Symbol": l.symbol, "Company": l.company_name, "Trend": l.trend, "Rationale": l.rationale, "Confidence": l.confidence})
            export_df = pd.DataFrame(rows)
            st.download_button(
                "📊 Download as CSV",
                data=export_df.to_csv(index=False),
                file_name=f"{industry_name.replace(' ', '_')}_winners_losers.csv",
                mime="text/csv"
            )

        # PDF and Email row
        st.markdown("---")
        st.markdown("### 📑 Professional Report")

        # Check if original was a Word document
        original_filename = scan_results.get('original_filename', '')
        is_word_doc = original_filename.lower().endswith('.docx')

        if is_word_doc:
            st.info("📝 **Original document formatting preserved** - Download as Word to keep all bullet points, indentation, and formatting from your uploaded document.")

        col1, col2, col3 = st.columns(3)

        with col1:
            # Generate Word doc with original formatting preserved
            if is_word_doc and scan_results.get('original_file_bytes'):
                original_buffer = BytesIO(scan_results['original_file_bytes'])
                word_buffer = generate_winners_losers_word(
                    industry_name,
                    wl,
                    original_file_buffer=original_buffer
                )
                st.download_button(
                    "📥 Download Word (Preserves Formatting)",
                    data=word_buffer,
                    file_name=f"{industry_name.replace(' ', '_')}_Winners_Losers_{datetime.now().strftime('%Y%m%d')}.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    type="primary"
                )
            else:
                # Generate PDF if not Word doc
                pdf_buffer = generate_winners_losers_pdf(
                    industry_name,
                    trends_data,
                    wl,
                    scan_results.get('note_content')
                )
                st.download_button(
                    "📥 Download PDF Report",
                    data=pdf_buffer,
                    file_name=f"{industry_name.replace(' ', '_')}_Winners_Losers_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    type="primary"
                )

        with col2:
            # Always offer PDF as secondary option (Winners/Losers only)
            pdf_buffer = generate_winners_losers_pdf(
                industry_name,
                trends_data,
                wl,
                scan_results.get('note_content')
            )
            st.download_button(
                "📄 Download PDF (Winners/Losers Only)",
                data=pdf_buffer,
                file_name=f"{industry_name.replace(' ', '_')}_WL_Summary_{datetime.now().strftime('%Y%m%d')}.pdf",
                mime="application/pdf"
            )

        with col3:
            if st.button("📧 Email Report to Me", type="primary", key="email_report"):
                with st.spinner("Sending email..."):
                    # Send Word doc if original was Word, otherwise PDF
                    if is_word_doc and scan_results.get('original_file_bytes'):
                        original_buffer = BytesIO(scan_results['original_file_bytes'])
                        word_buffer = generate_winners_losers_word(
                            industry_name,
                            wl,
                            original_file_buffer=original_buffer
                        )
                        success, message = send_email_with_attachment(word_buffer, industry_name, "docx")
                    else:
                        pdf_for_email = generate_winners_losers_pdf(
                            industry_name,
                            trends_data,
                            wl,
                            scan_results.get('note_content')
                        )
                        success, message = send_email_with_attachment(pdf_for_email, industry_name, "pdf")
                    if success:
                        st.success(message)
                    else:
                        st.error(message)

        # Clear results button
        st.markdown("---")
        if st.button("🗑️ Clear Results", key="clear_scan_results"):
            del st.session_state['universe_scan_results']
            st.rerun()

    if generate_button:
        target = selected_industry if selected_industry else selected_sector

        if not target:
            st.error("Please select a sector or industry.")
            return

        # Build research notes
        research_notes = None
        if uploaded_notes:
            try:
                notes_data = json.load(uploaded_notes)
                articles = []
                for a in notes_data.get('articles', []):
                    articles.append(Article(
                        title=a.get('title', ''),
                        source=a.get('source', ''),
                        date=a.get('date', ''),
                        content=a.get('content', ''),
                        url=a.get('url', '')
                    ))
                research_notes = ResearchNotes(
                    analyst_notes=notes_data.get('analyst_notes', []),
                    key_themes=notes_data.get('key_themes', []),
                    investment_thesis=notes_data.get('investment_thesis', ''),
                    articles=articles
                )
                st.success("Loaded research notes from uploaded file.")
            except Exception as e:
                st.error(f"Error loading notes file: {e}")
        else:
            # Build from form inputs
            analyst_notes = [n.strip() for n in analyst_notes_text.split('\n') if n.strip()] if analyst_notes_text else []
            key_themes = [t.strip() for t in key_themes_text.split('\n') if t.strip()] if key_themes_text else []
            articles = [Article(**a) for a in articles_data] if articles_data else []

            if analyst_notes or key_themes or investment_thesis or articles:
                research_notes = ResearchNotes(
                    analyst_notes=analyst_notes,
                    key_themes=key_themes,
                    investment_thesis=investment_thesis,
                    articles=articles
                )

        with st.spinner(f"Generating industry report for {target}..."):
            try:
                # Fetch companies
                progress = st.progress(0)
                st.text("Fetching companies...")

                if selected_industry:
                    companies = get_stocks_by_industry(selected_industry, limit=company_limit)
                else:
                    companies = get_stocks_by_sector(selected_sector, limit=company_limit)

                if not companies:
                    st.error(f"No companies found for {target}")
                    return

                # Sort by market cap
                companies = sorted(companies, key=lambda x: x.get('marketCap', 0), reverse=True)
                progress.progress(20)

                # Get sector data
                st.text("Fetching sector metrics...")
                sector_pe_data = get_sector_pe_ratios()
                sector_data = {}
                if sector_pe_data:
                    for item in sector_pe_data:
                        if selected_sector and item.get('sector', '').lower() == selected_sector.lower():
                            sector_data = item
                            break
                    if not sector_data and companies:
                        sector_data = {'sector': companies[0].get('sector', 'N/A')}
                progress.progress(40)

                # Generate AI analysis
                st.text("Generating AI analysis...")
                ai_analysis = generate_industry_analysis(
                    target,
                    companies,
                    sector_data,
                    ai_provider=ai_provider
                )
                progress.progress(60)

                # Identify winners and losers
                st.text("Identifying winners and losers from trends...")
                winners_losers = None
                if ai_analysis.get('trends'):
                    winners_losers = identify_winners_losers(
                        target,
                        companies,
                        ai_analysis['trends'],
                        ai_provider=ai_provider
                    )

                progress.progress(80)

                # Generate PDF
                st.text("Generating PDF report...")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = target.replace(" ", "_").replace("/", "_")[:30]
                output_path = f"output/{safe_name}_Industry_Report_{timestamp}.pdf"

                pdf_path = generate_industry_pdf(
                    target,
                    companies,
                    sector_data,
                    ai_analysis,
                    output_path=output_path,
                    research_notes=research_notes,
                    winners_losers=winners_losers
                )
                progress.progress(100)

                # Store in session state
                st.session_state['report_data'] = {
                    'target': target,
                    'companies': companies,
                    'sector_data': sector_data,
                    'ai_analysis': ai_analysis,
                    'winners_losers': winners_losers,
                    'pdf_path': pdf_path
                }

                st.success(f"Report generated successfully!")

            except Exception as e:
                st.error(f"Error generating report: {str(e)}")
                logger.exception("Report generation failed")
                return

    # Display results if available
    if 'report_data' in st.session_state:
        data = st.session_state['report_data']
        target = data['target']
        companies = data['companies']
        sector_data = data['sector_data']
        ai_analysis = data['ai_analysis']
        winners_losers = data['winners_losers']
        pdf_path = data['pdf_path']

        # Download button
        if os.path.exists(pdf_path):
            with open(pdf_path, 'rb') as f:
                st.download_button(
                    label="📥 Download PDF Report",
                    data=f.read(),
                    file_name=os.path.basename(pdf_path),
                    mime="application/pdf",
                    type="primary"
                )

        st.markdown("---")

        # Report header
        st.markdown(f"<h2 style='text-align: center;'>Industry Report: {target}</h2>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center; color: #666;'>Generated: {datetime.now().strftime('%B %d, %Y')}</p>", unsafe_allow_html=True)

        # Key metrics
        st.markdown("<div class='section-header'><h3 style='margin:0; color: white;'>📊 Key Metrics</h3></div>", unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Companies Analyzed", len(companies))
        with col2:
            total_mcap = sum(c.get('marketCap', 0) for c in companies)
            st.metric("Total Market Cap", format_currency(total_mcap))
        with col3:
            avg_mcap = total_mcap / len(companies) if companies else 0
            st.metric("Avg Market Cap", format_currency(avg_mcap))
        with col4:
            st.metric("Sector P/E", f"{sector_data.get('pe', 'N/A')}")

        # Industry Overview
        st.markdown("<div class='section-header'><h3 style='margin:0; color: white;'>📋 Industry Overview</h3></div>", unsafe_allow_html=True)
        if ai_analysis.get('overview'):
            st.markdown(ai_analysis['overview'])

        # Top Companies Table
        st.markdown("<div class='section-header'><h3 style='margin:0; color: white;'>🏢 Top Companies</h3></div>", unsafe_allow_html=True)

        company_df = pd.DataFrame([
            {
                'Rank': i + 1,
                'Symbol': c.get('symbol', 'N/A'),
                'Company': c.get('companyName', 'N/A')[:40],
                'Market Cap': format_currency(c.get('marketCap', 0)),
                'Price': f"${c.get('price', 0):.2f}",
                'Beta': f"{c.get('beta', 0):.2f}" if c.get('beta') else "N/A"
            }
            for i, c in enumerate(companies[:15])
        ])
        st.dataframe(company_df, use_container_width=True, hide_index=True)

        # Key Trends
        st.markdown("<div class='section-header'><h3 style='margin:0; color: white;'>📈 Key Industry Trends</h3></div>", unsafe_allow_html=True)
        if ai_analysis.get('trends'):
            st.markdown(ai_analysis['trends'])

        # Winners & Losers
        st.markdown("<div class='section-header'><h3 style='margin:0; color: white;'>🎯 Winners & Losers from Trends</h3></div>", unsafe_allow_html=True)
        display_winners_losers(winners_losers)

        # Industry Risks
        st.markdown("<div class='section-header'><h3 style='margin:0; color: white;'>⚠️ Industry Risks</h3></div>", unsafe_allow_html=True)
        if ai_analysis.get('risks'):
            st.markdown(ai_analysis['risks'])

        # 12-Month Outlook
        st.markdown("<div class='section-header'><h3 style='margin:0; color: white;'>🔮 12-Month Outlook</h3></div>", unsafe_allow_html=True)
        if ai_analysis.get('outlook'):
            st.markdown(ai_analysis['outlook'])

        # Footer
        st.markdown("---")
        st.markdown(f"<p style='text-align: center; color: #999; font-size: 0.8em;'>Report generated by Industry Report Generator | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
