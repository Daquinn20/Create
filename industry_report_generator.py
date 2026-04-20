"""
Industry Report Generator
Generates comprehensive industry/sector analysis reports with AI-powered insights
Compares companies within a sector and provides industry-level metrics and trends
"""
import os
import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
import anthropic
from openai import OpenAI
from dotenv import load_dotenv
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph,
    Spacer, Image, PageBreak, KeepTogether
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.lib.colors import HexColor
import io
import json
from dataclasses import dataclass, field

load_dotenv()

# Default logo path
DEFAULT_LOGO_PATH = "company_logo.png"


@dataclass
class Article:
    """Represents an article or research note to include in the report."""
    title: str
    source: str = ""
    date: str = ""
    content: str = ""
    url: str = ""


@dataclass
class ResearchFile:
    """Represents an uploaded research file with extracted content and AI summary."""
    filename: str
    file_type: str  # "pdf", "excel", "word"
    content: str  # Extracted text content
    summary: str = ""  # AI-generated summary


@dataclass
class ResearchNotes:
    """Container for analyst notes and articles to include in the report."""
    analyst_notes: List[str] = field(default_factory=list)
    articles: List[Article] = field(default_factory=list)
    key_themes: List[str] = field(default_factory=list)
    investment_thesis: str = ""
    research_files: List[ResearchFile] = field(default_factory=list)


@dataclass
class CompanyTrendPosition:
    """Represents a company's position relative to industry trends."""
    symbol: str
    company_name: str
    position: str  # "winner" or "loser"
    trend: str  # The trend this relates to
    rationale: str  # Why they're positioned this way
    confidence: str = "Medium"  # High, Medium, Low


@dataclass
class WinnersLosersAnalysis:
    """Container for winners and losers analysis."""
    winners: List[CompanyTrendPosition] = field(default_factory=list)
    losers: List[CompanyTrendPosition] = field(default_factory=list)
    neutral: List[CompanyTrendPosition] = field(default_factory=list)
    summary: str = ""

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# API Configuration
FMP_API_KEY = os.getenv("FMP_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/api/v3"
FMP_V4_BASE = "https://financialmodelingprep.com/api/v4"

# Initialize AI clients
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


# ============================================
# FMP API FUNCTIONS
# ============================================

def _fmp_get(path: str, params: Dict[str, Any] = None, base_url: str = FMP_BASE) -> Any:
    """Make a GET request to the FMP API."""
    if not FMP_API_KEY:
        raise ValueError("Missing FMP_API_KEY in environment")

    params = params or {}
    params["apikey"] = FMP_API_KEY
    url = f"{base_url}/{path}"

    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            logger.error(f"FMP API error {resp.status_code}: {resp.text[:200]}")
            return None
        return resp.json()
    except Exception as e:
        logger.error(f"FMP API request failed: {e}")
        return None


def get_sector_performance() -> List[Dict]:
    """Get performance data for all sectors."""
    data = _fmp_get("sector-performance")
    return data if data else []


def get_industry_pe_ratios() -> List[Dict]:
    """Get P/E ratios by industry."""
    data = _fmp_get("industry_price_earning_ratio", params={"date": datetime.now().strftime("%Y-%m-%d"), "exchange": "NYSE"}, base_url=FMP_V4_BASE)
    return data if data else []


def get_sector_pe_ratios() -> List[Dict]:
    """Get P/E ratios by sector."""
    data = _fmp_get("sector_price_earning_ratio", params={"date": datetime.now().strftime("%Y-%m-%d"), "exchange": "NYSE"}, base_url=FMP_V4_BASE)
    return data if data else []


def get_stocks_by_sector(sector: str, limit: int = 20) -> List[Dict]:
    """Get list of stocks in a specific sector."""
    data = _fmp_get("stock-screener", params={
        "sector": sector,
        "limit": limit,
        "isActivelyTrading": True,
        "marketCapMoreThan": 1000000000  # Market cap > $1B
    })
    return data if data else []


def get_stocks_by_industry(industry: str, limit: int = 20) -> List[Dict]:
    """Get list of stocks in a specific industry."""
    data = _fmp_get("stock-screener", params={
        "industry": industry,
        "limit": limit,
        "isActivelyTrading": True,
        "marketCapMoreThan": 500000000  # Market cap > $500M
    })
    return data if data else []


def get_company_profile(symbol: str) -> Dict:
    """Get company profile data."""
    data = _fmp_get(f"profile/{symbol}")
    return data[0] if data and len(data) > 0 else {}


def get_company_ratios(symbol: str) -> Dict:
    """Get company financial ratios."""
    data = _fmp_get(f"ratios/{symbol}", params={"limit": 1})
    return data[0] if data and len(data) > 0 else {}


def get_company_key_metrics(symbol: str) -> Dict:
    """Get company key metrics."""
    data = _fmp_get(f"key-metrics/{symbol}", params={"limit": 1})
    return data[0] if data and len(data) > 0 else {}


def get_income_statement(symbol: str, limit: int = 4) -> List[Dict]:
    """Get income statement data."""
    data = _fmp_get(f"income-statement/{symbol}", params={"period": "quarter", "limit": limit})
    return data if data else []


def get_sector_historical_performance(sector: str) -> List[Dict]:
    """Get historical sector performance."""
    data = _fmp_get("historical-sectors-performance", params={"limit": 30})
    return data if data else []


# ============================================
# RESEARCH NOTES UTILITIES
# ============================================

def load_research_notes_from_json(file_path: str) -> ResearchNotes:
    """Load research notes and articles from a JSON file.

    Expected JSON format:
    {
        "analyst_notes": ["Note 1", "Note 2"],
        "key_themes": ["Theme 1", "Theme 2"],
        "investment_thesis": "Overall thesis text...",
        "articles": [
            {
                "title": "Article Title",
                "source": "Source Name",
                "date": "2024-01-15",
                "content": "Article summary or key points...",
                "url": "https://..."
            }
        ]
    }
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        articles = []
        for article_data in data.get('articles', []):
            articles.append(Article(
                title=article_data.get('title', 'Untitled'),
                source=article_data.get('source', ''),
                date=article_data.get('date', ''),
                content=article_data.get('content', ''),
                url=article_data.get('url', '')
            ))

        return ResearchNotes(
            analyst_notes=data.get('analyst_notes', []),
            articles=articles,
            key_themes=data.get('key_themes', []),
            investment_thesis=data.get('investment_thesis', '')
        )
    except FileNotFoundError:
        logger.error(f"Research notes file not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in research notes file: {e}")
        raise


def create_research_notes(
    analyst_notes: List[str] = None,
    articles: List[Dict] = None,
    key_themes: List[str] = None,
    investment_thesis: str = ""
) -> ResearchNotes:
    """Create a ResearchNotes object from individual components.

    Args:
        analyst_notes: List of analyst note strings
        articles: List of article dictionaries with keys: title, source, date, content, url
        key_themes: List of key investment themes
        investment_thesis: Overall investment thesis text

    Returns:
        ResearchNotes object
    """
    article_objects = []
    if articles:
        for article_data in articles:
            article_objects.append(Article(
                title=article_data.get('title', 'Untitled'),
                source=article_data.get('source', ''),
                date=article_data.get('date', ''),
                content=article_data.get('content', ''),
                url=article_data.get('url', '')
            ))

    return ResearchNotes(
        analyst_notes=analyst_notes or [],
        articles=article_objects,
        key_themes=key_themes or [],
        investment_thesis=investment_thesis
    )


# ============================================
# AI ANALYSIS FUNCTIONS
# ============================================

def generate_industry_analysis(
    industry: str,
    companies: List[Dict],
    sector_data: Dict,
    ai_provider: str = "anthropic"
) -> Dict[str, str]:
    """Generate AI-powered industry analysis."""

    # Build context for AI
    company_summaries = []
    for company in companies[:10]:  # Top 10 companies
        summary = f"- {company.get('companyName', 'N/A')} ({company.get('symbol', 'N/A')}): "
        summary += f"Market Cap: ${company.get('marketCap', 0)/1e9:.1f}B, "
        summary += f"Price: ${company.get('price', 0):.2f}, "
        summary += f"Beta: {company.get('beta', 'N/A')}"
        company_summaries.append(summary)

    context = f"""
Industry: {industry}
Sector: {sector_data.get('sector', 'N/A')}
Average P/E: {sector_data.get('pe', 'N/A')}

Top Companies in this Industry:
{chr(10).join(company_summaries)}
"""

    prompts = {
        "overview": f"""Analyze this industry and provide a concise overview (2-3 paragraphs):
{context}

Focus on:
1. Current state of the industry
2. Key growth drivers and headwinds
3. Competitive dynamics""",

        "trends": f"""Identify the top 3-5 trends shaping this industry:
{context}

For each trend, explain:
1. What the trend is
2. How it impacts companies in the space
3. Which companies are best/worst positioned""",

        "risks": f"""Identify the top risks facing this industry:
{context}

Categories to consider:
1. Regulatory risks
2. Technological disruption
3. Economic sensitivity
4. Competitive threats
5. Supply chain vulnerabilities""",

        "outlook": f"""Provide a 12-month outlook for this industry:
{context}

Include:
1. Growth expectations
2. Key catalysts to watch
3. Potential headwinds
4. Investment recommendation (Overweight/Neutral/Underweight)"""
    }

    results = {}

    for section, prompt in prompts.items():
        try:
            if ai_provider == "anthropic" and anthropic_client:
                response = anthropic_client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1000,
                    messages=[{"role": "user", "content": prompt}]
                )
                results[section] = response.content[0].text
            elif ai_provider == "openai" and openai_client:
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    max_tokens=1000,
                    messages=[{"role": "user", "content": prompt}]
                )
                results[section] = response.choices[0].message.content
            else:
                results[section] = "AI analysis not available - no API key configured."
        except Exception as e:
            logger.error(f"AI analysis failed for {section}: {e}")
            results[section] = f"Analysis unavailable: {str(e)}"

    return results


def identify_winners_losers(
    industry: str,
    companies: List[Dict],
    trends_analysis: str,
    ai_provider: str = "anthropic"
) -> WinnersLosersAnalysis:
    """Identify winners and losers based on industry trends.

    Args:
        industry: Industry name
        companies: List of company data
        trends_analysis: AI-generated trends analysis text
        ai_provider: "anthropic" or "openai"

    Returns:
        WinnersLosersAnalysis object with categorized companies
    """
    # Build company context with more details
    company_details = []
    for company in companies[:15]:  # Analyze top 15 companies
        detail = f"- {company.get('symbol', 'N/A')} ({company.get('companyName', 'N/A')}): "
        detail += f"Market Cap: ${company.get('marketCap', 0)/1e9:.1f}B, "
        detail += f"Price: ${company.get('price', 0):.2f}, "
        detail += f"Beta: {company.get('beta', 'N/A')}, "
        detail += f"Volume: {company.get('volume', 'N/A')}"
        company_details.append(detail)

    prompt = f"""Based on the industry trends analysis below, categorize the following companies as WINNERS, LOSERS, or NEUTRAL.

INDUSTRY: {industry}

KEY TRENDS:
{trends_analysis}

COMPANIES TO ANALYZE:
{chr(10).join(company_details)}

For each company, provide your assessment in the following JSON format:
{{
    "summary": "Brief 2-3 sentence summary of overall winners/losers dynamics",
    "winners": [
        {{
            "symbol": "TICKER",
            "company_name": "Full Name",
            "trend": "The specific trend they benefit from",
            "rationale": "Why they are positioned to win (2-3 sentences)",
            "confidence": "High/Medium/Low"
        }}
    ],
    "losers": [
        {{
            "symbol": "TICKER",
            "company_name": "Full Name",
            "trend": "The specific trend hurting them",
            "rationale": "Why they are at risk (2-3 sentences)",
            "confidence": "High/Medium/Low"
        }}
    ],
    "neutral": [
        {{
            "symbol": "TICKER",
            "company_name": "Full Name",
            "trend": "Mixed exposure",
            "rationale": "Why position is unclear (1-2 sentences)",
            "confidence": "Low"
        }}
    ]
}}

Focus on:
1. Which companies have business models that ALIGN with or BENEFIT from the key trends
2. Which companies face HEADWINDS or DISRUPTION from the trends
3. Be specific about WHY each company is positioned as winner/loser
4. Consider company size, market position, and adaptability

Return ONLY valid JSON, no other text."""

    try:
        if ai_provider == "anthropic" and anthropic_client:
            response = anthropic_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            response_text = response.content[0].text
        elif ai_provider == "openai" and openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            response_text = response.choices[0].message.content
        else:
            return WinnersLosersAnalysis(summary="AI analysis not available - no API key configured.")

        # Parse JSON response
        # Clean up response if needed (remove markdown code blocks)
        response_text = response_text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        response_text = response_text.strip()

        data = json.loads(response_text)

        # Build result
        winners = []
        for w in data.get('winners', []):
            winners.append(CompanyTrendPosition(
                symbol=w.get('symbol', ''),
                company_name=w.get('company_name', ''),
                position='winner',
                trend=w.get('trend', ''),
                rationale=w.get('rationale', ''),
                confidence=w.get('confidence', 'Medium')
            ))

        losers = []
        for l in data.get('losers', []):
            losers.append(CompanyTrendPosition(
                symbol=l.get('symbol', ''),
                company_name=l.get('company_name', ''),
                position='loser',
                trend=l.get('trend', ''),
                rationale=l.get('rationale', ''),
                confidence=l.get('confidence', 'Medium')
            ))

        neutral = []
        for n in data.get('neutral', []):
            neutral.append(CompanyTrendPosition(
                symbol=n.get('symbol', ''),
                company_name=n.get('company_name', ''),
                position='neutral',
                trend=n.get('trend', ''),
                rationale=n.get('rationale', ''),
                confidence=n.get('confidence', 'Low')
            ))

        return WinnersLosersAnalysis(
            winners=winners,
            losers=losers,
            neutral=neutral,
            summary=data.get('summary', '')
        )

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse winners/losers JSON: {e}")
        return WinnersLosersAnalysis(summary=f"Analysis parsing failed: {str(e)}")
    except Exception as e:
        logger.error(f"Winners/losers analysis failed: {e}")
        return WinnersLosersAnalysis(summary=f"Analysis failed: {str(e)}")


def generate_market_view_analysis(
    title: str,
    research_notes: ResearchNotes,
    ai_provider: str = "anthropic"
) -> Dict[str, str]:
    """Generate AI-powered market view analysis based on research files and notes.

    This function creates analysis from uploaded research documents rather than
    fetching company data from FMP API.
    """

    # Build context from research files and notes
    context_parts = []

    # Add research file content
    if research_notes.research_files:
        context_parts.append("=== RESEARCH DOCUMENTS ===")
        for rf in research_notes.research_files:
            context_parts.append(f"\n--- {rf.filename} ---")
            if rf.summary:
                context_parts.append(rf.summary)
            elif rf.content:
                # Use first 3000 chars of content if no summary
                context_parts.append(rf.content[:3000])

    # Add analyst notes
    if research_notes.analyst_notes:
        context_parts.append("\n=== ANALYST NOTES ===")
        for note in research_notes.analyst_notes:
            context_parts.append(f"• {note}")

    # Add key themes
    if research_notes.key_themes:
        context_parts.append("\n=== KEY THEMES ===")
        for theme in research_notes.key_themes:
            context_parts.append(f"• {theme}")

    # Add investment thesis
    if research_notes.investment_thesis:
        context_parts.append("\n=== INVESTMENT THESIS ===")
        context_parts.append(research_notes.investment_thesis)

    # Add articles
    if research_notes.articles:
        context_parts.append("\n=== RESEARCH ARTICLES ===")
        for article in research_notes.articles:
            context_parts.append(f"\n{article.title}")
            if article.source:
                context_parts.append(f"Source: {article.source}")
            if article.content:
                context_parts.append(article.content[:1000])

    context = "\n".join(context_parts)

    if not context.strip():
        return {
            "overview": "No research content provided for analysis.",
            "trends": "No trends identified - please provide research files or notes.",
            "risks": "No risks identified - please provide research files or notes.",
            "outlook": "No outlook available - please provide research files or notes."
        }

    prompts = {
        "overview": f"""Based on the following research materials, provide a comprehensive market overview (2-3 paragraphs):

{context}

Focus on:
1. Current market conditions and sentiment
2. Key factors driving the market
3. Notable developments or themes from the research""",

        "trends": f"""Based on the following research materials, identify the top 3-5 market trends:

{context}

For each trend, explain:
1. What the trend is
2. Why it matters for investors
3. Which sectors or companies are most affected""",

        "risks": f"""Based on the following research materials, identify the key market risks:

{context}

Categories to consider:
1. Macroeconomic risks
2. Geopolitical risks
3. Sector-specific risks
4. Valuation concerns
5. Policy/regulatory risks""",

        "outlook": f"""Based on the following research materials, provide a market outlook:

{context}

Include:
1. Near-term expectations (1-3 months)
2. Key catalysts to watch
3. Potential headwinds
4. Investment positioning recommendations"""
    }

    results = {}

    for section, prompt in prompts.items():
        try:
            if ai_provider == "anthropic" and anthropic_client:
                response = anthropic_client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}]
                )
                results[section] = response.content[0].text
            elif ai_provider == "openai" and openai_client:
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}]
                )
                results[section] = response.choices[0].message.content
            else:
                results[section] = "AI analysis not available - no API key configured."
        except Exception as e:
            logger.error(f"Market view AI analysis failed for {section}: {e}")
            results[section] = f"Analysis unavailable: {str(e)}"

    return results


# ============================================
# PDF GENERATION
# ============================================

def format_currency(value: float, in_millions: bool = True) -> str:
    """Format currency values."""
    if value is None:
        return "N/A"
    if in_millions:
        if abs(value) >= 1e12:
            return f"${value/1e12:.1f}T"
        elif abs(value) >= 1e9:
            return f"${value/1e9:.1f}B"
        elif abs(value) >= 1e6:
            return f"${value/1e6:.1f}M"
    return f"${value:,.0f}"


def format_percentage(value: float) -> str:
    """Format percentage values."""
    if value is None:
        return "N/A"
    return f"{value:.1f}%"


def create_comparison_chart(companies: List[Dict], metric_key: str, title: str, color: str = "#2E86AB") -> Drawing:
    """Create a bar chart comparing companies on a specific metric."""
    drawing = Drawing(500, 200)

    data = []
    labels = []
    for company in companies[:8]:  # Max 8 companies
        value = company.get(metric_key, 0)
        if value and isinstance(value, (int, float)):
            data.append(value)
            labels.append(company.get('symbol', '?')[:5])

    if not data:
        return drawing

    chart = VerticalBarChart()
    chart.x = 50
    chart.y = 30
    chart.height = 140
    chart.width = 400
    chart.data = [data]
    chart.categoryAxis.categoryNames = labels
    chart.categoryAxis.labels.fontName = 'Helvetica'
    chart.categoryAxis.labels.fontSize = 8
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = max(data) * 1.1 if data else 100
    chart.bars[0].fillColor = HexColor(color)

    drawing.add(chart)
    return drawing


def generate_industry_pdf(
    industry: str,
    companies: List[Dict],
    sector_data: Dict,
    ai_analysis: Dict[str, str],
    output_path: str = None,
    logo_path: str = None,
    research_notes: ResearchNotes = None,
    winners_losers: WinnersLosersAnalysis = None,
    market_view_mode: bool = False
) -> str:
    """Generate a PDF report for the industry.

    Args:
        industry: Industry or sector name
        companies: List of company data dictionaries
        sector_data: Sector-level metrics
        ai_analysis: AI-generated analysis sections
        output_path: Custom output path for PDF
        logo_path: Path to company logo image
        research_notes: Optional ResearchNotes with analyst notes and articles
        winners_losers: Optional WinnersLosersAnalysis with trend-based categorization
        market_view_mode: If True, skip company-specific sections
    """

    if not output_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_industry = industry.replace(" ", "_").replace("/", "_")[:30]
        output_path = f"output/{safe_industry}_Industry_Report_{timestamp}.pdf"

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else "output", exist_ok=True)

    # Create PDF
    buffer = io.BytesIO()
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
        fontSize=24,
        alignment=TA_CENTER,
        spaceAfter=20,
        textColor=HexColor('#1a1a2e')
    )
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=14,
        spaceBefore=15,
        spaceAfter=10,
        textColor=HexColor('#2E86AB')
    )
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['Normal'],
        fontSize=10,
        spaceAfter=8,
        leading=14
    )
    note_style = ParagraphStyle(
        'NoteStyle',
        parent=styles['Normal'],
        fontSize=9,
        spaceAfter=6,
        leading=12,
        leftIndent=10,
        bulletIndent=0
    )
    article_title_style = ParagraphStyle(
        'ArticleTitle',
        parent=styles['Normal'],
        fontSize=11,
        fontName='Helvetica-Bold',
        spaceAfter=4,
        textColor=HexColor('#1a1a2e')
    )
    article_meta_style = ParagraphStyle(
        'ArticleMeta',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.gray,
        spaceAfter=4
    )
    winner_style = ParagraphStyle(
        'WinnerStyle',
        parent=styles['Normal'],
        fontSize=9,
        spaceAfter=4,
        leftIndent=5,
        textColor=HexColor('#155724')  # Dark green
    )
    loser_style = ParagraphStyle(
        'LoserStyle',
        parent=styles['Normal'],
        fontSize=9,
        spaceAfter=4,
        leftIndent=5,
        textColor=HexColor('#721c24')  # Dark red
    )
    neutral_style = ParagraphStyle(
        'NeutralStyle',
        parent=styles['Normal'],
        fontSize=9,
        spaceAfter=4,
        leftIndent=5,
        textColor=HexColor('#856404')  # Dark yellow/amber
    )
    rationale_style = ParagraphStyle(
        'RationaleStyle',
        parent=styles['Normal'],
        fontSize=8,
        spaceAfter=8,
        leftIndent=15,
        textColor=HexColor('#555555'),
        leading=10
    )

    elements = []

    # Add company logo if it exists
    actual_logo_path = logo_path or DEFAULT_LOGO_PATH
    if os.path.exists(actual_logo_path):
        try:
            img = Image(actual_logo_path, width=4.68*inch, height=1.56*inch)
            img.hAlign = 'CENTER'
            elements.append(img)
            elements.append(Spacer(1, 0.15*inch))
        except (IOError, OSError, ValueError) as e:
            logger.warning(f"Could not load company logo: {e}")

    # Tagline
    tagline_style = ParagraphStyle(
        'Tagline',
        parent=styles['Normal'],
        fontSize=10,
        alignment=TA_CENTER,
        textColor=HexColor('#666666'),
        spaceAfter=15
    )
    elements.append(Paragraph("Precision Analysis for Informed Investment Decisions", tagline_style))

    # Title
    elements.append(Paragraph(f"Industry Report: {industry}", title_style))
    elements.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y')}",
                              ParagraphStyle('Date', alignment=TA_CENTER, fontSize=10, textColor=colors.gray)))
    elements.append(Spacer(1, 20))

    # Sector Overview
    elements.append(Paragraph("1. Industry Overview", heading_style))
    if ai_analysis.get('overview'):
        elements.append(Paragraph(ai_analysis['overview'].replace('\n', '<br/>'), body_style))
    elements.append(Spacer(1, 10))

    # Section numbering tracker
    section_num = 2

    # Key Metrics Table (skip for market view or show different metrics)
    if not market_view_mode:
        elements.append(Paragraph(f"{section_num}. Industry Metrics", heading_style))
        metrics_data = [
            ["Metric", "Value"],
            ["Sector", sector_data.get('sector', 'N/A')],
            ["Industry P/E Ratio", f"{sector_data.get('pe', 'N/A')}"],
            ["Number of Companies Analyzed", str(len(companies))],
            ["Total Market Cap", format_currency(sum(c.get('marketCap', 0) for c in companies))],
            ["Avg Market Cap", format_currency(sum(c.get('marketCap', 0) for c in companies) / len(companies) if companies else 0)],
        ]

        metrics_table = Table(metrics_data, colWidths=[200, 250])
        metrics_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#2E86AB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.gray),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
        ]))
        elements.append(metrics_table)
        elements.append(Spacer(1, 15))
        section_num += 1

        # Top Companies Table
        elements.append(Paragraph(f"{section_num}. Top Companies by Market Cap", heading_style))
        company_data = [["Rank", "Company", "Symbol", "Market Cap", "Price", "Beta"]]
        for i, company in enumerate(companies[:10], 1):
            company_data.append([
                str(i),
                company.get('companyName', 'N/A')[:30],
                company.get('symbol', 'N/A'),
                format_currency(company.get('marketCap', 0)),
                f"${company.get('price', 0):.2f}",
                f"{company.get('beta', 0):.2f}" if company.get('beta') else "N/A"
            ])

        company_table = Table(company_data, colWidths=[40, 150, 60, 90, 70, 50])
        company_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#2E86AB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ALIGN', (3, 0), (-1, -1), 'RIGHT'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.gray),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#f5f5f5')]),
        ]))
        elements.append(company_table)
        elements.append(PageBreak())
        section_num += 1

        # Market Cap Comparison Chart
        if companies:
            elements.append(Paragraph(f"{section_num}. Market Cap Comparison", heading_style))
            chart = create_comparison_chart(companies, 'marketCap', 'Market Cap by Company', '#2E86AB')
            elements.append(chart)
            elements.append(Spacer(1, 20))
            section_num += 1

    # Industry/Market Trends
    trend_title = "Key Market Trends" if market_view_mode else "Key Industry Trends"
    elements.append(Paragraph(f"{section_num}. {trend_title}", heading_style))
    if ai_analysis.get('trends'):
        elements.append(Paragraph(ai_analysis['trends'].replace('\n', '<br/>'), body_style))
    elements.append(Spacer(1, 15))
    section_num += 1

    # Winners & Losers Section (skip for market view)
    if not market_view_mode and winners_losers and (winners_losers.winners or winners_losers.losers):
        elements.append(Paragraph(f"{section_num}. Winners & Losers from Trends", heading_style))

        # Summary
        if winners_losers.summary:
            elements.append(Paragraph(winners_losers.summary.replace('\n', '<br/>'), body_style))
            elements.append(Spacer(1, 10))

        # Winners Table
        if winners_losers.winners:
            elements.append(Paragraph("<b>WINNERS</b> - Companies positioned to benefit",
                                      ParagraphStyle('WinnerHeader', fontSize=11, textColor=HexColor('#155724'),
                                                     fontName='Helvetica-Bold', spaceBefore=10, spaceAfter=5)))

            winner_data = [["Symbol", "Company", "Trend", "Confidence"]]
            for w in winners_losers.winners:
                winner_data.append([
                    w.symbol,
                    w.company_name[:25] if w.company_name else "N/A",
                    w.trend[:30] if w.trend else "N/A",
                    w.confidence
                ])

            winner_table = Table(winner_data, colWidths=[60, 150, 180, 70])
            winner_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), HexColor('#d4edda')),  # Light green header
                ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#155724')),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (-1, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 0.5, HexColor('#c3e6cb')),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#f8fff8')]),
            ]))
            elements.append(winner_table)

            # Winner rationales
            for w in winners_losers.winners:
                if w.rationale:
                    elements.append(Paragraph(f"<b>{w.symbol}:</b> {w.rationale}", rationale_style))
            elements.append(Spacer(1, 15))

        # Losers Table
        if winners_losers.losers:
            elements.append(Paragraph("<b>LOSERS</b> - Companies facing headwinds",
                                      ParagraphStyle('LoserHeader', fontSize=11, textColor=HexColor('#721c24'),
                                                     fontName='Helvetica-Bold', spaceBefore=10, spaceAfter=5)))

            loser_data = [["Symbol", "Company", "Trend", "Confidence"]]
            for l in winners_losers.losers:
                loser_data.append([
                    l.symbol,
                    l.company_name[:25] if l.company_name else "N/A",
                    l.trend[:30] if l.trend else "N/A",
                    l.confidence
                ])

            loser_table = Table(loser_data, colWidths=[60, 150, 180, 70])
            loser_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), HexColor('#f8d7da')),  # Light red header
                ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#721c24')),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (-1, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 0.5, HexColor('#f5c6cb')),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#fff8f8')]),
            ]))
            elements.append(loser_table)

            # Loser rationales
            for l in winners_losers.losers:
                if l.rationale:
                    elements.append(Paragraph(f"<b>{l.symbol}:</b> {l.rationale}", rationale_style))
            elements.append(Spacer(1, 15))

        # Neutral (optional, if any)
        if winners_losers.neutral:
            elements.append(Paragraph("<b>NEUTRAL</b> - Mixed or unclear positioning",
                                      ParagraphStyle('NeutralHeader', fontSize=11, textColor=HexColor('#856404'),
                                                     fontName='Helvetica-Bold', spaceBefore=10, spaceAfter=5)))
            for n in winners_losers.neutral:
                elements.append(Paragraph(f"• <b>{n.symbol}</b> ({n.company_name}): {n.rationale}", neutral_style))
            elements.append(Spacer(1, 15))

        elements.append(PageBreak())
        section_num += 1

    # Risks
    risk_title = "Market Risks" if market_view_mode else "Industry Risks"
    elements.append(Paragraph(f"{section_num}. {risk_title}", heading_style))
    if ai_analysis.get('risks'):
        elements.append(Paragraph(ai_analysis['risks'].replace('\n', '<br/>'), body_style))
    elements.append(Spacer(1, 15))
    section_num += 1

    # Outlook
    elements.append(Paragraph(f"{section_num}. Market Outlook", heading_style))
    if ai_analysis.get('outlook'):
        elements.append(Paragraph(ai_analysis['outlook'].replace('\n', '<br/>'), body_style))
    section_num += 1

    # Research Notes & Articles Section
    if research_notes:
        elements.append(PageBreak())

        # Analyst Notes
        if research_notes.analyst_notes:
            elements.append(Paragraph(f"{section_num}. Analyst Notes", heading_style))
            for note in research_notes.analyst_notes:
                # Add bullet point
                elements.append(Paragraph(f"• {note}", note_style))
            elements.append(Spacer(1, 15))
            section_num += 1

        # Key Themes
        if research_notes.key_themes:
            elements.append(Paragraph(f"{section_num}. Key Investment Themes", heading_style))
            for theme in research_notes.key_themes:
                elements.append(Paragraph(f"• {theme}", note_style))
            elements.append(Spacer(1, 15))
            section_num += 1

        # Investment Thesis
        if research_notes.investment_thesis:
            elements.append(Paragraph(f"{section_num}. Investment Thesis", heading_style))
            elements.append(Paragraph(research_notes.investment_thesis.replace('\n', '<br/>'), body_style))
            elements.append(Spacer(1, 15))
            section_num += 1

        # Articles
        if research_notes.articles:
            elements.append(Paragraph(f"{section_num}. Research Articles & Sources", heading_style))
            for article in research_notes.articles:
                # Article title
                title_text = article.title
                if article.url:
                    title_text = f'<link href="{article.url}">{article.title}</link>'
                elements.append(Paragraph(title_text, article_title_style))

                # Article metadata
                meta_parts = []
                if article.source:
                    meta_parts.append(article.source)
                if article.date:
                    meta_parts.append(article.date)
                if meta_parts:
                    elements.append(Paragraph(" | ".join(meta_parts), article_meta_style))

                # Article content/summary
                if article.content:
                    elements.append(Paragraph(article.content.replace('\n', '<br/>'), note_style))
                elements.append(Spacer(1, 10))
            section_num += 1

        # Research Files (uploaded documents with summaries)
        if research_notes.research_files:
            elements.append(PageBreak())
            elements.append(Paragraph(f"{section_num}. Research Documents", heading_style))
            elements.append(Paragraph(f"{len(research_notes.research_files)} document(s) analyzed", article_meta_style))
            elements.append(Spacer(1, 10))

            # Style for file type badge
            file_type_style = ParagraphStyle(
                'FileType',
                parent=styles['Normal'],
                fontSize=8,
                textColor=colors.gray,
                spaceAfter=2
            )

            for research_file in research_notes.research_files:
                # File icon based on type
                file_icon = {"pdf": "[PDF]", "word": "[WORD]", "excel": "[EXCEL]", "text": "[TXT]"}.get(research_file.file_type, "[FILE]")

                # File title
                elements.append(Paragraph(f"<b>{file_icon} {research_file.filename}</b>", article_title_style))

                # Summary
                if research_file.summary:
                    # Clean up the summary text for PDF rendering
                    summary_text = research_file.summary.replace('\n', '<br/>')
                    summary_text = summary_text.replace('**', '<b>').replace('**', '</b>')  # Basic markdown
                    elements.append(Paragraph(summary_text, note_style))

                elements.append(Spacer(1, 15))

    # Build PDF
    doc.build(elements)

    # Write to file
    with open(output_path, 'wb') as f:
        f.write(buffer.getvalue())

    logger.info(f"Industry report generated: {output_path}")
    return output_path


# ============================================
# MAIN REPORT GENERATION FUNCTION
# ============================================

def generate_industry_report(
    industry: str = None,
    sector: str = None,
    limit: int = 20,
    ai_provider: str = "anthropic",
    output_path: str = None,
    logo_path: str = None,
    research_notes: ResearchNotes = None,
    market_view: bool = False,
    report_title: str = None
) -> str:
    """
    Generate a comprehensive industry report.

    Args:
        industry: Specific industry name (e.g., "Software - Application")
        sector: Sector name (e.g., "Technology") - used if industry not specified
        limit: Maximum number of companies to include
        ai_provider: "anthropic" or "openai"
        output_path: Custom output path for the PDF
        logo_path: Path to company logo image file
        research_notes: ResearchNotes object with analyst notes and articles
        market_view: If True, generate a market view report based on research files only
        report_title: Custom title for the report (used with market_view)

    Returns:
        Path to generated PDF report
    """

    # Market View mode - generate report from research files without FMP data
    if market_view:
        if not research_notes:
            raise ValueError("Market view report requires research_notes")

        title = report_title or "Market View"
        logger.info(f"Generating market view report: {title}")

        # Generate AI analysis from research content
        ai_analysis = generate_market_view_analysis(
            title,
            research_notes,
            ai_provider=ai_provider
        )

        # Generate PDF without company data
        pdf_path = generate_industry_pdf(
            title,
            companies=[],  # No companies for market view
            sector_data={'sector': 'Market View'},
            ai_analysis=ai_analysis,
            output_path=output_path,
            logo_path=logo_path,
            research_notes=research_notes,
            winners_losers=None,
            market_view_mode=True
        )

        return pdf_path

    logger.info(f"Generating industry report for: {industry or sector}")

    # Get companies
    if industry:
        companies = get_stocks_by_industry(industry, limit=limit)
        target = industry
    elif sector:
        companies = get_stocks_by_sector(sector, limit=limit)
        target = sector
    else:
        raise ValueError("Must specify either industry or sector")

    if not companies:
        raise ValueError(f"No companies found for {target}")

    # Sort by market cap
    companies = sorted(companies, key=lambda x: x.get('marketCap', 0), reverse=True)

    # Get sector data
    sector_pe_data = get_sector_pe_ratios()
    sector_data = {}
    if sector_pe_data:
        for item in sector_pe_data:
            if sector and item.get('sector', '').lower() == sector.lower():
                sector_data = item
                break
        if not sector_data and companies:
            sector_data = {'sector': companies[0].get('sector', 'N/A')}

    # Generate AI analysis
    ai_analysis = generate_industry_analysis(
        target,
        companies,
        sector_data,
        ai_provider=ai_provider
    )

    # Identify winners and losers based on trends
    winners_losers = None
    if ai_analysis.get('trends'):
        logger.info("Identifying winners and losers from industry trends...")
        winners_losers = identify_winners_losers(
            target,
            companies,
            ai_analysis['trends'],
            ai_provider=ai_provider
        )
        logger.info(f"Identified {len(winners_losers.winners)} winners, {len(winners_losers.losers)} losers")

    # Generate PDF
    pdf_path = generate_industry_pdf(
        target,
        companies,
        sector_data,
        ai_analysis,
        output_path=output_path,
        logo_path=logo_path,
        research_notes=research_notes,
        winners_losers=winners_losers
    )

    return pdf_path


# ============================================
# CLI INTERFACE
# ============================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate Industry Analysis Report")
    parser.add_argument("--industry", type=str, help="Industry name (e.g., 'Software - Application')")
    parser.add_argument("--sector", type=str, help="Sector name (e.g., 'Technology')")
    parser.add_argument("--market-view", action="store_true", dest="market_view",
                        help="Generate a market view report from research files (no FMP data needed)")
    parser.add_argument("--title", type=str, help="Custom report title (used with --market-view)")
    parser.add_argument("--limit", type=int, default=20, help="Max companies to analyze (default: 20)")
    parser.add_argument("--ai", type=str, default="anthropic", choices=["anthropic", "openai"],
                        help="AI provider for analysis")
    parser.add_argument("--output", type=str, help="Custom output path")
    parser.add_argument("--logo", type=str, help="Path to company logo image")
    parser.add_argument("--notes", type=str, help="Path to JSON file with research notes and articles")

    args = parser.parse_args()

    if not args.industry and not args.sector and not args.market_view:
        print("Error: Must specify --industry, --sector, or --market-view")
        print("\nExample usage:")
        print("  python industry_report_generator.py --sector Technology")
        print("  python industry_report_generator.py --industry 'Software - Application'")
        print("  python industry_report_generator.py --sector Healthcare --notes research_notes.json")
        print("  python industry_report_generator.py --sector Technology --logo my_logo.png")
        print("  python industry_report_generator.py --market-view --notes research.json --title 'Q1 2026 Market View'")
        print("\nNotes JSON format:")
        print('''  {
    "analyst_notes": ["Note 1", "Note 2"],
    "key_themes": ["Theme 1", "Theme 2"],
    "investment_thesis": "Overall thesis...",
    "articles": [
      {"title": "Article Title", "source": "Source", "date": "2024-01-15", "content": "Summary..."}
    ]
  }''')
        exit(1)

    # Load research notes if provided
    research_notes = None
    if args.notes:
        try:
            research_notes = load_research_notes_from_json(args.notes)
            print(f"Loaded research notes from: {args.notes}")
        except Exception as e:
            print(f"Warning: Could not load research notes: {e}")

    # Check if market view requires notes
    if args.market_view and not research_notes:
        print("Error: --market-view requires --notes with research content")
        exit(1)

    try:
        output_path = generate_industry_report(
            industry=args.industry,
            sector=args.sector,
            limit=args.limit,
            ai_provider=args.ai,
            output_path=args.output,
            logo_path=args.logo,
            research_notes=research_notes,
            market_view=args.market_view,
            report_title=args.title
        )
        print(f"\nReport generated successfully: {output_path}")
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        print(f"Error: {e}")
        exit(1)
