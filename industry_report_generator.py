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


@dataclass
class WebSource:
    """A single web source surfaced by web research."""
    title: str
    url: str = ""
    source: str = ""
    date: str = ""
    summary: str = ""
    relevance: str = ""


@dataclass
class WebResearchFindings:
    """Structured output from a web research pass."""
    industry_overview: str = ""
    trends: List[Dict[str, str]] = field(default_factory=list)
    key_developments: List[Dict[str, Any]] = field(default_factory=list)
    articles: List[WebSource] = field(default_factory=list)
    ticker_notes: Dict[str, str] = field(default_factory=dict)
    raw_text: str = ""  # full model output, kept for debugging / fallback

    def as_context_block(self) -> str:
        """Render findings as a plaintext block to include in downstream prompts."""
        parts = []
        if self.industry_overview:
            parts.append(f"INDUSTRY OVERVIEW (from web research):\n{self.industry_overview}")
        if self.trends:
            parts.append("RECENT TRENDS (from web research):")
            for t in self.trends:
                title = t.get("title", "")
                summary = t.get("summary", "")
                impact = t.get("impact", "")
                line = f"- {title}: {summary}"
                if impact:
                    line += f" Impact: {impact}"
                parts.append(line)
        if self.key_developments:
            parts.append("KEY RECENT DEVELOPMENTS (from web research):")
            for d in self.key_developments:
                date = d.get("date", "")
                headline = d.get("headline", "")
                summary = d.get("summary", "")
                tickers = d.get("tickers_affected", [])
                tline = f" [{', '.join(tickers)}]" if tickers else ""
                parts.append(f"- {date} {headline}{tline}: {summary}")
        if self.ticker_notes:
            parts.append("PER-COMPANY NOTES (from web research):")
            for sym, note in self.ticker_notes.items():
                parts.append(f"- {sym}: {note}")
        if self.articles:
            parts.append("SOURCES CITED:")
            for a in self.articles[:20]:
                meta = " | ".join(x for x in [a.source, a.date] if x)
                parts.append(f"- {a.title} ({meta}) {a.url}")
        return "\n".join(parts).strip()

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


def parse_ticker_input(raw: str) -> List[str]:
    """Parse a free-text ticker list (comma/space/newline separated) into a clean list.

    Strips whitespace, uppercases, removes duplicates while preserving order.
    """
    if not raw:
        return []
    import re
    tokens = re.split(r"[\s,;]+", raw.strip())
    seen = set()
    out = []
    for t in tokens:
        sym = t.strip().upper().lstrip("$")
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


def get_companies_by_tickers(tickers: List[str]) -> List[Dict]:
    """Build the company list directly from a user-supplied ticker list.

    Returns dicts shaped like FMP's stock-screener output so downstream code is unchanged.
    """
    companies: List[Dict] = []
    for sym in tickers:
        try:
            profile = get_company_profile(sym)
        except Exception as e:
            logger.warning(f"FMP profile fetch failed for {sym}: {e}")
            profile = {}
        if not profile:
            # Profile missing — still include a minimal stub so the user sees the ticker in the report
            companies.append({
                "symbol": sym,
                "companyName": sym,
                "marketCap": 0,
                "price": 0,
                "beta": None,
                "sector": "N/A",
                "industry": "N/A",
            })
            continue
        companies.append({
            "symbol": profile.get("symbol", sym),
            "companyName": profile.get("companyName", sym),
            "marketCap": profile.get("mktCap", 0) or 0,
            "price": profile.get("price", 0) or 0,
            "beta": profile.get("beta"),
            "sector": profile.get("sector", "N/A"),
            "industry": profile.get("industry", "N/A"),
            "volume": profile.get("volAvg", 0) or 0,
            "description": profile.get("description", ""),
            "exchange": profile.get("exchangeShortName", ""),
            "website": profile.get("website", ""),
        })
    return companies


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
# WEB RESEARCH (Anthropic web_search tool)
# ============================================

ANTHROPIC_MODEL = "claude-sonnet-4-6"


def _extract_text_and_citations(response) -> tuple:
    """Pull final assistant text + any citation URLs from a tool-using response."""
    text_parts = []
    cited_urls = []
    for block in getattr(response, "content", []) or []:
        btype = getattr(block, "type", None)
        if btype == "text":
            text_parts.append(block.text or "")
            for cit in getattr(block, "citations", None) or []:
                url = getattr(cit, "url", None)
                title = getattr(cit, "title", None)
                if url:
                    cited_urls.append({"url": url, "title": title or ""})
    return "\n".join(text_parts).strip(), cited_urls


def _strip_json_fence(text: str) -> str:
    """Strip code fences and any surrounding prose, returning the inner JSON.

    Handles three shapes the model commonly emits:
      * ```json {...} ```
      * {...}
      * "Some preamble.\n\n{...}\n\nSome postamble."
    """
    s = text.strip()
    if s.startswith("```json"):
        s = s[7:]
    elif s.startswith("```"):
        s = s[3:]
    if s.endswith("```"):
        s = s[:-3]
    s = s.strip()
    if s and not s.startswith("{"):
        first = s.find("{")
        last = s.rfind("}")
        if first >= 0 and last > first:
            return s[first:last + 1].strip()
    return s


def web_research_industry(
    theme: str,
    tickers: List[str],
    lookback_days: int = 60,
    max_searches: int = 12,
    tracker: Optional[Any] = None,
) -> WebResearchFindings:
    """Use Anthropic's web_search tool to gather live data on an industry theme + tickers.

    Returns structured findings. Falls back to an empty WebResearchFindings if the
    Anthropic client is unavailable or the call fails.
    """
    if not anthropic_client:
        logger.warning("Anthropic client unavailable; skipping web research.")
        return WebResearchFindings()

    ticker_list = ", ".join(tickers) if tickers else "(none provided)"
    prompt = f"""You are a senior equity research analyst. Use the web_search tool to gather live, recent information on the industry theme and companies below.

THEME: {theme}
COMPANIES OF INTEREST: {ticker_list}
LOOKBACK WINDOW: last {lookback_days} days (prioritize recency)

Research plan — execute via web_search (use multiple queries, up to {max_searches} total):
1. Industry-level: current state of the {theme} industry, structural trends, regulatory or policy moves, M&A activity, macro factors.
2. Company-level: for each ticker ({ticker_list}), find recent news, earnings highlights, analyst commentary, guidance changes, notable announcements.
3. Competitive landscape and any disruptors / new entrants.

After research is complete, output ONLY a single JSON object (no markdown, no preamble, no commentary) with this exact shape:

{{
  "industry_overview": "2-4 sentence synthesis of the current state of this industry",
  "trends": [
    {{"title": "short trend name", "summary": "1-2 sentence summary", "impact": "which of the named companies it helps or hurts and why"}}
  ],
  "key_developments": [
    {{"date": "YYYY-MM-DD", "headline": "what happened", "summary": "1-2 sentences", "tickers_affected": ["GPN"]}}
  ],
  "articles": [
    {{"title": "article title", "source": "publisher name", "date": "YYYY-MM-DD", "url": "https://...", "summary": "1-2 sentence summary", "relevance": "why this matters for the report"}}
  ],
  "ticker_notes": {{
    "TICKER1": "1-3 sentence ticker-specific synthesis tying back to the trends above"
  }}
}}

Requirements:
- Aim for 6-12 articles with real URLs from your web_search results.
- Identify 3-5 trends and 4-8 key developments.
- ticker_notes must include an entry for every ticker in COMPANIES OF INTEREST.
- Return ONLY the JSON object. Do not wrap in code fences."""

    try:
        response = anthropic_client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=8000,
            tools=[{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": max_searches,
            }],
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        logger.error(f"Anthropic web_search call failed: {e}")
        return WebResearchFindings(raw_text=f"Web research call failed: {e}")

    if tracker is not None:
        tracker.record_anthropic("industry_web_research", response)

    text, cited_urls = _extract_text_and_citations(response)

    findings = WebResearchFindings(raw_text=text)
    if not text:
        logger.warning("Web research returned no text content.")
        return findings

    try:
        data = json.loads(_strip_json_fence(text))
    except json.JSONDecodeError as e:
        logger.warning(f"Web research JSON parse failed: {e}; returning raw text only.")
        # Even if JSON parsing fails, surface the cited URLs as articles so the user still sees sources.
        findings.articles = [
            WebSource(title=c.get("title") or c.get("url", ""), url=c.get("url", ""))
            for c in cited_urls
        ]
        return findings

    findings.industry_overview = data.get("industry_overview", "") or ""
    findings.trends = data.get("trends", []) or []
    findings.key_developments = data.get("key_developments", []) or []
    findings.ticker_notes = data.get("ticker_notes", {}) or {}

    articles_raw = data.get("articles", []) or []
    articles: List[WebSource] = []
    seen_urls = set()
    for a in articles_raw:
        url = (a.get("url") or "").strip()
        if url and url in seen_urls:
            continue
        if url:
            seen_urls.add(url)
        articles.append(WebSource(
            title=a.get("title", "") or "",
            url=url,
            source=a.get("source", "") or "",
            date=a.get("date", "") or "",
            summary=a.get("summary", "") or "",
            relevance=a.get("relevance", "") or "",
        ))
    # Backfill from raw citations if model didn't include them in JSON
    for c in cited_urls:
        url = c.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            articles.append(WebSource(title=c.get("title") or url, url=url))
    findings.articles = articles

    logger.info(
        f"Web research returned {len(findings.trends)} trends, "
        f"{len(findings.key_developments)} developments, "
        f"{len(findings.articles)} articles."
    )
    return findings


@dataclass
class TickerDeepDive:
    """Focused per-ticker research result for the robust report mode."""
    ticker: str
    company_name: str = ""
    summary: str = ""
    deals: List[Dict[str, Any]] = field(default_factory=list)
    capacity: Dict[str, Any] = field(default_factory=dict)
    risks: List[str] = field(default_factory=list)
    catalysts: List[str] = field(default_factory=list)
    sources: List[WebSource] = field(default_factory=list)
    raw_text: str = ""

    def as_brief(self) -> str:
        parts = [f"=== {self.ticker} ({self.company_name}) ==="]
        if self.summary:
            parts.append(self.summary)
        if self.deals:
            parts.append("Deals & contracts:")
            for d in self.deals:
                cp = d.get("counterparty", "")
                val = d.get("value", "")
                date = d.get("date", "")
                desc = d.get("description", "")
                parts.append(f"  - {cp} | {val} | {date}: {desc}")
        if self.capacity:
            parts.append("Capacity:")
            for k, v in self.capacity.items():
                parts.append(f"  - {k}: {v}")
        if self.catalysts:
            parts.append("Catalysts: " + "; ".join(self.catalysts))
        if self.risks:
            parts.append("Risks: " + "; ".join(self.risks))
        if self.sources:
            parts.append("Sources:")
            for s in self.sources[:8]:
                parts.append(f"  - {s.title} ({s.source} {s.date}) {s.url}")
        return "\n".join(parts)


def deep_research_ticker(
    ticker: str,
    company_name: str = "",
    theme: str = "",
    lookback_days: int = 120,
    max_searches: int = 8,
    tracker: Optional[Any] = None,
) -> TickerDeepDive:
    """Run a single-ticker deep-dive using Anthropic's web_search tool.

    Returns a TickerDeepDive with structured deals / capacity / risks /
    catalysts plus cited sources. If the Anthropic client is missing or
    the call fails, returns an empty TickerDeepDive.
    """
    if not anthropic_client:
        logger.warning(f"Anthropic client unavailable; skipping deep dive for {ticker}.")
        return TickerDeepDive(ticker=ticker, company_name=company_name)

    prompt = f"""You are a senior equity research analyst writing a focused brief on {ticker} ({company_name or 'company'}) within the theme: {theme or 'industry research'}.

Use the web_search tool ({max_searches} searches max) to find recent (last {lookback_days} days) primary-source data on this company:
- Customer contracts and partnerships (counterparty, contract value, term, date)
- Operating capacity (power MW, GPU count, sites, utilization)
- Capacity expansion / build-out pipeline (announced or feasible)
- Financial position (debt, capex plans, recent earnings)
- Material catalysts in the next 6-12 months
- Material risks specific to this name

After research, return ONLY a single JSON object (no prose, no code fences) in this exact shape:

{{
  "summary": "2-4 sentence synthesis of this company's positioning",
  "deals": [
    {{"counterparty": "Company name", "value": "$X.XB", "date": "YYYY-MM-DD", "description": "1 sentence on the deal"}}
  ],
  "capacity": {{
    "current_mw": "value with units",
    "current_gpus": "value or 'n/d'",
    "target_mw_by_yearend": "value",
    "longterm_capacity_ceiling": "value",
    "notable_sites": "comma-separated"
  }},
  "catalysts": ["1 sentence per catalyst, 3-5 items"],
  "risks": ["1 sentence per risk, 3-5 items"],
  "sources": [
    {{"title": "...", "source": "publisher", "date": "YYYY-MM-DD", "url": "https://...", "summary": "1 sentence"}}
  ]
}}

Requirements: at least 5 sources with real URLs from your searches; deals list must include all material contracts found; risks and catalysts must be specific to {ticker}, not generic industry commentary."""

    try:
        response = anthropic_client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=4000,
            tools=[{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": max_searches,
            }],
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        logger.error(f"Deep-dive call for {ticker} failed: {e}")
        return TickerDeepDive(ticker=ticker, company_name=company_name, raw_text=f"call failed: {e}")

    if tracker is not None:
        tracker.record_anthropic(f"deep_dive_{ticker}", response)

    text, cited_urls = _extract_text_and_citations(response)
    dive = TickerDeepDive(ticker=ticker, company_name=company_name, raw_text=text)
    if not text:
        return dive

    try:
        data = json.loads(_strip_json_fence(text))
    except json.JSONDecodeError as e:
        logger.warning(f"Deep-dive JSON parse failed for {ticker}: {e}")
        # Surface cited URLs even when JSON parse fails
        dive.sources = [WebSource(title=c.get("title") or c.get("url", ""), url=c.get("url", "")) for c in cited_urls]
        return dive

    dive.summary = data.get("summary", "") or ""
    dive.deals = data.get("deals", []) or []
    dive.capacity = data.get("capacity", {}) or {}
    dive.catalysts = data.get("catalysts", []) or []
    dive.risks = data.get("risks", []) or []

    sources_raw = data.get("sources", []) or []
    sources: List[WebSource] = []
    seen_urls = set()
    for s in sources_raw:
        url = (s.get("url") or "").strip()
        if url and url in seen_urls:
            continue
        if url:
            seen_urls.add(url)
        sources.append(WebSource(
            title=s.get("title", "") or "",
            url=url,
            source=s.get("source", "") or "",
            date=s.get("date", "") or "",
            summary=s.get("summary", "") or "",
        ))
    for c in cited_urls:
        url = c.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            sources.append(WebSource(title=c.get("title") or url, url=url))
    dive.sources = sources

    logger.info(
        f"Deep dive {ticker}: {len(dive.deals)} deals, "
        f"{len(dive.catalysts)} catalysts, {len(dive.risks)} risks, "
        f"{len(dive.sources)} sources"
    )
    return dive


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
                    model=ANTHROPIC_MODEL,
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
                model=ANTHROPIC_MODEL,
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
                    model=ANTHROPIC_MODEL,
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
    """Format currency values with thousands separators on the mantissa."""
    if value is None:
        return "N/A"
    if in_millions:
        if abs(value) >= 1e12:
            return f"${value/1e12:,.1f}T"
        elif abs(value) >= 1e9:
            return f"${value/1e9:,.1f}B"
        elif abs(value) >= 1e6:
            return f"${value/1e6:,.1f}M"
    return f"${value:,.0f}"


def _format_markdown_for_pdf(text: str) -> str:
    """Render markdown-ish text for a reportlab Paragraph.

    - Lines that start with one or more '#' followed by a space become bold lines.
    - Newlines become <br/>.
    - Reportlab Paragraph already supports <b> inline tags, so we use those.
    """
    if not text:
        return ""
    lines = text.split("\n")
    rendered = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("#"):
            # count leading hashes
            i = 0
            while i < len(stripped) and stripped[i] == "#":
                i += 1
            # require a space after the hashes to qualify as a header line
            if i <= 6 and i < len(stripped) and stripped[i] == " ":
                rendered.append(f"<b>{stripped[i + 1:]}</b>")
                continue
        rendered.append(line)
    return "<br/>".join(rendered)


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
    chart.valueAxis.labelTextFormat = lambda v: f"{v:,.0f}"
    chart.valueAxis.labels.fontName = 'Helvetica'
    chart.valueAxis.labels.fontSize = 8
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
    market_view_mode: bool = False,
    web_findings: Any = None,
    deep_dives: Dict[str, "TickerDeepDive"] = None,
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
        elements.append(Paragraph(_format_markdown_for_pdf(ai_analysis['overview']), body_style))
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
        elements.append(Paragraph(_format_markdown_for_pdf(ai_analysis['trends']), body_style))
    elements.append(Spacer(1, 15))
    section_num += 1

    # Winners & Losers Section (skip for market view)
    if not market_view_mode and winners_losers and (winners_losers.winners or winners_losers.losers):
        elements.append(Paragraph(f"{section_num}. Winners & Losers from Trends", heading_style))

        # Summary
        if winners_losers.summary:
            elements.append(Paragraph(_format_markdown_for_pdf(winners_losers.summary), body_style))
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
        elements.append(Paragraph(_format_markdown_for_pdf(ai_analysis['risks']), body_style))
    elements.append(Spacer(1, 15))
    section_num += 1

    # Outlook
    elements.append(Paragraph(f"{section_num}. Market Outlook", heading_style))
    if ai_analysis.get('outlook'):
        elements.append(Paragraph(_format_markdown_for_pdf(ai_analysis['outlook']), body_style))
    section_num += 1

    # Industry web research & per-ticker deep dives (robust mode only)
    if web_findings is not None or deep_dives:
        elements.append(PageBreak())
        elements.append(Paragraph(f"{section_num}. Primary-Source Research", heading_style))
        section_num += 1

        if web_findings is not None:
            elements.append(Paragraph("<b>Industry Overview</b>", body_style))
            if getattr(web_findings, "industry_overview", ""):
                elements.append(Paragraph(_format_markdown_for_pdf(web_findings.industry_overview), body_style))
            elements.append(Spacer(1, 8))

            trends = getattr(web_findings, "trends", None) or []
            if trends:
                elements.append(Paragraph("<b>Sector Trends</b>", body_style))
                for t in trends:
                    title = t.get("title", "") if isinstance(t, dict) else ""
                    summary = t.get("summary", "") if isinstance(t, dict) else ""
                    impact = t.get("impact", "") if isinstance(t, dict) else ""
                    elements.append(Paragraph(
                        f"<b>{title}</b><br/>{summary}<br/><i>Impact: {impact}</i>",
                        body_style,
                    ))
                    elements.append(Spacer(1, 4))

            developments = getattr(web_findings, "key_developments", None) or []
            if developments:
                elements.append(Paragraph("<b>Key Recent Developments</b>", body_style))
                dev_rows = [["Date", "Tickers", "Headline"]]
                for d in developments[:12]:
                    if not isinstance(d, dict):
                        continue
                    dev_rows.append([
                        d.get("date", ""),
                        ", ".join(d.get("tickers_affected", []) or [])[:18],
                        (d.get("headline", "") + (": " + d.get("summary", "") if d.get("summary") else ""))[:120],
                    ])
                if len(dev_rows) > 1:
                    dev_table = Table(dev_rows, colWidths=[70, 70, 360])
                    dev_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#2E86AB')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, -1), 8),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                        ('GRID', (0, 0), (-1, -1), 0.4, colors.gray),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#f5f5f5')]),
                    ]))
                    elements.append(dev_table)
                    elements.append(Spacer(1, 8))

        if deep_dives:
            elements.append(Paragraph("<b>Per-Ticker Deep Dives</b>", body_style))
            for sym, dive in deep_dives.items():
                elements.append(Spacer(1, 6))
                elements.append(Paragraph(
                    f"<b>{sym} — {dive.company_name or sym}</b>",
                    body_style,
                ))
                if dive.summary:
                    elements.append(Paragraph(_format_markdown_for_pdf(dive.summary), body_style))
                if dive.deals:
                    deal_rows = [["Counterparty", "Value", "Date", "Description"]]
                    for d in dive.deals[:8]:
                        if not isinstance(d, dict):
                            continue
                        deal_rows.append([
                            d.get("counterparty", "")[:25],
                            d.get("value", "")[:15],
                            d.get("date", ""),
                            d.get("description", "")[:80],
                        ])
                    if len(deal_rows) > 1:
                        deal_table = Table(deal_rows, colWidths=[110, 70, 70, 250])
                        deal_table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#155724')),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, -1), 8),
                            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                            ('GRID', (0, 0), (-1, -1), 0.4, colors.gray),
                            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#f5f5f5')]),
                        ]))
                        elements.append(deal_table)
                if dive.capacity:
                    cap_lines = "<br/>".join(f"<b>{k.replace('_', ' ').title()}:</b> {v}" for k, v in dive.capacity.items() if v)
                    if cap_lines:
                        elements.append(Spacer(1, 4))
                        elements.append(Paragraph(cap_lines, body_style))
                if dive.catalysts:
                    elements.append(Paragraph("<b>Catalysts:</b> " + "; ".join(dive.catalysts), body_style))
                if dive.risks:
                    elements.append(Paragraph("<b>Risks:</b> " + "; ".join(dive.risks), body_style))
                if dive.sources:
                    src_text = "<br/>".join(
                        f"- <a href='{s.url}'>{s.title}</a> ({s.source} {s.date})"
                        for s in dive.sources[:6] if s.url
                    )
                    if src_text:
                        elements.append(Spacer(1, 4))
                        elements.append(Paragraph(f"<b>Sources:</b><br/>{src_text}", body_style))
                elements.append(Spacer(1, 12))

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
            elements.append(Paragraph(_format_markdown_for_pdf(research_notes.investment_thesis), body_style))
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
                    elements.append(Paragraph(_format_markdown_for_pdf(article.content), note_style))
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
                    summary_text = _format_markdown_for_pdf(research_file.summary)
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
