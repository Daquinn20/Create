"""
Annual Report Analysis Module

Fetches and analyzes the last 3 annual reports (10-K filings) for a company
using FMP API and SEC EDGAR as data sources.
"""

import os
import re
import requests
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# AI Libraries
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# API Configuration
FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"
SEC_EDGAR_BASE = "https://www.sec.gov"
SEC_SEARCH_API = "https://efts.sec.gov/LATEST/search-index"

# AI API Keys
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# AI Analysis Prompts
AI_PROMPTS = {
    "business_summary": """Analyze this company's Business section from their 10-K annual report. Focus on:

1. **What is the Business?**
   - Core business model (how do they make money?)
   - Main products/services and revenue mix
   - Key customers and end markets

2. **Business Changes & Evolution**
   - New products or services launched
   - New markets entered (geographic or segment)
   - Products/markets being exited or de-emphasized
   - Strategic pivots or shifts in focus

3. **Competitive Position**
   - Key competitors mentioned
   - Competitive advantages/moat
   - Market share or positioning claims
   - Any competitive threats mentioned

4. **Management & Leadership**
   - Key executive changes mentioned
   - Management structure or reorganizations

Be specific and quote relevant passages where helpful. Flag anything that signals change.

BUSINESS SECTION:
{content}""",

    "risk_analysis": """Analyze the Risk Factors section from this 10-K. Focus on:

1. **Most Concerning Risks** (Top 5)
   - What are the biggest threats to this business?
   - Which risks could be existential or severely damaging?

2. **New/Growing Risks**
   - Risks that appear to be new or getting worse
   - Emerging competitive threats
   - Regulatory or legal concerns building

3. **Competition Risks**
   - New competitors entering the space
   - Competitive dynamics changing
   - Pricing pressure or margin threats

4. **Financial/Debt Risks**
   - Debt levels or covenant concerns
   - Liquidity issues
   - Cash flow problems
   - Currency or interest rate exposure

5. **Management/Operational Risks**
   - Key person dependencies
   - Operational vulnerabilities
   - Supply chain or concentration risks

Rate each major risk as: LOW / MEDIUM / HIGH concern.

RISK FACTORS SECTION:
{content}""",

    "mda_analysis": """Analyze the MD&A section from this 10-K. Focus on:

1. **Business Strength/Weakness Indicators**
   - Is the business getting stronger or weaker? Evidence?
   - Revenue growth trends (accelerating/decelerating?)
   - Market share gains or losses
   - Customer retention/churn signals

2. **Financial Health**
   - Revenue and profit trends with numbers
   - Margin trends (gross, operating, net)
   - Cash flow generation
   - Debt levels and changes
   - Any balance sheet concerns?

3. **Strategic Changes**
   - New initiatives or investments
   - Cost cutting or restructuring
   - M&A activity (acquisitions, divestitures)
   - Changes in capital allocation

4. **Management Commentary**
   - Tone (confident, cautious, concerned?)
   - Forward guidance or outlook
   - Areas management is emphasizing
   - Any concerning language or hedging?

5. **Red Flags**
   - Anything that signals trouble
   - Unusual accounting or one-time items
   - Declining metrics management is downplaying

Provide specific numbers and quotes where possible.

MD&A SECTION:
{content}""",

    "yoy_comparison": """Compare these two annual reports and identify what has CHANGED:

1. **Business Evolution**
   - New products, services, or markets
   - Discontinued or declining segments
   - Strategic pivots

2. **Competitive Landscape Changes**
   - New competitors mentioned
   - Changes in competitive positioning
   - Market share shifts

3. **Risk Profile Changes**
   - NEW risks added (what emerged?)
   - Risks REMOVED (what was resolved?)
   - Risks that got MORE severe
   - Risks that got LESS severe

4. **Financial Trajectory**
   - Revenue/profit trend direction
   - Margin changes
   - Debt level changes
   - Cash flow changes

5. **Management Changes**
   - Leadership changes
   - Organizational restructuring
   - Tone/sentiment shifts

6. **Overall Assessment**
   - Is the company getting STRONGER or WEAKER? Why?
   - Key concerns that are growing
   - Positive developments

NEWER REPORT (FY{newer_year}):
{newer_content}

OLDER REPORT (FY{older_year}):
{older_content}""",

    "executive_summary": """Based on the analysis of {num_years} annual reports for {symbol}, provide an INVESTMENT-FOCUSED executive summary:

1. **The Business** (2-3 sentences)
   - What do they do? How do they make money?

2. **Business Trajectory**
   - Is the business getting STRONGER or WEAKER over the period?
   - Key evidence supporting this assessment

3. **Major Changes Over {num_years} Years**
   - New products/markets/strategies
   - Competitive position changes
   - Management changes

4. **Key Risks to Monitor**
   - Most concerning risks
   - Risks that are growing
   - Competition threats
   - Debt/financial concerns

5. **Strengths & Advantages**
   - Competitive moat
   - Financial strengths
   - Strategic assets

6. **Investment Verdict**
   - BULL CASE: Why this could be a good investment
   - BEAR CASE: Why this could be a bad investment
   - KEY QUESTION: What would you want to know more about?

Analysis Data:
{analysis_data}"""
}

# ============================================
# MULTI-AGENT SYSTEM - 10 Specialized Agents
# ============================================

SPECIALIZED_AGENTS = {
    "business_model_analyst": {
        "name": "Business Model Analyst",
        "emoji": "🏢",
        "focus": "business",
        "prompt": """You are a Business Model Analyst specializing in corporate strategy.
Analyze this company's 10-K Business section focusing on:
- Core business model: How do they make money?
- Revenue streams and their relative importance
- Key products/services and their evolution
- Customer segments and concentration
- Geographic footprint and expansion
- Business model changes or pivots over time
- Vertical integration and value chain position

Be specific about changes year-over-year. Flag any concerning shifts in the business model.
Provide a BUSINESS MODEL STRENGTH rating: STRONG / MODERATE / WEAK with justification.

10-K BUSINESS SECTION:
{content}"""
    },
    "risk_deep_dive_analyst": {
        "name": "Risk Deep Dive Analyst",
        "emoji": "⚠️",
        "focus": "risk_factors",
        "prompt": """You are a Risk Analyst specializing in corporate risk assessment from 10-K filings.
Analyze the Risk Factors section to identify:

1. **TOP 5 CRITICAL RISKS** - Most material threats to the business
2. **EMERGING RISKS** - New risks that appeared or intensified
3. **COMPETITIVE RISKS** - Threats from competition, disruption, market shifts
4. **FINANCIAL RISKS** - Debt, liquidity, covenant, currency, interest rate
5. **OPERATIONAL RISKS** - Supply chain, key person, concentration, cyber
6. **REGULATORY RISKS** - Legal, compliance, government action
7. **EXISTENTIAL RISKS** - Could any risk kill the company?

Rate each major risk category: LOW / MEDIUM / HIGH / CRITICAL
Provide an OVERALL RISK RATING: LOW / MODERATE / ELEVATED / HIGH

10-K RISK FACTORS SECTION:
{content}"""
    },
    "financial_health_analyst": {
        "name": "Financial Health Analyst",
        "emoji": "📊",
        "focus": "mda",
        "prompt": """You are a Senior Financial Analyst reviewing the MD&A section of a 10-K.
Analyze financial health and performance:

1. **REVENUE ANALYSIS**
   - Growth rates and trends (accelerating/decelerating)
   - Revenue quality and sustainability
   - Segment performance breakdown

2. **PROFITABILITY**
   - Gross margin trends
   - Operating margin trends
   - Net margin and earnings quality

3. **CASH FLOW HEALTH**
   - Operating cash flow vs net income
   - Free cash flow generation
   - Cash conversion quality

4. **BALANCE SHEET STRENGTH**
   - Debt levels and leverage ratios
   - Liquidity position (current ratio, quick ratio)
   - Working capital trends

5. **CAPITAL ALLOCATION**
   - CapEx investments
   - M&A activity
   - Buybacks and dividends

Provide specific numbers where available.
FINANCIAL HEALTH RATING: EXCELLENT / GOOD / FAIR / WEAK / DISTRESSED

10-K MD&A SECTION:
{content}"""
    },
    "competitive_position_analyst": {
        "name": "Competitive Position Analyst",
        "emoji": "🎯",
        "focus": "business",
        "prompt": """You are a Competitive Intelligence Analyst from a top strategy consulting firm.
Analyze competitive position from the 10-K:

1. **MARKET POSITION**
   - Market share claims or implied position
   - Industry ranking and standing
   - Geographic leadership positions

2. **COMPETITIVE MOAT**
   - Sources of competitive advantage
   - Brand strength and recognition
   - Network effects or platform advantages
   - Switching costs for customers
   - Scale advantages
   - Intellectual property / patents

3. **COMPETITIVE THREATS**
   - Named competitors and their strength
   - New entrants or disruptors mentioned
   - Technology threats
   - Pricing pressure indicators

4. **MOAT DURABILITY**
   - How sustainable are advantages?
   - What could erode the moat?

MOAT RATING: WIDE / NARROW / NONE
COMPETITIVE POSITION: DOMINANT / STRONG / MODERATE / WEAK / VULNERABLE

10-K BUSINESS SECTION:
{content}"""
    },
    "management_governance_analyst": {
        "name": "Management & Governance Analyst",
        "emoji": "👔",
        "focus": "business",
        "prompt": """You are a Corporate Governance Analyst evaluating management from the 10-K.
Analyze management and governance signals:

1. **LEADERSHIP**
   - Key executives mentioned and their roles
   - Any leadership changes indicated
   - Management structure and organization

2. **GOVERNANCE SIGNALS**
   - Board composition hints
   - Control structure (founder-led, PE-backed, etc.)
   - Related party transactions mentioned

3. **MANAGEMENT TONE**
   - Confidence vs caution in language
   - Transparency level
   - Forward guidance quality

4. **CAPITAL ALLOCATION PHILOSOPHY**
   - Investment priorities mentioned
   - Dividend/buyback philosophy
   - M&A approach

5. **RED FLAGS**
   - Excessive executive compensation mentions
   - Related party concerns
   - Governance weaknesses

MANAGEMENT QUALITY GRADE: A / B / C / D / F

10-K BUSINESS/MD&A SECTIONS:
{content}"""
    },
    "strategy_analyst": {
        "name": "Strategy Analyst",
        "emoji": "🧭",
        "focus": "business",
        "prompt": """You are a Strategy Analyst evaluating corporate strategy from the 10-K.
Analyze strategic direction and initiatives:

1. **STRATEGIC PRIORITIES**
   - Key growth initiatives mentioned
   - Investment areas emphasized
   - Strategic focus areas

2. **INNOVATION & R&D**
   - New product development
   - Technology investments
   - Innovation pipeline signals

3. **GROWTH STRATEGY**
   - Organic growth drivers
   - M&A strategy
   - Geographic expansion plans

4. **TRANSFORMATION**
   - Digital transformation efforts
   - Business model evolution
   - Restructuring or pivot signals

5. **STRATEGIC RISKS**
   - Execution challenges
   - Strategy gaps
   - Competitive response risks

STRATEGIC DIRECTION: CLEAR & COMPELLING / REASONABLE / UNCLEAR / CONCERNING

10-K BUSINESS SECTION:
{content}"""
    },
    "debt_credit_analyst": {
        "name": "Debt & Credit Analyst",
        "emoji": "💳",
        "focus": "mda",
        "prompt": """You are a Credit Analyst evaluating debt and credit risk from the 10-K.
Analyze debt and credit factors:

1. **DEBT PROFILE**
   - Total debt levels mentioned
   - Debt maturity schedule
   - Interest rate exposure (fixed vs floating)
   - Covenant compliance

2. **LIQUIDITY**
   - Cash position
   - Credit facility availability
   - Working capital adequacy
   - Near-term obligations

3. **LEVERAGE CONCERNS**
   - Debt/EBITDA or similar ratios
   - Interest coverage
   - Debt service ability

4. **REFINANCING RISK**
   - Upcoming maturities
   - Market access
   - Rating agency concerns

5. **CREDIT TRAJECTORY**
   - Is credit profile improving or deteriorating?
   - Deleveraging plans

CREDIT RISK RATING: MINIMAL / LOW / MODERATE / HIGH / DISTRESSED
LIQUIDITY RATING: STRONG / ADEQUATE / TIGHT / STRESSED

10-K MD&A SECTION:
{content}"""
    },
    "esg_analyst": {
        "name": "ESG Analyst",
        "emoji": "🌱",
        "focus": "business",
        "prompt": """You are an ESG (Environmental, Social, Governance) Analyst reviewing the 10-K.
Analyze ESG factors and disclosures:

1. **ENVIRONMENTAL**
   - Climate/emissions mentions
   - Environmental regulations impact
   - Sustainability initiatives
   - Environmental liabilities

2. **SOCIAL**
   - Employee count and human capital
   - Labor practices and relations
   - Diversity mentions
   - Community impact
   - Product safety

3. **GOVERNANCE**
   - Board structure mentions
   - Executive compensation
   - Shareholder rights
   - Ethics and compliance

4. **ESG RISKS**
   - Material ESG risks for this industry
   - Regulatory ESG pressures
   - Reputational risks

5. **ESG TRAJECTORY**
   - Improving or declining ESG focus?

ESG GRADE: A / B / C / D / F (relative to industry)

10-K SECTIONS:
{content}"""
    },
    "red_flag_analyst": {
        "name": "Red Flag Analyst",
        "emoji": "🚩",
        "focus": "mda",
        "prompt": """You are a Forensic Accounting Analyst looking for red flags in the 10-K.
Identify warning signs and concerns:

1. **ACCOUNTING RED FLAGS**
   - Revenue recognition concerns
   - Unusual accounting policies
   - One-time items and adjustments
   - Aggressive assumptions

2. **FINANCIAL RED FLAGS**
   - Cash flow vs earnings divergence
   - Receivables/inventory buildup
   - Related party transactions
   - Off-balance sheet items

3. **OPERATIONAL RED FLAGS**
   - Customer concentration
   - Key person dependencies
   - Supply chain vulnerabilities
   - Execution problems

4. **DISCLOSURE RED FLAGS**
   - Vague or evasive language
   - Missing information
   - Inconsistencies
   - Management excuses

5. **TRAJECTORY CONCERNS**
   - Deteriorating trends
   - Guidance misses
   - Strategy pivots that seem desperate

RED FLAG SEVERITY: NONE / MINOR / MODERATE / SIGNIFICANT / SEVERE
List specific red flags found with evidence.

10-K MD&A SECTION:
{content}"""
    },
    "product_segment_analyst": {
        "name": "Product & Segment Analyst",
        "emoji": "📦",
        "focus": "all",
        "prompt": """You are a Product & Segment Analyst specializing in dissecting company revenue streams and product performance from 10-K filings.

Your analysis MUST be SPECIFIC - name actual products, services, brands, and segments. No generic statements.

**SECTOR-SPECIFIC ANALYSIS REQUIREMENTS:**

**TECHNOLOGY COMPANIES:**
- Name specific products/services (e.g., iPhone, AWS, Azure, Google Cloud)
- Revenue and growth by product line
- Which products are growing vs declining
- New product launches and discontinued products
- Subscription vs one-time revenue breakdown

**CONSUMER/RETAIL COMPANIES:**
- Name specific brands, product lines, store formats
- Same-store sales and comparable sales trends
- Top-performing vs underperforming categories
- New product introductions and discontinued lines
- E-commerce vs physical store performance
- Customer demographics and spending patterns

**MEDICAL TECHNOLOGY/DEVICES:**
- Name SPECIFIC devices and equipment
- Revenue by device category
- FDA approvals and clearances mentioned
- Pipeline of new devices in development
- Reimbursement and pricing trends
- Competitive device comparisons

**BIOTECH/PHARMACEUTICALS:**
- List ALL approved drugs with revenue figures
- Pipeline drugs by development phase (Phase 1/2/3, NDA)
- Patent expiration dates for key drugs
- Biosimilar/generic competition threats
- R&D spending and success rates
- Licensing deals and partnerships

**FINANCIAL SERVICES:**
- Revenue by business line (trading, advisory, lending, etc.)
- Net interest margin and spread trends
- Assets under management growth
- Loan portfolio composition and quality
- Fee income breakdown

**INDUSTRIAL/MANUFACTURING:**
- Revenue by product segment
- Backlog and order trends
- Capacity utilization
- Key contracts won/lost
- Supply chain and input cost trends

**PROVIDE FOR EACH MAJOR PRODUCT/SEGMENT:**
1. **Name** - Specific product/service/brand name
2. **Revenue** - Dollar amount and % of total revenue
3. **Growth** - YoY growth rate (accelerating/decelerating)
4. **Margins** - Gross/operating margin if disclosed
5. **Outlook** - Growing, stable, or declining? Why?

**WINNERS vs LOSERS:**
- Which products/segments are OUTPERFORMING? Why?
- Which products/segments are UNDERPERFORMING? Why?
- Any products being discontinued or de-emphasized?

**PRODUCT PIPELINE:**
- What new products/services are coming?
- Expected launch timing
- Investment levels in new products

Be SPECIFIC. Use actual names, numbers, and quotes from the filing.

10-K CONTENT:
{content}"""
    },
    "investment_strategist": {
        "name": "Investment Strategist",
        "emoji": "💼",
        "focus": "all",
        "prompt": """You are a Chief Investment Strategist synthesizing the 10-K analysis.
Provide investment thesis based on all sections:

1. **COMPANY SUMMARY** (2-3 sentences)
   - What do they do and how do they make money?

2. **BULL CASE** (5 key points)
   - Why this could be a winning investment
   - Upside catalysts and opportunities

3. **BEAR CASE** (5 key points)
   - Why this could be a losing investment
   - Downside risks and concerns

4. **KEY METRICS TO WATCH**
   - What numbers should investors monitor?

5. **CRITICAL QUESTIONS**
   - What would you want to investigate further?

6. **INVESTMENT VERDICT**
   - BUY / HOLD / SELL recommendation
   - Conviction level: HIGH / MEDIUM / LOW
   - Key assumption for thesis

10-K ANALYSIS DATA:
{content}"""
    }
}


def get_agent_content(report: 'AnnualReport', agent_focus: str) -> str:
    """Get relevant content for an agent based on its focus area."""
    if agent_focus == "business":
        return report.sections.get("business", "")[:40000]
    elif agent_focus == "risk_factors":
        return report.sections.get("risk_factors", "")[:40000]
    elif agent_focus == "mda":
        return report.sections.get("mda", "")[:40000]
    elif agent_focus == "all":
        # Combine key sections for strategist
        parts = []
        if report.sections.get("business"):
            parts.append(f"BUSINESS:\n{report.sections['business'][:15000]}")
        if report.sections.get("risk_factors"):
            parts.append(f"RISK FACTORS:\n{report.sections['risk_factors'][:15000]}")
        if report.sections.get("mda"):
            parts.append(f"MD&A:\n{report.sections['mda'][:15000]}")
        return "\n\n".join(parts)
    return ""


@dataclass
class AnnualReport:
    """Data class representing a single annual report (10-K filing)"""
    symbol: str
    filing_date: str
    fiscal_year: str
    filing_url: str
    accepted_date: str = ""
    cik: str = ""
    form_type: str = "10-K"
    content: str = ""
    sections: Dict[str, str] = field(default_factory=dict)

    def __repr__(self):
        return f"AnnualReport({self.symbol}, FY{self.fiscal_year}, filed {self.filing_date})"


class AnnualReportAnalyzer:
    """
    Fetches and analyzes annual reports (10-K filings) for a given company.
    Supports both FMP API and direct SEC EDGAR access.
    """

    def __init__(self, api_key: str = None, enable_ai: bool = True):
        """
        Initialize the analyzer.

        Args:
            api_key: FMP API key. If not provided, will try to load from environment.
            enable_ai: Whether to enable AI analysis (default: True)
        """
        self.api_key = api_key or FMP_API_KEY
        if not self.api_key:
            logger.warning("No FMP API key provided. Some features may be limited.")

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'AnnualReportAnalyzer/1.0 (Contact: research@example.com)'
        })

        # Initialize AI clients
        self.enable_ai = enable_ai
        self.anthropic_client = None
        self.openai_client = None

        if enable_ai:
            self._init_ai_clients()

    # =========================================================================
    # AI Client Initialization
    # =========================================================================

    def _init_ai_clients(self):
        """Initialize AI clients (Anthropic Claude and OpenAI GPT)."""
        # Try Anthropic first
        if ANTHROPIC_AVAILABLE and ANTHROPIC_API_KEY:
            try:
                self.anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                logger.info("Anthropic Claude client initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Anthropic client: {e}")

        # Try OpenAI as fallback
        if OPENAI_AVAILABLE and OPENAI_API_KEY:
            try:
                self.openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
                logger.info("OpenAI GPT client initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize OpenAI client: {e}")

        if not self.anthropic_client and not self.openai_client:
            logger.warning("No AI clients available. AI analysis will be disabled.")
            self.enable_ai = False

    def _call_ai(self, prompt: str, max_tokens: int = 2000) -> str:
        """
        Call AI model (Claude first, GPT as fallback).

        Args:
            prompt: The prompt to send to the AI
            max_tokens: Maximum tokens for response

        Returns:
            AI response text
        """
        # Try Anthropic Claude first
        if self.anthropic_client:
            try:
                message = self.anthropic_client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}]
                )
                return message.content[0].text
            except Exception as e:
                logger.warning(f"Claude API error: {e}, trying OpenAI...")

        # Fallback to OpenAI GPT
        if self.openai_client:
            try:
                response = self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content
            except Exception as e:
                logger.error(f"OpenAI API error: {e}")

        return ""

    # =========================================================================
    # AI Analysis Methods
    # =========================================================================

    def ai_analyze_business(self, report: AnnualReport) -> str:
        """
        Use AI to analyze the Business section of a 10-K.

        Args:
            report: AnnualReport object with sections extracted

        Returns:
            AI analysis of the business section
        """
        if not self.enable_ai:
            return ""

        business_content = report.sections.get("business", "")
        if not business_content:
            logger.warning(f"No business section found for {report}")
            return ""

        # Truncate to fit context window
        content = business_content[:30000]
        prompt = AI_PROMPTS["business_summary"].format(content=content)

        logger.info(f"AI analyzing Business section for {report.symbol} FY{report.fiscal_year}...")
        return self._call_ai(prompt)

    def ai_analyze_risks(self, report: AnnualReport) -> str:
        """
        Use AI to analyze the Risk Factors section of a 10-K.

        Args:
            report: AnnualReport object with sections extracted

        Returns:
            AI analysis of risk factors
        """
        if not self.enable_ai:
            return ""

        risk_content = report.sections.get("risk_factors", "")
        if not risk_content:
            logger.warning(f"No risk factors section found for {report}")
            return ""

        # Truncate to fit context window
        content = risk_content[:30000]
        prompt = AI_PROMPTS["risk_analysis"].format(content=content)

        logger.info(f"AI analyzing Risk Factors for {report.symbol} FY{report.fiscal_year}...")
        return self._call_ai(prompt)

    def ai_analyze_mda(self, report: AnnualReport) -> str:
        """
        Use AI to analyze the MD&A section of a 10-K.

        Args:
            report: AnnualReport object with sections extracted

        Returns:
            AI analysis of MD&A section
        """
        if not self.enable_ai:
            return ""

        mda_content = report.sections.get("mda", "")
        if not mda_content:
            logger.warning(f"No MD&A section found for {report}")
            return ""

        # Truncate to fit context window
        content = mda_content[:30000]
        prompt = AI_PROMPTS["mda_analysis"].format(content=content)

        logger.info(f"AI analyzing MD&A for {report.symbol} FY{report.fiscal_year}...")
        return self._call_ai(prompt)

    def ai_compare_years(self, newer_report: AnnualReport, older_report: AnnualReport) -> str:
        """
        Use AI to compare two annual reports year-over-year.

        Args:
            newer_report: More recent AnnualReport
            older_report: Older AnnualReport

        Returns:
            AI comparison analysis
        """
        if not self.enable_ai:
            return ""

        # Get key sections from both reports (truncated)
        newer_content = self._get_comparison_content(newer_report)
        older_content = self._get_comparison_content(older_report)

        prompt = AI_PROMPTS["yoy_comparison"].format(
            newer_year=newer_report.fiscal_year,
            older_year=older_report.fiscal_year,
            newer_content=newer_content,
            older_content=older_content
        )

        logger.info(f"AI comparing FY{newer_report.fiscal_year} vs FY{older_report.fiscal_year}...")
        return self._call_ai(prompt, max_tokens=2500)

    def _get_comparison_content(self, report: AnnualReport) -> str:
        """Get truncated content from multiple sections for comparison."""
        sections = []

        if report.sections.get("business"):
            sections.append(f"BUSINESS:\n{report.sections['business'][:8000]}")

        if report.sections.get("risk_factors"):
            sections.append(f"RISK FACTORS:\n{report.sections['risk_factors'][:8000]}")

        if report.sections.get("mda"):
            sections.append(f"MD&A:\n{report.sections['mda'][:8000]}")

        return "\n\n".join(sections)

    def ai_generate_executive_summary(self, symbol: str, reports: List[AnnualReport],
                                      analysis_results: Dict[str, Any]) -> str:
        """
        Generate an executive summary using AI based on all analysis.

        Args:
            symbol: Stock ticker
            reports: List of analyzed reports
            analysis_results: Dictionary containing all analysis results

        Returns:
            Executive summary text
        """
        if not self.enable_ai:
            return ""

        # Format analysis data for the prompt
        analysis_data = self._format_analysis_for_summary(analysis_results)

        prompt = AI_PROMPTS["executive_summary"].format(
            num_years=len(reports),
            symbol=symbol,
            analysis_data=analysis_data
        )

        logger.info(f"AI generating executive summary for {symbol}...")
        return self._call_ai(prompt, max_tokens=3000)

    def _format_analysis_for_summary(self, results: Dict[str, Any]) -> str:
        """Format analysis results for the executive summary prompt."""
        formatted = []

        for report_data in results.get("reports", []):
            fy = report_data.get("fiscal_year", "Unknown")
            formatted.append(f"\n=== FY{fy} ===")

            if report_data.get("ai_analysis"):
                ai = report_data["ai_analysis"]
                if ai.get("business_summary"):
                    formatted.append(f"\nBusiness Analysis:\n{ai['business_summary'][:2000]}")
                if ai.get("risk_analysis"):
                    formatted.append(f"\nRisk Analysis:\n{ai['risk_analysis'][:2000]}")
                if ai.get("mda_analysis"):
                    formatted.append(f"\nMD&A Analysis:\n{ai['mda_analysis'][:2000]}")

        if results.get("yoy_comparisons"):
            formatted.append("\n=== Year-over-Year Changes ===")
            for comparison in results["yoy_comparisons"]:
                if isinstance(comparison, dict):
                    formatted.append(f"\n{comparison.get('years', '')}:")
                    formatted.append(comparison.get('analysis', '')[:2000])
                else:
                    formatted.append(str(comparison)[:2000])

        return "\n".join(formatted)

    def run_ai_analysis_parallel(self, report: AnnualReport) -> Dict[str, str]:
        """
        Run all AI analyses for a single report in parallel.

        Args:
            report: AnnualReport object with sections extracted

        Returns:
            Dictionary with all AI analysis results
        """
        if not self.enable_ai:
            return {}

        results = {}

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(self.ai_analyze_business, report): "business_summary",
                executor.submit(self.ai_analyze_risks, report): "risk_analysis",
                executor.submit(self.ai_analyze_mda, report): "mda_analysis"
            }

            for future in as_completed(futures):
                analysis_type = futures[future]
                try:
                    results[analysis_type] = future.result()
                except Exception as e:
                    logger.error(f"Error in {analysis_type}: {e}")
                    results[analysis_type] = f"Error: {str(e)}"

        return results

    # =========================================================================
    # Multi-Agent Analysis Methods
    # =========================================================================

    def run_single_agent(self, agent_id: str, report: AnnualReport) -> Dict[str, Any]:
        """
        Run a single specialized agent on a report.

        Args:
            agent_id: ID of the agent from SPECIALIZED_AGENTS
            report: AnnualReport object with sections extracted

        Returns:
            Dictionary with agent analysis results
        """
        if agent_id not in SPECIALIZED_AGENTS:
            logger.error(f"Unknown agent: {agent_id}")
            return {"error": f"Unknown agent: {agent_id}"}

        agent = SPECIALIZED_AGENTS[agent_id]
        content = get_agent_content(report, agent["focus"])

        if not content:
            logger.warning(f"No content available for {agent['name']}")
            return {
                "agent_id": agent_id,
                "agent_name": agent["name"],
                "emoji": agent["emoji"],
                "analysis": "No relevant content found in this report."
            }

        # Fill in the prompt template
        prompt = agent["prompt"].format(content=content)

        logger.info(f"{agent['emoji']} Running {agent['name']}...")

        try:
            analysis = self._call_ai(prompt, max_tokens=2500)
            return {
                "agent_id": agent_id,
                "agent_name": agent["name"],
                "emoji": agent["emoji"],
                "analysis": analysis
            }
        except Exception as e:
            logger.error(f"Error in {agent['name']}: {e}")
            return {
                "agent_id": agent_id,
                "agent_name": agent["name"],
                "emoji": agent["emoji"],
                "error": str(e)
            }

    def run_all_agents_parallel(self, report: AnnualReport,
                                max_workers: int = 5) -> Dict[str, Dict[str, Any]]:
        """
        Run all specialized agents in parallel on a single report.

        Args:
            report: AnnualReport object with sections extracted
            max_workers: Maximum concurrent agent threads (default: 5)

        Returns:
            Dictionary mapping agent_id to analysis results
        """
        if not self.enable_ai:
            return {}

        results = {}
        agent_ids = list(SPECIALIZED_AGENTS.keys())

        logger.info(f"\n{'='*60}")
        logger.info(f"Running {len(agent_ids)} specialized agents for FY{report.fiscal_year}")
        logger.info(f"{'='*60}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.run_single_agent, agent_id, report): agent_id
                for agent_id in agent_ids
            }

            for future in as_completed(futures):
                agent_id = futures[future]
                try:
                    results[agent_id] = future.result()
                except Exception as e:
                    logger.error(f"Error running agent {agent_id}: {e}")
                    results[agent_id] = {"error": str(e)}

        logger.info(f"Completed all {len(results)} agent analyses")
        return results

    def run_agents_for_all_reports(self, reports: List[AnnualReport],
                                   max_workers: int = 5) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Run all agents on multiple reports.

        Args:
            reports: List of AnnualReport objects
            max_workers: Maximum concurrent agent threads per report

        Returns:
            Dictionary mapping fiscal_year to agent results
        """
        all_results = {}

        for report in reports:
            if report.content and report.sections:
                fy_key = f"FY{report.fiscal_year}"
                all_results[fy_key] = self.run_all_agents_parallel(report, max_workers)

        return all_results

    # =========================================================================
    # FMP API Methods
    # =========================================================================

    def fetch_annual_reports_fmp(self, symbol: str, num_reports: int = 3) -> List[AnnualReport]:
        """
        Fetch annual reports (10-K filings) using FMP API.

        Args:
            symbol: Stock ticker symbol (e.g., 'AAPL')
            num_reports: Number of annual reports to fetch (default: 3)

        Returns:
            List of AnnualReport objects
        """
        if not self.api_key:
            logger.error("FMP API key required for this method")
            return []

        logger.info(f"Fetching last {num_reports} annual reports for {symbol} via FMP...")

        # Fetch SEC filings metadata from FMP
        url = f"{FMP_BASE_URL}/sec_filings/{symbol}"
        params = {
            "type": "10-K",
            "limit": num_reports * 2,  # Get extra in case some are 10-K/A amendments
            "apikey": self.api_key
        }

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            filings = response.json()

            if isinstance(filings, dict) and "Error Message" in filings:
                logger.error(f"FMP API Error: {filings['Error Message']}")
                return []

            if not filings:
                logger.warning(f"No 10-K filings found for {symbol}")
                return []

            # Filter for 10-K only (not 10-K/A amendments) and get required count
            annual_reports = []
            for filing in filings:
                if filing.get("type") == "10-K" and len(annual_reports) < num_reports:
                    report = AnnualReport(
                        symbol=symbol.upper(),
                        filing_date=filing.get("fillingDate", filing.get("filingDate", "")),
                        fiscal_year=self._extract_fiscal_year(filing.get("fillingDate", "")),
                        filing_url=filing.get("finalLink", ""),
                        accepted_date=filing.get("acceptedDate", ""),
                        cik=filing.get("cik", ""),
                        form_type="10-K"
                    )
                    annual_reports.append(report)

            logger.info(f"Found {len(annual_reports)} annual reports for {symbol}")
            return annual_reports

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching filings from FMP: {e}")
            return []

    def fetch_report_content_fmp(self, report: AnnualReport, max_chars: int = 200000) -> str:
        """
        Fetch the actual content of an annual report.

        Args:
            report: AnnualReport object with filing_url
            max_chars: Maximum characters to fetch (default: 200000)

        Returns:
            Report content as string
        """
        if not report.filing_url:
            logger.warning("No filing URL available for this report")
            return ""

        logger.info(f"Fetching content for {report.symbol} FY{report.fiscal_year}...")

        try:
            response = self.session.get(report.filing_url, timeout=90)
            response.raise_for_status()

            # Parse HTML and extract text
            soup = BeautifulSoup(response.content, 'html.parser')

            # Remove script, style, and XBRL-specific elements
            for element in soup(['script', 'style', 'meta', 'link', 'head']):
                element.decompose()

            # Try to find the main document body
            # Modern SEC iXBRL filings have the content in specific containers
            main_content = None

            # Try common SEC filing containers
            for selector in ['body', 'div.body', '#main-content', '.document']:
                main_content = soup.select_one(selector)
                if main_content:
                    break

            if not main_content:
                main_content = soup

            # Extract text while preserving some structure
            # Remove hidden elements and XBRL tags that don't contribute to readable text
            for element in main_content.find_all(attrs={'style': lambda x: x and 'display:none' in str(x).lower()}):
                element.decompose()

            # Get text content - use space separator for inline elements
            text_parts = []
            for element in main_content.descendants:
                if isinstance(element, str):
                    text = element.strip()
                    if text and not text.startswith('http://') and not text.startswith('false') and len(text) > 2:
                        # Skip XBRL namespace declarations and boolean values
                        if not re.match(r'^(true|false|P\d+[YMD]|[\d.]+|http[s]?://)', text):
                            text_parts.append(text)

            text = '\n'.join(text_parts)

            # Clean up: remove multiple newlines and spaces
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            text = re.sub(r'^\s*\n', '', text, flags=re.MULTILINE)

            # If we got mostly gibberish (XBRL), try an alternative approach
            if len(text) < 10000 or 'ITEM 1' not in text.upper():
                logger.info("Trying alternative text extraction...")
                text = main_content.get_text(separator='\n', strip=True)
                text = re.sub(r'\n{3,}', '\n\n', text)
                text = re.sub(r' {2,}', ' ', text)

            report.content = text[:max_chars]
            logger.info(f"Fetched {len(report.content):,} characters of content")
            return report.content

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching report content: {e}")
            return ""

    # =========================================================================
    # SEC EDGAR Direct Methods
    # =========================================================================

    def fetch_annual_reports_sec(self, symbol: str, num_reports: int = 3) -> List[AnnualReport]:
        """
        Fetch annual reports directly from SEC EDGAR.

        Args:
            symbol: Stock ticker symbol (e.g., 'AAPL')
            num_reports: Number of annual reports to fetch (default: 3)

        Returns:
            List of AnnualReport objects
        """
        logger.info(f"Fetching last {num_reports} annual reports for {symbol} via SEC EDGAR...")

        # First, get the company's CIK number
        cik = self._get_cik_from_ticker(symbol)
        if not cik:
            logger.error(f"Could not find CIK for {symbol}")
            return []

        # Fetch filings from SEC EDGAR
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            filings = data.get("filings", {}).get("recent", {})
            if not filings:
                logger.warning(f"No filings found for {symbol}")
                return []

            # Extract 10-K filings
            annual_reports = []
            forms = filings.get("form", [])
            dates = filings.get("filingDate", [])
            accession_numbers = filings.get("accessionNumber", [])
            primary_docs = filings.get("primaryDocument", [])

            for i, form in enumerate(forms):
                if form == "10-K" and len(annual_reports) < num_reports:
                    accession = accession_numbers[i].replace("-", "")
                    filing_url = f"{SEC_EDGAR_BASE}/Archives/edgar/data/{cik}/{accession}/{primary_docs[i]}"

                    report = AnnualReport(
                        symbol=symbol.upper(),
                        filing_date=dates[i],
                        fiscal_year=self._extract_fiscal_year(dates[i]),
                        filing_url=filing_url,
                        cik=cik,
                        form_type="10-K"
                    )
                    annual_reports.append(report)

            logger.info(f"Found {len(annual_reports)} annual reports for {symbol}")
            return annual_reports

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching from SEC EDGAR: {e}")
            return []

    def _get_cik_from_ticker(self, symbol: str) -> Optional[str]:
        """
        Get the CIK number for a ticker symbol from SEC.

        Args:
            symbol: Stock ticker symbol

        Returns:
            CIK number as string (zero-padded to 10 digits) or None
        """
        url = "https://www.sec.gov/cgi-bin/browse-edgar"
        params = {
            "action": "getcompany",
            "company": symbol,
            "type": "10-K",
            "dateb": "",
            "owner": "include",
            "count": "1",
            "output": "atom"
        }

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()

            # Parse XML response to find CIK
            match = re.search(r'CIK=(\d+)', response.text)
            if match:
                cik = match.group(1).zfill(10)
                logger.info(f"Found CIK {cik} for {symbol}")
                return cik

            # Try the company tickers JSON file
            tickers_url = "https://www.sec.gov/files/company_tickers.json"
            response = self.session.get(tickers_url, timeout=15)
            response.raise_for_status()
            tickers_data = response.json()

            for entry in tickers_data.values():
                if entry.get("ticker", "").upper() == symbol.upper():
                    cik = str(entry.get("cik_str", "")).zfill(10)
                    logger.info(f"Found CIK {cik} for {symbol}")
                    return cik

            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting CIK: {e}")
            return None

    # =========================================================================
    # Content Analysis Methods
    # =========================================================================

    def extract_sections(self, report: AnnualReport) -> Dict[str, str]:
        """
        Extract key sections from a 10-K annual report.

        Standard 10-K sections:
        - Item 1: Business
        - Item 1A: Risk Factors
        - Item 2: Properties
        - Item 3: Legal Proceedings
        - Item 5: Market for Common Equity
        - Item 6: Selected Financial Data (removed in 2021)
        - Item 7: MD&A (Management's Discussion and Analysis)
        - Item 7A: Quantitative and Qualitative Disclosures About Market Risk
        - Item 8: Financial Statements

        Args:
            report: AnnualReport object with content loaded

        Returns:
            Dictionary mapping section names to content
        """
        if not report.content:
            logger.warning("Report content not loaded. Call fetch_report_content first.")
            return {}

        sections = {}
        content = report.content

        # Normalize content - replace non-breaking spaces and clean up
        content = content.replace('\xa0', ' ')
        content = re.sub(r'[ \t]+', ' ', content)

        # Define section patterns - more flexible to handle various SEC filing formats
        # Pattern: Look for ITEM X header, capture until next ITEM or end
        # Using [\s\S] instead of . for multiline matching
        section_patterns = {
            "business": [
                # Match "Item 1.    Business" followed by actual content (skip TOC)
                r"Item\s*1\.?\s+Business\s*\n+(?:Company\s+Background|The\s+Company|[A-Z][a-z]+\s+Inc)([\s\S]*?)(?=Item\s*1A|Item\s*2|\Z)",
                r"Item\s*1\.?\s+Business\s*\n+([\s\S]{500,}?)(?=Item\s*1A|Item\s*2|\Z)",
            ],
            "risk_factors": [
                r"Item\s*1A\.?\s+Risk\s*Factors?\s*\n+([\s\S]*?)(?=Item\s*1B|Item\s*1C|Item\s*2|\Z)",
            ],
            "mda": [
                r"Item\s*7\.?\s+Management['\u2019]?s?\s*Discussion\s*(?:and\s*Analysis)?\s*(?:of\s*Financial\s*Condition)?([\s\S]*?)(?=Item\s*7A|Item\s*8|\Z)",
                r"Management['\u2019]s\s*Discussion\s*and\s*Analysis\s*of\s*Financial\s*Condition([\s\S]*?)(?=Item\s*7A|Item\s*8|\Z)",
            ],
            "properties": [
                r"Item\s*2\.?\s+Properties\s*\n+([\s\S]*?)(?=Item\s*3|\Z)"
            ],
            "legal_proceedings": [
                r"Item\s*3\.?\s+Legal\s*Proceedings\s*\n+([\s\S]*?)(?=Item\s*4|\Z)"
            ],
            "market_risk": [
                r"Item\s*7A\.?\s+Quant(?:itative)?\s*(?:and\s*Qualitative)?([\s\S]*?)(?=Item\s*8|\Z)"
            ],
        }

        for section_name, patterns in section_patterns.items():
            for pattern in patterns:
                matches = list(re.finditer(pattern, content, re.IGNORECASE))
                # Take the last match if multiple (first one is usually TOC)
                if matches:
                    for match in reversed(matches):
                        section_text = match.group(1).strip()
                        # Only keep if we got meaningful content (more than 500 chars)
                        if len(section_text) > 500:
                            sections[section_name] = section_text[:50000]
                            logger.debug(f"Extracted {section_name}: {len(sections[section_name]):,} chars")
                            break
                    if section_name in sections:
                        break  # Found a match, move to next section

        report.sections = sections
        logger.info(f"Extracted {len(sections)} sections from {report}")
        return sections

    def get_key_metrics_from_mda(self, report: AnnualReport) -> Dict[str, Any]:
        """
        Extract key metrics mentioned in Management's Discussion and Analysis.

        Args:
            report: AnnualReport object with sections extracted

        Returns:
            Dictionary of extracted metrics
        """
        mda = report.sections.get("mda", "")
        if not mda:
            return {}

        metrics = {}

        # Revenue patterns
        revenue_match = re.search(
            r'(?:total\s+)?(?:net\s+)?revenue[s]?\s+(?:was|were|of|increased|decreased)?\s*\$?([\d,.]+)\s*(billion|million|B|M)?',
            mda, re.IGNORECASE
        )
        if revenue_match:
            metrics["revenue_mentioned"] = revenue_match.group(0)

        # Growth patterns
        growth_match = re.search(
            r'(?:revenue|sales|net\s+sales)\s+(?:grew|increased|decreased)\s+(?:by\s+)?([\d.]+)\s*%',
            mda, re.IGNORECASE
        )
        if growth_match:
            metrics["growth_rate"] = growth_match.group(1) + "%"

        # Net income patterns
        income_match = re.search(
            r'net\s+income\s+(?:was|of|totaled)?\s*\$?([\d,.]+)\s*(billion|million|B|M)?',
            mda, re.IGNORECASE
        )
        if income_match:
            metrics["net_income_mentioned"] = income_match.group(0)

        # Margin patterns
        margin_match = re.search(
            r'(?:gross|operating|net)\s+margin\s+(?:was|of|at)?\s*([\d.]+)\s*%',
            mda, re.IGNORECASE
        )
        if margin_match:
            metrics["margin_mentioned"] = margin_match.group(0)

        return metrics

    def get_risk_summary(self, report: AnnualReport, max_risks: int = 10) -> List[str]:
        """
        Extract a summary of key risks from Risk Factors section.

        Args:
            report: AnnualReport object with sections extracted
            max_risks: Maximum number of risks to extract

        Returns:
            List of risk factor headings/summaries
        """
        risks_section = report.sections.get("risk_factors", "")
        if not risks_section:
            return []

        risks = []

        # Pattern for risk headings (typically bold or all caps)
        # Look for lines that appear to be risk titles
        lines = risks_section.split('\n')

        for line in lines:
            line = line.strip()
            # Skip short lines and pure numbers
            if len(line) < 20 or len(line) > 300:
                continue

            # Risk headings often contain keywords like "risk", "may", "could", "might"
            risk_keywords = ['risk', 'may adversely', 'could harm', 'might affect',
                            'uncertainty', 'competition', 'regulatory', 'litigation']

            if any(keyword in line.lower() for keyword in risk_keywords):
                # Clean up the line
                clean_line = re.sub(r'^[\d.\-•*]+\s*', '', line)
                if clean_line and clean_line not in risks:
                    risks.append(clean_line)
                    if len(risks) >= max_risks:
                        break

        return risks

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def _extract_fiscal_year(self, filing_date: str) -> str:
        """Extract fiscal year from filing date."""
        if not filing_date:
            return ""
        try:
            # Most 10-Ks are filed Q1 of the following year
            # So a filing in Feb 2024 is for FY2023
            date = datetime.strptime(filing_date[:10], "%Y-%m-%d")
            # If filed in Q1 (Jan-Mar), it's usually for prior year
            if date.month <= 3:
                return str(date.year - 1)
            return str(date.year)
        except ValueError:
            return filing_date[:4] if len(filing_date) >= 4 else ""

    def compare_reports(self, reports: List[AnnualReport]) -> Dict[str, Any]:
        """
        Compare key elements across multiple annual reports.

        Args:
            reports: List of AnnualReport objects (should be 2-3 reports)

        Returns:
            Dictionary with comparison data
        """
        if len(reports) < 2:
            return {"error": "Need at least 2 reports to compare"}

        comparison = {
            "reports_compared": [f"FY{r.fiscal_year}" for r in reports],
            "risk_changes": [],
            "business_evolution": []
        }

        # Compare risk factors between years
        if len(reports) >= 2:
            newer_risks = set(self.get_risk_summary(reports[0]))
            older_risks = set(self.get_risk_summary(reports[1]))

            comparison["new_risks"] = list(newer_risks - older_risks)
            comparison["removed_risks"] = list(older_risks - newer_risks)

        return comparison

    # =========================================================================
    # Main Entry Point
    # =========================================================================

    def analyze_company(self, symbol: str, num_reports: int = 3,
                       use_sec_fallback: bool = True,
                       run_ai_analysis: bool = True,
                       run_multi_agent: bool = True) -> Dict[str, Any]:
        """
        Main method to fetch and analyze annual reports for a company.

        Args:
            symbol: Stock ticker symbol (e.g., 'AAPL')
            num_reports: Number of annual reports to analyze (default: 3)
            use_sec_fallback: Whether to use SEC EDGAR if FMP fails
            run_ai_analysis: Whether to run AI analysis on reports (default: True)
            run_multi_agent: Whether to run 10 specialized agents on most recent report (default: True)

        Returns:
            Dictionary containing analysis results
        """
        logger.info(f"=" * 60)
        logger.info(f"Starting Annual Report Analysis for {symbol.upper()}")
        logger.info(f"AI Analysis: {'Enabled' if run_ai_analysis and self.enable_ai else 'Disabled'}")
        logger.info(f"Multi-Agent Analysis: {'Enabled' if run_multi_agent and self.enable_ai else 'Disabled'}")
        logger.info(f"=" * 60)

        results = {
            "symbol": symbol.upper(),
            "analysis_date": datetime.now().isoformat(),
            "reports": [],
            "yoy_comparisons": [],
            "agent_analysis": {},  # Multi-agent results
            "executive_summary": "",
            "summary": {},
            "errors": []
        }

        # Fetch reports (try FMP first, then SEC EDGAR)
        reports = self.fetch_annual_reports_fmp(symbol, num_reports)

        if not reports and use_sec_fallback:
            logger.info("FMP returned no results, trying SEC EDGAR...")
            reports = self.fetch_annual_reports_sec(symbol, num_reports)

        if not reports:
            results["errors"].append("Could not fetch annual reports from any source")
            return results

        # Store report objects for later use
        report_objects = []

        # Process each report
        for report in reports:
            logger.info(f"\nProcessing {report}...")

            # Fetch content
            self.fetch_report_content_fmp(report)

            if report.content:
                # Extract sections
                self.extract_sections(report)

                # Extract metrics from MD&A
                metrics = self.get_key_metrics_from_mda(report)

                # Get risk summary
                risks = self.get_risk_summary(report, max_risks=5)

                report_data = {
                    "fiscal_year": report.fiscal_year,
                    "filing_date": report.filing_date,
                    "filing_url": report.filing_url,
                    "sections_extracted": list(report.sections.keys()),
                    "content_length": len(report.content),
                    "key_metrics": metrics,
                    "top_risks": risks
                }

                # Run AI analysis if enabled
                if run_ai_analysis and self.enable_ai:
                    logger.info(f"Running AI analysis for FY{report.fiscal_year}...")
                    ai_analysis = self.run_ai_analysis_parallel(report)
                    report_data["ai_analysis"] = ai_analysis

                report_objects.append(report)
            else:
                report_data = {
                    "fiscal_year": report.fiscal_year,
                    "filing_date": report.filing_date,
                    "filing_url": report.filing_url,
                    "error": "Could not fetch report content"
                }

            results["reports"].append(report_data)

        # Generate year-over-year AI comparisons
        if run_ai_analysis and self.enable_ai and len(report_objects) >= 2:
            logger.info("\nGenerating year-over-year comparisons...")
            for i in range(len(report_objects) - 1):
                comparison = self.ai_compare_years(report_objects[i], report_objects[i + 1])
                if comparison:
                    results["yoy_comparisons"].append({
                        "years": f"FY{report_objects[i].fiscal_year} vs FY{report_objects[i+1].fiscal_year}",
                        "analysis": comparison
                    })

        # Run multi-agent analysis on most recent report
        if run_multi_agent and self.enable_ai and report_objects:
            logger.info("\nRunning multi-agent analysis on most recent report...")
            most_recent = report_objects[0]  # Reports are in reverse chronological order
            results["agent_analysis"] = self.run_all_agents_parallel(most_recent, max_workers=5)

        # Generate executive summary
        if run_ai_analysis and self.enable_ai and report_objects:
            logger.info("\nGenerating executive summary...")
            results["executive_summary"] = self.ai_generate_executive_summary(
                symbol, report_objects, results
            )

        # Generate summary comparison if we have multiple reports
        if len(reports) >= 2:
            results["summary"]["comparison"] = self.compare_reports(reports)

        results["summary"]["total_reports"] = len(reports)
        results["summary"]["years_covered"] = [r.fiscal_year for r in reports]
        results["summary"]["ai_analysis_enabled"] = run_ai_analysis and self.enable_ai
        results["summary"]["multi_agent_enabled"] = run_multi_agent and self.enable_ai
        results["summary"]["agents_count"] = len(results.get("agent_analysis", {}))

        logger.info(f"\n{'=' * 60}")
        logger.info(f"Analysis complete for {symbol.upper()}")
        logger.info(f"{'=' * 60}")

        return results


# =============================================================================
# Standalone Usage
# =============================================================================

def main():
    """Main function for standalone usage."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Analyze annual reports (10-K filings) with AI")
    parser.add_argument("symbol", help="Stock ticker symbol (e.g., AAPL)")
    parser.add_argument("-n", "--num-reports", type=int, default=3,
                       help="Number of annual reports to analyze (default: 3)")
    parser.add_argument("-o", "--output", help="Output file path (JSON)")
    parser.add_argument("-v", "--verbose", action="store_true",
                       help="Enable verbose logging")
    parser.add_argument("--no-ai", action="store_true",
                       help="Disable AI analysis (faster, no API calls)")
    parser.add_argument("--summary-only", action="store_true",
                       help="Only print executive summary (requires AI)")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run analysis
    enable_ai = not args.no_ai
    analyzer = AnnualReportAnalyzer(enable_ai=enable_ai)
    results = analyzer.analyze_company(
        args.symbol,
        args.num_reports,
        run_ai_analysis=enable_ai
    )

    # Output results
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to {args.output}")
    elif args.summary_only and results.get("executive_summary"):
        print("\n" + "=" * 60)
        print(f"EXECUTIVE SUMMARY: {args.symbol.upper()}")
        print("=" * 60)
        print(results["executive_summary"])
    else:
        print("\n" + "=" * 60)
        print("ANALYSIS RESULTS")
        print("=" * 60)

        # Print executive summary first if available
        if results.get("executive_summary"):
            print("\n### EXECUTIVE SUMMARY ###")
            print(results["executive_summary"])
            print("\n" + "-" * 60)

        # Print per-report AI analysis
        for report in results.get("reports", []):
            print(f"\n### FY{report.get('fiscal_year', 'N/A')} ###")
            print(f"Filed: {report.get('filing_date', 'N/A')}")

            if report.get("ai_analysis"):
                ai = report["ai_analysis"]
                if ai.get("business_summary"):
                    print(f"\n**Business Summary:**\n{ai['business_summary']}")
                if ai.get("risk_analysis"):
                    print(f"\n**Risk Analysis:**\n{ai['risk_analysis']}")
                if ai.get("mda_analysis"):
                    print(f"\n**MD&A Analysis:**\n{ai['mda_analysis']}")
            print("-" * 40)

        # Print YoY comparisons
        if results.get("yoy_comparisons"):
            print("\n### YEAR-OVER-YEAR COMPARISONS ###")
            for comp in results["yoy_comparisons"]:
                print(f"\n{comp.get('years', '')}:")
                print(comp.get("analysis", ""))
                print("-" * 40)

    return results


if __name__ == "__main__":
    main()
