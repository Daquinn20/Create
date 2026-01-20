"""
Company Report Backend
Fetches comprehensive company data from FMP and Fiscal.ai APIs
Enhanced with AI analysis using Anthropic Claude and OpenAI
"""
import os
import logging
import requests
from flask import Flask, jsonify, request, send_from_directory, send_file
from flask_cors import CORS
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, Any, List, Optional
import anthropic
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak, KeepTogether
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
import io

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# API Configuration
FMP_API_KEY = os.getenv("FMP_API_KEY")
FISCAL_AI_API_KEY = os.getenv("FISCAL_AI_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/api/v3"
FMP_V4_BASE = "https://financialmodelingprep.com/api/v4"
FISCAL_BASE = "https://api.fiscalnote.com"

# Initialize AI clients
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================
# LANGUAGE TRANSLATIONS
# ============================================
TRANSLATIONS = {
    "en": {
        # Report titles
        "company_report": "Company Report",
        "generated": "Generated",
        "as_of": "As of",
        "tagline": "Precision Analysis for Informed Investment Decisions",

        # Section headers
        "section_1": "1. Company Details",
        "section_2": "2. Business Overview",
        "section_3": "3. Competitive Landscape",
        "section_4": "4. Risks and Red Flags",
        "section_5": "5. Revenue and Margins",
        "section_6": "6. Highlights from Recent Quarters",
        "section_7": "7. Key Metrics",
        "section_8": "8. Valuations",
        "section_9": "9. Balance Sheet / Credit Metrics",
        "section_10": "10. Technical Analysis",
        "section_11": "11. Management",
        "section_12": "12. Prior Analysis Insights",

        # Company details table
        "ticker": "Ticker",
        "price": "Price",
        "market_cap": "Market Cap",
        "enterprise_value": "Enterprise Value",
        "52w_high": "52W High",
        "52w_low": "52W Low",
        "sector": "Sector",
        "industry": "Industry",
        "headquarters": "Headquarters",
        "employees": "Employees",
        "beta": "Beta",
        "div_yield": "Div. Yield",

        # Competitive landscape
        "key_competitors": "Key Competitors",
        "competitor": "Competitor",
        "competitive_threat": "Competitive Threat",
        "their_strength": "Their Strength",
        "emerging_competitors": "Emerging Competitors",
        "threat_level": "Threat Level",
        "disruption_potential": "Disruption Potential",
        "competitive_advantages": "Competitive Advantages",
        "moat_analysis": "Moat Analysis",
        "market_dynamics": "Market Dynamics",

        # Risks
        "company_red_flags": "A) Company Red Flags",
        "general_risks": "B) General Risks",

        # Revenue and margins
        "revenue_margins_history": "Revenue & Margins - 8 Year History + Estimates",
        "metric": "Metric",
        "revenue": "Revenue",
        "gross_margin": "Gross Margin",
        "op_margin": "Op. Margin",
        "net_margin": "Net Margin",
        "segment": "Segment",
        "pct_of_total": "% of Total",
        "segment_analysis": "Segment Analysis",

        # Quarterly highlights
        "qoq_changes": "Quarter-over-Quarter Changes",
        "positive_trends": "Positive Trends",
        "areas_of_concern": "Areas of Concern",
        "key_business_drivers": "Key Business Drivers",
        "ai_identified_metrics": "AI-identified metrics most important to this company",
        "value": "Value",
        "change": "Change",
        "insight": "Insight",
        "op_income": "Op. Income",
        "net_income": "Net Income",
        "eps": "EPS",
        "op_cash_flow": "Op. Cash Flow",
        "deferred_rev": "Deferred Rev",
        "eps_surprise": "EPS Surprise",

        # Key metrics
        "5yr_avg": "5 Year Avg",
        "3yr_avg": "3 Yr Avg",
        "ttm": "TTM",
        "est_1yr": "Estimated 1 Yr",
        "est_2yr": "Estimated 2 Yr",
        "revenue_growth": "Revenue Growth",
        "operating_margin": "Operating Margin",
        "net_income_margin": "Net Income Margin",
        "roic": "ROIC",
        "roe": "ROE",
        "roa": "ROA",
        "wacc": "WACC",

        # Valuations
        "current": "Current",
        "historical_avg": "Historical",
        "forward": "Forward",
        "pe_ratio": "P/E Ratio",
        "ev_ebitda": "EV/EBITDA",
        "price_sales": "Price/Sales",
        "price_book": "Price/Book",
        "peg_ratio": "PEG Ratio",
        "fcf_yield": "FCF Yield",
        "valuation_analysis": "Valuation Analysis",

        # Balance sheet
        "balance_sheet_summary": "Balance Sheet Summary",
        "total_assets": "Total Assets",
        "total_liabilities": "Total Liabilities",
        "total_equity": "Total Equity",
        "total_debt": "Total Debt",
        "cash_equivalents": "Cash & Equivalents",
        "net_debt": "Net Debt",
        "credit_metrics": "Credit Metrics",
        "debt_to_equity": "Debt/Equity",
        "debt_to_ebitda": "Debt/EBITDA",
        "interest_coverage": "Interest Coverage",
        "current_ratio": "Current Ratio",
        "quick_ratio": "Quick Ratio",
        "altman_z": "Altman Z-Score",
        "balance_sheet_analysis": "Balance Sheet Analysis",

        # Technical analysis
        "trend_analysis": "Trend Analysis",
        "timeframe": "Timeframe",
        "trend": "Trend",
        "signal": "Signal",
        "support_resistance": "Support/Resistance Levels",
        "level": "Level",
        "type": "Type",
        "strength": "Strength",
        "support": "Support",
        "resistance": "Resistance",
        "momentum_indicators": "Momentum Indicators",
        "indicator": "Indicator",
        "status": "Status",
        "rsi": "RSI (14)",
        "macd": "MACD",
        "moving_averages": "Moving Averages",
        "sma_50": "SMA 50",
        "sma_200": "SMA 200",
        "technical_summary": "Technical Summary",

        # Management
        "executive": "Executive",
        "title": "Title",
        "tenure": "Tenure",
        "compensation": "Compensation",
        "years": "years",
        "management_analysis": "Management Analysis",

        # Prior analysis
        "prior_earnings_insights": "Prior Earnings Call Insights",
        "prior_annual_report_insights": "Prior Annual Report (10-K) Insights",

        # Investment thesis
        "investment_thesis": "Investment Thesis",
        "bull_case": "Bull Case",
        "bear_case": "Bear Case",
        "recommendation": "Recommendation",
        "catalysts": "Key Catalysts",

        # AI language instruction
        "ai_language_instruction": "",
    },
    "it": {
        # Report titles
        "company_report": "Report Aziendale",
        "generated": "Generato",
        "as_of": "Alla data del",
        "tagline": "Analisi di Precisione per Decisioni di Investimento Informate",

        # Section headers
        "section_1": "1. Dettagli Aziendali",
        "section_2": "2. Panoramica Aziendale",
        "section_3": "3. Panorama Competitivo",
        "section_4": "4. Rischi e Segnali di Allarme",
        "section_5": "5. Ricavi e Margini",
        "section_6": "6. Highlights dei Trimestri Recenti",
        "section_7": "7. Metriche Chiave",
        "section_8": "8. Valutazioni",
        "section_9": "9. Stato Patrimoniale / Metriche di Credito",
        "section_10": "10. Analisi Tecnica",
        "section_11": "11. Management",
        "section_12": "12. Approfondimenti da Analisi Precedenti",

        # Company details table
        "ticker": "Ticker",
        "price": "Prezzo",
        "market_cap": "Cap. di Mercato",
        "enterprise_value": "Enterprise Value",
        "52w_high": "Max 52 Sett.",
        "52w_low": "Min 52 Sett.",
        "sector": "Settore",
        "industry": "Industria",
        "headquarters": "Sede Centrale",
        "employees": "Dipendenti",
        "beta": "Beta",
        "div_yield": "Rend. Div.",

        # Competitive landscape
        "key_competitors": "Principali Concorrenti",
        "competitor": "Concorrente",
        "competitive_threat": "Minaccia Competitiva",
        "their_strength": "Loro Punto di Forza",
        "emerging_competitors": "Concorrenti Emergenti",
        "threat_level": "Livello di Minaccia",
        "disruption_potential": "Potenziale di Disruption",
        "competitive_advantages": "Vantaggi Competitivi",
        "moat_analysis": "Analisi del Moat",
        "market_dynamics": "Dinamiche di Mercato",

        # Risks
        "company_red_flags": "A) Segnali di Allarme Aziendali",
        "general_risks": "B) Rischi Generali",

        # Revenue and margins
        "revenue_margins_history": "Ricavi e Margini - Storico 8 Anni + Stime",
        "metric": "Metrica",
        "revenue": "Ricavi",
        "gross_margin": "Margine Lordo",
        "op_margin": "Margine Op.",
        "net_margin": "Margine Netto",
        "segment": "Segmento",
        "pct_of_total": "% del Totale",
        "segment_analysis": "Analisi dei Segmenti",

        # Quarterly highlights
        "qoq_changes": "Variazioni Trimestre su Trimestre",
        "positive_trends": "Trend Positivi",
        "areas_of_concern": "Aree di Preoccupazione",
        "key_business_drivers": "Driver Chiave del Business",
        "ai_identified_metrics": "Metriche identificate dall'IA più importanti per questa azienda",
        "value": "Valore",
        "change": "Variazione",
        "insight": "Insight",
        "op_income": "Reddito Op.",
        "net_income": "Utile Netto",
        "eps": "EPS",
        "op_cash_flow": "Flusso Cassa Op.",
        "deferred_rev": "Ricavi Diff.",
        "eps_surprise": "Sorpresa EPS",

        # Key metrics
        "5yr_avg": "Media 5 Anni",
        "3yr_avg": "Media 3 Anni",
        "ttm": "TTM",
        "est_1yr": "Stima 1 Anno",
        "est_2yr": "Stima 2 Anni",
        "revenue_growth": "Crescita Ricavi",
        "operating_margin": "Margine Operativo",
        "net_income_margin": "Margine Utile Netto",
        "roic": "ROIC",
        "roe": "ROE",
        "roa": "ROA",
        "wacc": "WACC",

        # Valuations
        "current": "Attuale",
        "historical_avg": "Storico",
        "forward": "Forward",
        "pe_ratio": "Rapporto P/E",
        "ev_ebitda": "EV/EBITDA",
        "price_sales": "Prezzo/Vendite",
        "price_book": "Prezzo/Book",
        "peg_ratio": "Rapporto PEG",
        "fcf_yield": "Rendimento FCF",
        "valuation_analysis": "Analisi delle Valutazioni",

        # Balance sheet
        "balance_sheet_summary": "Riepilogo Stato Patrimoniale",
        "total_assets": "Totale Attività",
        "total_liabilities": "Totale Passività",
        "total_equity": "Patrimonio Netto",
        "total_debt": "Debito Totale",
        "cash_equivalents": "Cassa e Equivalenti",
        "net_debt": "Debito Netto",
        "credit_metrics": "Metriche di Credito",
        "debt_to_equity": "Debito/Equity",
        "debt_to_ebitda": "Debito/EBITDA",
        "interest_coverage": "Copertura Interessi",
        "current_ratio": "Rapporto Corrente",
        "quick_ratio": "Quick Ratio",
        "altman_z": "Altman Z-Score",
        "balance_sheet_analysis": "Analisi dello Stato Patrimoniale",

        # Technical analysis
        "trend_analysis": "Analisi del Trend",
        "timeframe": "Orizzonte Temporale",
        "trend": "Trend",
        "signal": "Segnale",
        "support_resistance": "Livelli di Supporto/Resistenza",
        "level": "Livello",
        "type": "Tipo",
        "strength": "Forza",
        "support": "Supporto",
        "resistance": "Resistenza",
        "momentum_indicators": "Indicatori di Momentum",
        "indicator": "Indicatore",
        "status": "Stato",
        "rsi": "RSI (14)",
        "macd": "MACD",
        "moving_averages": "Medie Mobili",
        "sma_50": "SMA 50",
        "sma_200": "SMA 200",
        "technical_summary": "Riepilogo Tecnico",

        # Management
        "executive": "Dirigente",
        "title": "Ruolo",
        "tenure": "Anzianità",
        "compensation": "Compenso",
        "years": "anni",
        "management_analysis": "Analisi del Management",

        # Prior analysis
        "prior_earnings_insights": "Approfondimenti da Earnings Call Precedenti",
        "prior_annual_report_insights": "Approfondimenti dal Report Annuale (10-K)",

        # Investment thesis
        "investment_thesis": "Tesi di Investimento",
        "bull_case": "Scenario Rialzista",
        "bear_case": "Scenario Ribassista",
        "recommendation": "Raccomandazione",
        "catalysts": "Catalizzatori Chiave",

        # AI language instruction
        "ai_language_instruction": "IMPORTANT: You must respond ENTIRELY in Italian. All analysis, insights, and commentary must be written in Italian language.",
    }
}


def get_translation(key: str, language: str = "en") -> str:
    """Get translation for a key in the specified language."""
    return TRANSLATIONS.get(language, TRANSLATIONS["en"]).get(key, TRANSLATIONS["en"].get(key, key))


# ============================================
# MULTI-AGENT SYSTEM - 10 Specialized Agents
# ============================================

SPECIALIZED_AGENTS = {
    "financial_analyst": {
        "name": "Financial Analyst",
        "emoji": "📊",
        "prompt": """You are a Senior Financial Analyst with 20+ years of experience at top investment banks.
Analyze this company's financial health focusing on:
- Revenue growth trajectory and sustainability
- Margin trends (gross, operating, net)
- Cash flow quality and free cash flow generation
- Working capital efficiency
- Return metrics (ROE, ROIC, ROA)
Provide specific numbers and comparisons to industry averages. Be concise but thorough."""
    },
    "valuation_expert": {
        "name": "Valuation Expert",
        "emoji": "💰",
        "prompt": """You are a Valuation Expert specializing in equity research and M&A.
Analyze this company's valuation focusing on:
- Current multiples vs historical averages (P/E, EV/EBITDA, P/S, P/FCF)
- Valuation relative to peers and sector
- PEG ratio and growth-adjusted value
- Fair value estimate range
- Whether the stock appears overvalued, fairly valued, or undervalued
Provide specific price targets or valuation ranges where possible."""
    },
    "risk_analyst": {
        "name": "Risk Analyst",
        "emoji": "⚠️",
        "prompt": """You are a Risk Analyst specializing in corporate credit and equity risk.
Identify and analyze key risks:
- Balance sheet risks (debt levels, liquidity, refinancing needs)
- Business model vulnerabilities
- Regulatory and legal risks
- Customer/supplier concentration
- Currency and commodity exposure
- ESG and reputational risks
Rate overall risk level (Low/Medium/High) with specific justification."""
    },
    "technical_analyst": {
        "name": "Technical Analyst",
        "emoji": "📈",
        "prompt": """You are a Chartered Market Technician (CMT) with expertise in price action.
Analyze the technical setup:
- Current trend (uptrend, downtrend, sideways)
- Key support and resistance levels
- Moving average alignment (golden cross, death cross)
- Momentum indicators (RSI, MACD signals)
- Volume patterns and significance
- Near-term price outlook (1-3 months)
Provide specific price levels and actionable insights."""
    },
    "competitive_intel": {
        "name": "Competitive Intelligence",
        "emoji": "🎯",
        "prompt": """You are a Competitive Intelligence Analyst from a top strategy consulting firm.
Analyze the competitive landscape:
- Economic moat strength and durability (wide, narrow, none)
- Sources of competitive advantage (brand, scale, network effects, switching costs, patents)
- Market share position and trends
- Competitive threats and disruptors
- Barriers to entry in the industry
Provide a moat rating (1-10) with detailed justification."""
    },
    "industry_analyst": {
        "name": "Industry Analyst",
        "emoji": "🏭",
        "prompt": """You are an Industry Analyst covering this sector for a major research firm.
Analyze the industry dynamics:
- Total addressable market (TAM) and growth rate
- Industry lifecycle stage (growth, mature, declining)
- Key secular trends and tailwinds/headwinds
- Regulatory environment and changes
- Technology disruption potential
- Industry consolidation trends
Provide specific market size figures and growth projections."""
    },
    "earnings_analyst": {
        "name": "Earnings Analyst",
        "emoji": "📋",
        "prompt": """You are an Earnings Quality Analyst specializing in accounting forensics.
Analyze earnings quality and trends:
- Revenue recognition practices
- Earnings vs cash flow relationship
- One-time items and adjustments
- Guidance track record (beats/misses)
- Analyst estimate revisions trend
- Earnings predictability and consistency
Flag any accounting red flags or quality concerns."""
    },
    "management_analyst": {
        "name": "Management Analyst",
        "emoji": "👔",
        "prompt": """You are a Corporate Governance Analyst evaluating management teams.
Analyze management and governance:
- CEO and C-suite track record and tenure
- Capital allocation history (M&A, buybacks, dividends)
- Insider ownership and recent transactions
- Executive compensation alignment
- Board composition and independence
- Management credibility and transparency
Rate management quality (A/B/C/D/F) with justification."""
    },
    "investment_strategist": {
        "name": "Investment Strategist",
        "emoji": "🎲",
        "prompt": """You are a Chief Investment Strategist at a major asset manager.
Provide investment thesis and recommendations:
- Bull case (3-5 key points with upside catalysts)
- Bear case (3-5 key points with downside risks)
- Base case scenario and probability
- Key metrics to monitor
- Upcoming catalysts (earnings, events, macro)
- Investment recommendation (Buy/Hold/Sell) with conviction level"""
    },
    "esg_analyst": {
        "name": "ESG Analyst",
        "emoji": "🌱",
        "prompt": """You are an ESG (Environmental, Social, Governance) Research Analyst.
Analyze sustainability and governance factors:
- Environmental footprint and climate commitments
- Social responsibility (labor practices, diversity, community)
- Governance quality (board, transparency, shareholder rights)
- ESG rating relative to peers
- Material ESG risks for this industry
- Improvement trajectory on ESG metrics
Provide an overall ESG grade (A/B/C/D/F) with reasoning."""
    },
    "prior_analysis_synthesizer": {
        "name": "Prior Analysis Synthesizer",
        "emoji": "🔗",
        "prompt": """You are a Senior Research Analyst tasked with synthesizing prior earnings call and annual report analyses.

Your job is to extract and highlight the MOST IMPORTANT findings from the prior analyses provided.

FOCUS ON:

**FROM EARNINGS CALLS:**
- Key management quotes and guidance
- Revenue and earnings surprises (beat/miss)
- Forward guidance changes
- Product/segment commentary
- Margin trends discussed
- Capital allocation priorities
- Competitive positioning statements

**FROM ANNUAL REPORTS (10-K):**
- Major business model changes
- Revenue mix shifts by segment/product
- Key risk factor changes
- R&D and capex priorities
- Regulatory or legal developments
- Geographic expansion/contraction
- Acquisition or divestiture activity

**SYNTHESIZE INTO:**
1. **Top 5 Bullish Points** - strongest positives from both sources
2. **Top 5 Concerns** - key risks and negatives identified
3. **Key Products/Segments** - what's driving growth or declining
4. **Management Credibility** - are they delivering on promises?
5. **Critical Metrics to Watch** - what should investors monitor?

Be SPECIFIC with product names, revenue figures, percentages, and direct quotes where available."""
    }
}


def run_single_agent(agent_id: str, symbol: str, company_data: Dict[str, Any], language: str = "en") -> Dict[str, Any]:
    """Run a single specialized agent analysis."""
    agent = SPECIALIZED_AGENTS.get(agent_id)
    if not agent:
        return {"agent": agent_id, "error": "Agent not found"}

    # Get language instruction
    lang_instruction = get_translation("ai_language_instruction", language)

    # Build context from company data
    context = f"""
Company: {company_data.get('company_name', symbol)} ({symbol})
Industry: {company_data.get('industry', 'N/A')}
Sector: {company_data.get('sector', 'N/A')}
Market Cap: ${company_data.get('market_cap', 0):,.0f}
Current Price: ${company_data.get('price', 0):.2f}

Key Metrics:
- Revenue Growth (TTM): {company_data.get('revenue_growth_ttm', 'N/A')}%
- Gross Margin: {company_data.get('gross_margin', 'N/A')}
- Operating Margin: {company_data.get('operating_margin', 'N/A')}
- Net Margin: {company_data.get('net_margin', 'N/A')}
- ROE: {company_data.get('roe', 'N/A')}
- ROIC: {company_data.get('roic', 'N/A')}
- P/E Ratio: {company_data.get('pe_ratio', 'N/A')}
- EV/EBITDA: {company_data.get('ev_to_ebitda', 'N/A')}
- Debt/Equity: {company_data.get('debt_to_equity', 'N/A')}
- Beta: {company_data.get('beta', 'N/A')}

Recent Performance:
- 52-Week High: ${company_data.get('week_52_high', 'N/A')}
- 52-Week Low: ${company_data.get('week_52_low', 'N/A')}
- YTD Return: {company_data.get('ytd_return', 'N/A')}%

Description: {company_data.get('description', 'N/A')[:500]}
"""

    # Add prior analysis context if available (increased to 8K per document for deeper insights)
    prior_context = ""
    prior_earnings = company_data.get('prior_earnings_analysis', '')
    prior_annual = company_data.get('prior_annual_report_analysis', '')

    if prior_earnings or prior_annual:
        prior_context = "\n\n=== PRIOR ANALYSIS FOR DEEPER INSIGHTS ===\n"
        prior_context += "IMPORTANT: Use these prior analyses to provide detailed, specific insights. Reference specific products, revenue figures, growth rates, and management commentary.\n"

        if prior_earnings:
            prior_context += f"\n--- EARNINGS CALL ANALYSIS ---\n{prior_earnings[:8000]}\n"

        if prior_annual:
            prior_context += f"\n--- ANNUAL REPORT (10-K) ANALYSIS ---\n{prior_annual[:8000]}\n"

        prior_context += "\n=== END PRIOR ANALYSIS ===\n"
        prior_context += "You MUST incorporate specific products, segments, revenue figures, growth rates, and management commentary from the above analyses. Be specific with names and numbers.\n"

    # Add language instruction to prompt if not English
    lang_prefix = f"{lang_instruction}\n\n" if lang_instruction else ""
    full_prompt = f"{lang_prefix}{agent['prompt']}\n\nAnalyze this company:\n{context}{prior_context}\n\nProvide your expert analysis (3-4 detailed paragraphs, be SPECIFIC with product names, revenue figures, growth rates, and insights from prior analyses):"

    try:
        # Try Claude first - increased tokens for deeper analysis
        if anthropic_client:
            message = anthropic_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1500,
                messages=[{"role": "user", "content": full_prompt}]
            )
            analysis = message.content[0].text
        elif openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": full_prompt}],
                max_tokens=1500
            )
            analysis = response.choices[0].message.content
        else:
            analysis = "AI analysis not available - no API keys configured"

        return {
            "agent_id": agent_id,
            "agent_name": agent["name"],
            "emoji": agent["emoji"],
            "analysis": analysis,
            "status": "success"
        }
    except Exception as e:
        return {
            "agent_id": agent_id,
            "agent_name": agent["name"],
            "emoji": agent["emoji"],
            "analysis": f"Analysis failed: {str(e)}",
            "status": "error"
        }


def run_all_agents_parallel(symbol: str, company_data: Dict[str, Any],
                            progress_callback=None, language: str = "en") -> Dict[str, Any]:
    """Run all 10 specialized agents in parallel."""
    results = {}
    agent_ids = list(SPECIALIZED_AGENTS.keys())
    completed = 0
    lock = threading.Lock()

    def run_with_tracking(agent_id):
        nonlocal completed
        result = run_single_agent(agent_id, symbol, company_data, language)
        with lock:
            completed += 1
            if progress_callback:
                progress_callback(agent_id, completed, len(agent_ids))
        return agent_id, result

    # Run all agents in parallel with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(run_with_tracking, agent_id): agent_id
                   for agent_id in agent_ids}

        for future in as_completed(futures):
            agent_id, result = future.result()
            results[agent_id] = result

    return results


def get_multi_agent_summary(agent_results: Dict[str, Any]) -> str:
    """Generate a summary combining insights from all agents."""
    summaries = []
    for agent_id, result in agent_results.items():
        if result.get("status") == "success":
            summaries.append(f"**{result['emoji']} {result['agent_name']}**: {result['analysis'][:200]}...")

    return "\n\n".join(summaries)


class APIError(Exception):
    """Custom exception for API errors"""
    pass


def fmp_get(endpoint: str, params: Optional[Dict] = None) -> Any:
    """Make GET request to FMP API (v3)"""
    if not FMP_API_KEY:
        raise APIError("FMP API key not found")

    if params is None:
        params = {}
    params['apikey'] = FMP_API_KEY

    url = f"{FMP_BASE}/{endpoint}"
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict) and "Error Message" in data:
            raise APIError(f"FMP API error: {data['Error Message']}")

        return data
    except requests.exceptions.RequestException as e:
        raise APIError(f"FMP API request failed: {str(e)}")


def fmp_get_v4(endpoint: str, params: Optional[Dict] = None) -> Any:
    """Make GET request to FMP API v4 (for segment data, etc.)"""
    if not FMP_API_KEY:
        return None

    if params is None:
        params = {}
    params['apikey'] = FMP_API_KEY

    url = f"{FMP_V4_BASE}/{endpoint}"
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        logger.debug(f"FMP v4 API error for {endpoint}: {e}")
        return None


def fiscal_get(endpoint: str, params: Optional[Dict] = None) -> Any:
    """Make GET request to Fiscal.ai API"""
    if not FISCAL_AI_API_KEY:
        raise APIError("Fiscal.ai API key not found")

    headers = {
        'Authorization': f'Bearer {FISCAL_AI_API_KEY}',
        'Content-Type': 'application/json'
    }

    url = f"{FISCAL_BASE}/{endpoint}"
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # Fiscal.ai might not be available, return empty dict instead of failing
        logger.error(f"Fiscal.ai API request failed: {str(e)}")
        return {}


def fetch_annual_report_text(symbol: str) -> str:
    """Fetch annual report text from FMP"""
    try:
        # Get the latest 10-K filing
        filings = fmp_get(f"sec_filings/{symbol}", {"type": "10-K", "limit": 1})

        if not filings or len(filings) == 0:
            return ""

        filing_url = filings[0].get("finalLink", "")
        if not filing_url:
            return ""

        # Fetch the filing content
        response = requests.get(filing_url, timeout=30)
        if response.status_code == 200:
            # Return first 50000 characters to stay within token limits
            return response.text[:50000]
    except Exception as e:
        logger.error(f" fetching annual report: {e}")

    return ""


def fetch_earnings_transcripts(symbol: str, limit: int = 4) -> List[Dict[str, str]]:
    """Fetch earnings call transcripts from FMP"""
    transcripts = []
    try:
        # Get earnings call transcripts
        calls = fmp_get(f"earning_call_transcript/{symbol}", {"limit": limit})

        if calls and isinstance(calls, list):
            for call in calls[:limit]:
                quarter = call.get("quarter", "")
                year = call.get("year", "")
                content = call.get("content", "")

                if content:
                    transcripts.append({
                        "quarter": f"Q{quarter} {year}",
                        "content": content[:20000]  # Limit to first 20k chars
                    })
    except Exception as e:
        logger.error(f" fetching earnings transcripts: {e}")

    return transcripts


def fetch_quarterly_reports(symbol: str, limit: int = 4) -> List[Dict[str, str]]:
    """Fetch quarterly earnings reports (10-Q filings) from FMP"""
    reports = []
    try:
        # Get the latest 10-Q filings
        filings = fmp_get(f"sec_filings/{symbol}", {"type": "10-Q", "limit": limit})

        if filings and isinstance(filings, list):
            for filing in filings[:limit]:
                filing_date = filing.get("fillingDate", "")
                filing_url = filing.get("finalLink", "")

                if filing_url:
                    try:
                        # Fetch the filing content
                        response = requests.get(filing_url, timeout=30)
                        if response.status_code == 200:
                            reports.append({
                                "date": filing_date,
                                "content": response.text[:30000]  # Limit to first 30k chars
                            })
                    except Exception as e:
                        logger.error(f" fetching 10-Q content: {e}")
                        continue
    except Exception as e:
        logger.error(f" fetching quarterly reports: {e}")

    return reports


def analyze_with_ai(prompt: str, content: str, use_claude: bool = True, language: str = "en") -> str:
    """Analyze content using AI (Claude or GPT)"""
    try:
        # Add language instruction if not English
        lang_instruction = get_translation("ai_language_instruction", language)
        lang_prefix = f"{lang_instruction}\n\n" if lang_instruction else ""
        full_prompt = f"{lang_prefix}{prompt}\n\nContent to analyze:\n{content}"

        if use_claude and anthropic_client:
            message = anthropic_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4000,
                messages=[
                    {"role": "user", "content": full_prompt}
                ]
            )
            return message.content[0].text

        elif openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a financial analyst expert."},
                    {"role": "user", "content": full_prompt}
                ],
                max_tokens=2000
            )
            return response.choices[0].message.content

        else:
            return "AI analysis unavailable - API keys not configured"

    except Exception as e:
        logger.error(f" in AI analysis: {e}")
        return f"Error performing AI analysis: {str(e)}"


def get_business_overview(symbol: str, language: str = "en") -> Dict[str, Any]:
    """Get company profile and business overview with AI-enhanced analysis"""
    try:
        # Get basic profile data
        profile = fmp_get(f"profile/{symbol}")
        if not profile or len(profile) == 0:
            return {"error": "Unable to fetch company profile"}

        data = profile[0]
        company_name = data.get("companyName", "N/A")

        # Fetch annual report for AI analysis
        logger.info(f"Fetching annual report for {symbol}...")
        annual_report = fetch_annual_report_text(symbol)

        # Fetch earnings transcripts for additional context
        logger.info(f"Fetching earnings transcripts for {symbol} business overview...")
        transcripts = fetch_earnings_transcripts(symbol, limit=2)
        transcript_content = ""
        if transcripts:
            transcript_content = "\n\n=== RECENT EARNINGS CALL TRANSCRIPTS ===\n\n".join(
                [f"Quarter: {t['quarter']}\n{t['content'][:15000]}" for t in transcripts[:2]]
            )

        # Generate AI-enhanced description
        if annual_report or transcript_content:
            logger.info(f"Analyzing annual report and transcripts with AI...")

            # Combine sources
            combined_sources = ""
            if annual_report:
                combined_sources += f"=== ANNUAL REPORT (10-K) ===\n{annual_report}\n\n"
            if transcript_content:
                combined_sources += transcript_content

            ai_prompt = f"""You are a senior equity research analyst preparing an in-depth investment report on {company_name} ({symbol}).

Based on the annual report and earnings call transcripts provided, create a comprehensive business overview (900-1200 words) structured as follows:

## BUSINESS MODEL & OPERATIONS
- **Core Business**: What does the company actually do? Describe ALL major products/services in detail
- **Revenue Model**: How does the company generate revenue? Break down by type (subscription, licensing, advertising, product sales, services, transaction fees, etc.)
- **Revenue Mix**: What percentage comes from each revenue stream? Which is growing fastest?
- **Unit Economics**: If available, describe customer acquisition costs (CAC), lifetime value (LTV), average revenue per user (ARPU), or key unit economics
- **Operating Model**: Asset-light vs asset-heavy? Fixed vs variable cost structure? Operating leverage characteristics?

## MARKET POSITION & SCALE
- **Total Addressable Market (TAM)**: Size of the market opportunity with specific dollar figures
- **Market Share**: Company's current share and trajectory (gaining or losing?)
- **Geographic Footprint**: Revenue distribution by region (Americas, EMEA, APAC) with growth rates
- **Customer Base**: Total customers, customer segments, enterprise vs SMB mix
- **Customer Concentration**: Top 10 customers as % of revenue, any single customer >10%?

## PRODUCTS & SERVICES DEEP DIVE
- **Product Portfolio**: List and describe each major product/service offering
- **Product Differentiation**: What makes each product unique vs competitors?
- **New Products**: Recent launches (last 12-18 months) and their traction
- **Product Roadmap**: Announced future products or capabilities
- **Technology Stack**: Key technologies, platforms, or infrastructure that enable the business

## STRATEGIC PRIORITIES & CAPITAL ALLOCATION
- **Strategic Initiatives**: Current major initiatives (M&A, partnerships, new markets, restructuring)
- **Recent M&A**: Notable acquisitions/divestitures and strategic rationale
- **R&D Investment**: R&D as % of revenue, key areas of investment
- **Capital Allocation**: Dividend policy, buybacks, debt management, capex priorities
- **Management Commentary**: CEO's stated vision and priorities from earnings calls

## GROWTH DRIVERS & RISKS
- **Primary Growth Levers**: Pricing power, volume growth, new markets, new products, cross-sell/upsell
- **Secular Tailwinds**: Macro trends benefiting the business (digitalization, cloud, AI, demographics, etc.)
- **Near-term Catalysts**: Specific events that could drive stock in next 6-12 months
- **Key Risks**: Top 3-5 business risks investors should monitor
- **Guidance**: Management's forward guidance and key assumptions

Write in a professional, analytical tone. Be SPECIFIC with numbers, percentages, and examples from the source documents. Avoid generic statements. If specific data is not available, note "not disclosed" rather than guessing."""

            # Use Anthropic Claude for analysis
            ai_description = analyze_with_ai(ai_prompt, combined_sources[:80000], use_claude=True, language=language)

            # If Claude fails, try OpenAI
            if "Error" in ai_description or "unavailable" in ai_description:
                logger.info("Claude analysis failed, trying OpenAI...")
                ai_description = analyze_with_ai(ai_prompt, annual_report, use_claude=False, language=language)

        else:
            # Fallback to basic description from FMP
            ai_description = data.get("description", "No detailed description available")
            logger.info(f"Annual report not available, using basic description")

        # Get additional key metrics
        key_metrics = {}
        try:
            metrics = fmp_get(f"key-metrics-ttm/{symbol}")
            if metrics and len(metrics) > 0:
                key_metrics = metrics[0]
        except (requests.RequestException, KeyError, IndexError, TypeError) as e:
            logger.warning(f" Could not fetch key metrics for {symbol}: {e}")

        # Get shares outstanding from income statement or key metrics
        shares_outstanding = 0
        try:
            income_stmt = fmp_get(f"income-statement/{symbol}", {"limit": 1})
            if income_stmt and len(income_stmt) > 0:
                shares_outstanding = income_stmt[0].get("weightedAverageShsOutDil", 0)
        except (requests.RequestException, KeyError, IndexError, TypeError) as e:
            logger.warning(f" Could not fetch shares outstanding for {symbol}: {e}")

        # Get 52-week high and low
        week_52_high = None
        week_52_low = None

        # First try to parse from range field
        if data.get("range"):
            try:
                range_parts = data.get("range", "").split("-")
                if len(range_parts) >= 2:
                    week_52_low = float(range_parts[0].strip())
                    week_52_high = float(range_parts[-1].strip())
            except (ValueError, TypeError):
                pass

        # If range not available or parsing failed, try to get from price data
        if week_52_high is None or week_52_low is None:
            try:
                # Get historical prices for 52 weeks
                historical = fmp_get(f"historical-price-full/{symbol}", {"timeseries": 252})  # ~1 year of trading days
                if historical and "historical" in historical and len(historical["historical"]) > 0:
                    prices = [day.get("high", 0) for day in historical["historical"]]
                    lows = [day.get("low", 0) for day in historical["historical"]]
                    if prices and lows:
                        week_52_high = max(prices)
                        week_52_low = min(lows)
            except (requests.RequestException, KeyError, IndexError, TypeError, ValueError) as e:
                logger.warning(f" Could not fetch 52-week range for {symbol}: {e}")

        # Get short interest if available
        short_interest = 0
        try:
            # Try to get from key metrics first
            if key_metrics and "shortInterestPercentage" in key_metrics:
                short_interest = key_metrics.get("shortInterestPercentage", 0)
            else:
                # Try from quote data
                short_interest = data.get("shortInterest", 0) / shares_outstanding * 100 if shares_outstanding > 0 and data.get("shortInterest") else 0
        except (KeyError, TypeError, ZeroDivisionError) as e:
            logger.warning(f" Could not calculate short interest for {symbol}: {e}")

        return {
            "company_name": company_name,
            "ticker": symbol,
            "exchange": data.get("exchangeShortName", "N/A"),
            "description": ai_description,
            "industry": data.get("industry", "N/A"),
            "sector": data.get("sector", "N/A"),
            "website": data.get("website", "N/A"),
            "ceo": data.get("ceo", "N/A"),
            "employees": data.get("fullTimeEmployees", "N/A"),
            "headquarters": f"{data.get('city', '')}, {data.get('state', '')}, {data.get('country', '')}",
            "market_cap": data.get("mktCap", 0),
            "enterprise_value": key_metrics.get("enterpriseValueTTM", 0) if key_metrics else 0,
            "beta": data.get("beta", 0),
            "price": data.get("price", 0),
            "shares_outstanding": shares_outstanding,
            "dividend_yield": key_metrics.get("dividendYieldTTM", 0) if key_metrics else 0,
            "week_52_high": week_52_high,
            "week_52_low": week_52_low,
            "short_interest": short_interest
        }
    except Exception as e:
        logger.error(f" fetching business overview: {e}")
        return {"error": f"Unable to fetch business overview: {str(e)}"}


def get_revenue_segments(symbol: str, language: str = "en") -> Dict[str, Any]:
    """Get revenue by segment and margin data with AI-enhanced analysis from annual report and quarterly earnings"""
    try:
        segment_data = []
        excluded_keys = ['date', 'symbol', 'cik', 'acceptedDate', 'period', 'filingDate', 'link', 'finalLink']

        def parse_segments(data, segment_type):
            """Parse segment data from FMP response"""
            parsed = []
            if data and isinstance(data, list) and len(data) > 0:
                latest = data[0]
                for key, value in latest.items():
                    if key not in excluded_keys:
                        if value and isinstance(value, (int, float)) and value > 0:
                            parsed.append({
                                "name": key,
                                "revenue": value,
                                "type": segment_type
                            })
            return parsed

        # Try all segmentation types - companies report differently
        # Note: Segment endpoints are FMP v4 API
        logger.info(f"Fetching revenue segments for {symbol}...")

        # 1. Product segmentation (e.g., iPhone, Mac, iPad for Apple)
        product_segments = fmp_get_v4(f"revenue-product-segmentation", {"symbol": symbol, "period": "annual", "structure": "flat"})
        product_data = parse_segments(product_segments, "product")
        if product_data:
            logger.info(f"Found {len(product_data)} product segments")
            segment_data.extend(product_data)

        # 2. Business/Operating segment (e.g., Google Services, Google Cloud)
        if not product_data:
            business_segments = fmp_get_v4(f"revenue-product-segmentation", {"symbol": symbol, "period": "annual"})
            business_data = parse_segments(business_segments, "business")
            if business_data:
                logger.info(f"Found {len(business_data)} business segments")
                segment_data.extend(business_data)

        # 3. Geographic segmentation (e.g., Americas, Europe, Asia)
        geo_segments = fmp_get_v4(f"revenue-geographic-segmentation", {"symbol": symbol, "period": "annual", "structure": "flat"})
        geo_data = parse_segments(geo_segments, "geographic")
        if geo_data and not segment_data:  # Only use geo if no product/business data
            logger.info(f"Using {len(geo_data)} geographic segments (no product/business data)")
            segment_data.extend(geo_data)
        elif geo_data:
            logger.info(f"Also found {len(geo_data)} geographic segments (available separately)")

        # Sort by revenue descending
        segment_data.sort(key=lambda x: x.get('revenue', 0), reverse=True)
        logger.info(f"Total segments found: {len(segment_data)}")

        # Get historical segment data (multiple years) from FMP
        logger.info(f"Fetching historical segment data for {symbol}...")
        historical_product = fmp_get_v4("revenue-product-segmentation", {"symbol": symbol, "period": "annual"})
        historical_geo = fmp_get_v4("revenue-geographic-segmentation", {"symbol": symbol, "period": "annual"})

        # Build FMP data summary for AI
        fmp_segment_summary = ""
        if segment_data:
            total_rev = sum(s.get('revenue', 0) for s in segment_data)
            fmp_segment_summary = f"\n\nFMP SEGMENT DATA (Most Recent):\nTotal Revenue: ${total_rev/1e9:.2f}B\n"
            for seg in segment_data:
                rev = seg.get('revenue', 0)
                pct = (rev / total_rev * 100) if total_rev > 0 else 0
                fmp_segment_summary += f"- {seg['name']}: ${rev/1e9:.2f}B ({pct:.1f}%)\n"

        if historical_product and len(historical_product) > 1:
            fmp_segment_summary += f"\nHISTORICAL DATA ({len(historical_product)} years available):\n"
            for i, year_data in enumerate(historical_product[:5]):  # Show up to 5 years
                if 'date' in year_data:
                    fmp_segment_summary += f"- {year_data.get('date', 'N/A')}\n"

        # Get financial ratios for TTM margins
        ratios = fmp_get(f"ratios-ttm/{symbol}")

        # Get 8 years of income statement for historical margins
        logger.info(f"Fetching 8 years of margin data for {symbol}...")
        income_annual = fmp_get(f"income-statement/{symbol}", {"limit": 8})
        income_quarterly = fmp_get(f"income-statement/{symbol}", {"period": "quarter", "limit": 1})

        # Build historical margins data (Last Q, then 8 years)
        historical_margins = []

        # Add last quarter first
        if income_quarterly and len(income_quarterly) > 0:
            q_data = income_quarterly[0]
            q_revenue = q_data.get('revenue', 0)
            if q_revenue > 0:
                historical_margins.append({
                    "period": f"Q{q_data.get('period', '?')} {q_data.get('calendarYear', '')}",
                    "date": q_data.get('date', ''),
                    "revenue": q_revenue,
                    "gross_margin": (q_data.get('grossProfit', 0) / q_revenue) * 100,
                    "operating_margin": (q_data.get('operatingIncome', 0) / q_revenue) * 100,
                    "net_margin": (q_data.get('netIncome', 0) / q_revenue) * 100,
                })

        # Add annual data (8 years)
        if income_annual:
            for year_data in income_annual:
                revenue = year_data.get('revenue', 0)
                if revenue > 0:
                    historical_margins.append({
                        "period": str(year_data.get('calendarYear', year_data.get('date', '')[:4])),
                        "date": year_data.get('date', ''),
                        "revenue": revenue,
                        "gross_margin": (year_data.get('grossProfit', 0) / revenue) * 100,
                        "operating_margin": (year_data.get('operatingIncome', 0) / revenue) * 100,
                        "net_margin": (year_data.get('netIncome', 0) / revenue) * 100,
                    })

        logger.info(f"Built {len(historical_margins)} periods of margin history")

        # Get single income statement for current margins (fallback)
        income = fmp_get(f"income-statement/{symbol}", {"limit": 1})

        # Fetch annual report for segment analysis
        logger.info(f"Fetching annual report for {symbol} segment analysis...")
        annual_report = fetch_annual_report_text(symbol)

        # Fetch quarterly reports (10-Q filings)
        logger.info(f"Fetching quarterly earnings reports for {symbol}...")
        quarterly_reports = fetch_quarterly_reports(symbol, limit=4)

        # Fetch earnings transcripts as additional source
        logger.info(f"Fetching earnings transcripts for {symbol}...")
        transcripts = fetch_earnings_transcripts(symbol, limit=4)

        # AI-enhanced segment analysis combining all sources
        analysis_sources = []

        # Add FMP segment data first (most reliable)
        if fmp_segment_summary:
            analysis_sources.append(("FMP Financial Data", fmp_segment_summary))

        # Add annual report
        if annual_report:
            analysis_sources.append(("Annual Report (10-K)", annual_report))

        # Add quarterly reports
        if quarterly_reports:
            for i, qreport in enumerate(quarterly_reports):
                analysis_sources.append((f"10-Q {qreport['date']}", qreport['content']))

        # Add transcripts
        if transcripts:
            for transcript in transcripts:
                analysis_sources.append((f"Earnings Call {transcript['quarter']}", transcript['content']))

        if analysis_sources:
            logger.info(f"Analyzing {len(analysis_sources)} sources with AI...")

            # Combine all sources
            combined_content = "\n\n=== NEXT SOURCE ===\n\n".join(
                [f"Source: {source[0]}\n{source[1]}" for source in analysis_sources]
            )

            ai_prompt = f"""You are analyzing financial data for {symbol}. You have FMP financial database data AND SEC filings.

PRIORITY: Use the FMP SEGMENT DATA numbers as your primary source - these are verified financial database figures.

Create a detailed revenue segment analysis (300-400 words):

1. **Segment Breakdown**: List each segment with TTM revenue in dollars and percentage of total
   - Use the exact numbers from FMP data when available
   - Format: "Segment Name: $XX.XB (XX% of total)"

2. **Segment Descriptions**: Brief description of what each segment does (from SEC filings)

3. **Segment Margins** (if available):
   - Operating margin by segment
   - Growth trends by segment

4. **Historical Context**: Note how segments have changed over time if historical data available

Use specific dollar amounts and percentages. If FMP data shows segments, those numbers are authoritative."""

            # Use Claude for analysis
            segment_analysis = analyze_with_ai(ai_prompt, combined_content, use_claude=True, language=language)

            # If Claude fails, try OpenAI
            if "Error" in segment_analysis or "unavailable" in segment_analysis or "cannot provide" in segment_analysis.lower():
                logger.info("Claude analysis incomplete, trying OpenAI...")
                segment_analysis = analyze_with_ai(ai_prompt, combined_content, use_claude=False, language=language)

            # Add AI analysis to segment data
            if segment_data:
                segment_data[0]["ai_analysis"] = segment_analysis
            elif segment_analysis:  # Even if no structured segment data, add the analysis
                segment_data.append({
                    "name": "AI Analysis",
                    "revenue": 0,
                    "ai_analysis": segment_analysis
                })
        else:
            logger.warning("No annual report or quarterly sources available for analysis")

        # Calculate margins
        margins = {}
        if ratios and len(ratios) > 0:
            ratio_data = ratios[0]
            margins = {
                "gross_margin": ratio_data.get("grossProfitMarginTTM", 0) * 100,
                "operating_margin": ratio_data.get("operatingProfitMarginTTM", 0) * 100,
                "net_margin": ratio_data.get("netProfitMarginTTM", 0) * 100
            }
        elif income and len(income) > 0:
            inc_data = income[0]
            revenue = inc_data.get("revenue", 1)
            if revenue > 0:
                gross_profit = inc_data.get("grossProfit", 0)
                operating_income = inc_data.get("operatingIncome", 0)
                net_income = inc_data.get("netIncome", 0)

                margins = {
                    "gross_margin": (gross_profit / revenue) * 100,
                    "operating_margin": (operating_income / revenue) * 100,
                    "net_margin": (net_income / revenue) * 100
                }

        # Fetch analyst estimates for +1Y and +2Y
        logger.info(f"Fetching analyst estimates for {symbol} revenue/margins...")
        estimates = {"year_1": {}, "year_2": {}}
        try:
            from datetime import datetime as dt
            today = dt.now().strftime('%Y-%m-%d')

            analyst_estimates = fmp_get(f"analyst-estimates/{symbol}", {"limit": 10})
            # Filter for future dates only and sort by date (nearest first)
            if analyst_estimates:
                future_estimates = [e for e in analyst_estimates if e.get('date', '') > today]
                analyst_estimates = sorted(future_estimates, key=lambda x: x.get('date', ''))
            if analyst_estimates and len(analyst_estimates) >= 1:
                # Year +1 estimates (nearest future year)
                est1 = analyst_estimates[0]
                est_rev_1 = est1.get("estimatedRevenueAvg", 0)
                est_ebitda_1 = est1.get("estimatedEbitdaAvg", 0)
                est_net_1 = est1.get("estimatedNetIncomeAvg", 0)
                estimates["year_1"] = {
                    "period": f"FY{est1.get('date', '')[:4]}E" if est1.get('date') else "+1Y",
                    "revenue": est_rev_1,
                    "gross_margin": None,  # Not available in estimates
                    "operating_margin": (est_ebitda_1 / est_rev_1 * 100) if est_rev_1 > 0 else None,
                    "net_margin": (est_net_1 / est_rev_1 * 100) if est_rev_1 > 0 else None
                }

            if analyst_estimates and len(analyst_estimates) >= 2:
                # Year +2 estimates
                est2 = analyst_estimates[1]
                est_rev_2 = est2.get("estimatedRevenueAvg", 0)
                est_ebitda_2 = est2.get("estimatedEbitdaAvg", 0)
                est_net_2 = est2.get("estimatedNetIncomeAvg", 0)
                estimates["year_2"] = {
                    "period": f"FY{est2.get('date', '')[:4]}E" if est2.get('date') else "+2Y",
                    "revenue": est_rev_2,
                    "gross_margin": None,  # Not available in estimates
                    "operating_margin": (est_ebitda_2 / est_rev_2 * 100) if est_rev_2 > 0 else None,
                    "net_margin": (est_net_2 / est_rev_2 * 100) if est_rev_2 > 0 else None
                }
        except Exception as est_err:
            logger.warning(f"Could not fetch analyst estimates: {est_err}")

        return {
            "segments": segment_data,
            "margins": margins,
            "historical_margins": historical_margins,
            "estimates": estimates
        }
    except Exception as e:
        logger.error(f" fetching revenue segments: {e}")

    return {"segments": [], "margins": {}, "historical_margins": [], "estimates": {}}


def get_competitive_advantages(symbol: str, language: str = "en") -> List[str]:
    """Get competitive advantages from company analysis"""
    advantages = []

    # Advantage translations
    advantage_texts = {
        "en": {
            "gross_margins": "Strong Gross Margins indicating pricing power and operational efficiency",
            "high_roe": "High Return on Equity demonstrating effective capital allocation",
            "strong_roa": "Strong Return on Assets showing efficient asset utilization",
            "superior_roic": "Superior Return on Invested Capital indicating competitive moat",
            "fcf_generation": "Consistent Free Cash Flow generation supporting growth and shareholder returns",
            "market_leadership": "Market leadership position with significant scale advantages",
            "analyzing": "Analyzing competitive positioning..."
        },
        "it": {
            "gross_margins": "Margini Lordi elevati che indicano potere di prezzo ed efficienza operativa",
            "high_roe": "Alto Return on Equity che dimostra un'efficace allocazione del capitale",
            "strong_roa": "Forte Return on Assets che mostra un utilizzo efficiente degli asset",
            "superior_roic": "ROIC superiore che indica un vantaggio competitivo sostenibile (moat)",
            "fcf_generation": "Generazione costante di Free Cash Flow a supporto della crescita e dei rendimenti agli azionisti",
            "market_leadership": "Posizione di leadership di mercato con significativi vantaggi di scala",
            "analyzing": "Analisi del posizionamento competitivo in corso..."
        }
    }
    texts = advantage_texts.get(language, advantage_texts["en"])

    try:
        # Get financial ratios to derive competitive advantages
        ratios = fmp_get(f"ratios-ttm/{symbol}")
        key_metrics = fmp_get(f"key-metrics-ttm/{symbol}")

        if ratios and len(ratios) > 0:
            ratio_data = ratios[0]

            # High margins indicate competitive advantage
            if ratio_data.get("grossProfitMarginTTM", 0) > 0.4:
                advantages.append(texts["gross_margins"])

            if ratio_data.get("returnOnEquityTTM", 0) > 0.15:
                advantages.append(texts["high_roe"])

            if ratio_data.get("returnOnAssetsTTM", 0) > 0.1:
                advantages.append(texts["strong_roa"])

        if key_metrics and len(key_metrics) > 0:
            metrics_data = key_metrics[0]

            if metrics_data.get("roicTTM", 0) > 0.12:
                advantages.append(texts["superior_roic"])

            # Check for strong cash generation
            if metrics_data.get("freeCashFlowPerShareTTM", 0) > 0:
                advantages.append(texts["fcf_generation"])

        # Get company profile for qualitative advantages
        profile = fmp_get(f"profile/{symbol}")
        if profile and len(profile) > 0:
            data = profile[0]
            if data.get("mktCap", 0) > 100000000000:  # >$100B market cap
                advantages.append(texts["market_leadership"])

    except Exception as e:
        logger.error(f" deriving competitive advantages: {e}")

    if not advantages:
        advantages.append(texts["analyzing"])

    return advantages


def get_key_metrics_data(symbol: str) -> Dict[str, Any]:
    """Get key financial metrics including revenue growth"""
    metrics = {}

    try:
        # Get income statement for revenue and net income
        income_stmt = fmp_get(f"income-statement/{symbol}", {"limit": 1})
        if income_stmt and len(income_stmt) > 0:
            data = income_stmt[0]
            metrics["revenue"] = data.get("revenue", 0)
            metrics["net_income"] = data.get("netIncome", 0)
            metrics["eps"] = data.get("eps", 0)
            metrics["gross_margin"] = data.get("grossProfitRatio", 0)
            metrics["operating_margin"] = data.get("operatingIncomeRatio", 0)
            # Calculate net income margin
            if metrics["revenue"] > 0:
                metrics["net_income_margin"] = (metrics["net_income"] / metrics["revenue"]) * 100
            else:
                metrics["net_income_margin"] = 0

        # Get cash flow statement for FCF
        cash_flow = fmp_get(f"cash-flow-statement/{symbol}", {"limit": 1})
        if cash_flow and len(cash_flow) > 0:
            data = cash_flow[0]
            metrics["free_cash_flow"] = data.get("freeCashFlow", 0)

        # Get ratios for ROE, ROA, ROIC (TTM)
        ratios = fmp_get(f"ratios-ttm/{symbol}")
        if ratios and len(ratios) > 0:
            data = ratios[0]
            metrics["roe"] = data.get("returnOnEquityTTM", 0) * 100  # Convert to percentage
            metrics["roa"] = data.get("returnOnAssetsTTM", 0) * 100  # Convert to percentage
            metrics["roic"] = data.get("returnOnCapitalEmployedTTM", 0) * 100  # Convert to percentage

        # Get WACC (Weighted Average Cost of Capital) from FMP Advanced DCF endpoint
        logger.debug(f"=== Fetching WACC for {symbol} ===")
        try:
            # Try FMP v4 advanced DCF endpoint first (includes WACC)
            url = f"{FMP_V4_BASE}/advanced_discounted_cash_flow"
            params = {"symbol": symbol, "apikey": FMP_API_KEY}
            logger.debug(f"Trying FMP Advanced DCF: {url}")
            response = requests.get(url, params=params, timeout=15)
            logger.debug(f"FMP Advanced DCF response status: {response.status_code}")
            if response.status_code == 200:
                dcf_data = response.json()
                logger.debug(f"FMP DCF data keys: {dcf_data[0].keys() if dcf_data and len(dcf_data) > 0 else 'no data'}")
                if dcf_data and len(dcf_data) > 0:
                    wacc_value = dcf_data[0].get("wacc", 0)
                    logger.debug(f"WACC value from API: {wacc_value}")
                    if wacc_value:
                        metrics["wacc"] = wacc_value * 100 if wacc_value < 1 else wacc_value  # Convert to percentage if decimal
                        logger.debug(f"WACC from FMP Advanced DCF: {metrics['wacc']}%")
                    else:
                        metrics["wacc"] = 0
                else:
                    metrics["wacc"] = 0
            else:
                logger.debug(f"FMP Advanced DCF failed: {response.text[:200]}")
                metrics["wacc"] = 0
        except Exception as e:
            logger.error(f" fetching WACC from FMP: {e}")
            metrics["wacc"] = 0

        # If FMP WACC not available, calculate manually
        if not metrics.get("wacc") or metrics["wacc"] == 0:
            logger.info(f"Calculating WACC manually for {symbol}...")
            try:
                # Get company profile for beta and market cap
                profile = fmp_get(f"profile/{symbol}")
                # Get balance sheet for debt
                balance_sheet = fmp_get(f"balance-sheet-statement/{symbol}", {"limit": 1})
                # Get income statement for interest expense and tax rate
                income_stmt_wacc = fmp_get(f"income-statement/{symbol}", {"limit": 1})

                if profile and len(profile) > 0 and balance_sheet and len(balance_sheet) > 0:
                    prof = profile[0]
                    bs = balance_sheet[0]
                    inc = income_stmt_wacc[0] if income_stmt_wacc and len(income_stmt_wacc) > 0 else {}

                    # Market value of equity (market cap)
                    market_cap = prof.get("mktCap", 0) or 0

                    # Total debt
                    total_debt = bs.get("totalDebt", 0) or 0

                    # Beta for CAPM
                    beta = prof.get("beta", 1.0) or 1.0

                    # Risk-free rate (approximate 10-year Treasury yield)
                    risk_free_rate = 0.045  # 4.5%

                    # Market risk premium (historical average)
                    market_risk_premium = 0.055  # 5.5%

                    # Cost of Equity using CAPM: Re = Rf + Beta * (Rm - Rf)
                    cost_of_equity = risk_free_rate + beta * market_risk_premium

                    # Cost of Debt: Interest Expense / Total Debt
                    interest_expense = inc.get("interestExpense", 0) or 0
                    if total_debt > 0 and interest_expense > 0:
                        cost_of_debt = interest_expense / total_debt
                    else:
                        cost_of_debt = 0.05  # Default 5% if not available

                    # Effective Tax Rate
                    income_before_tax = inc.get("incomeBeforeTax", 0) or 0
                    income_tax = inc.get("incomeTaxExpense", 0) or 0
                    if income_before_tax > 0 and income_tax > 0:
                        tax_rate = income_tax / income_before_tax
                    else:
                        tax_rate = 0.21  # Default US corporate tax rate

                    # Calculate weights
                    total_value = market_cap + total_debt
                    if total_value > 0:
                        weight_equity = market_cap / total_value
                        weight_debt = total_debt / total_value

                        # WACC calculation: WACC = (E/V) * Re + (D/V) * Rd * (1 - Tc)
                        wacc = (weight_equity * cost_of_equity) + (weight_debt * cost_of_debt * (1 - tax_rate))
                        metrics["wacc"] = wacc * 100  # Convert to percentage
                        logger.info(f"Calculated WACC: {metrics['wacc']:.2f}%")
            except Exception as e:
                logger.error(f" calculating WACC manually: {e}")

        # Final WACC check and debug
        logger.debug(f"=== Final WACC value: {metrics.get('wacc', 'NOT SET')} ===")

        # Get historical ratios for 5-year and 3-year averages (annual data)
        historical_ratios = fmp_get(f"ratios/{symbol}", {"limit": 6})
        if historical_ratios and len(historical_ratios) >= 2:
            roe_values = [r.get("returnOnEquity", 0) * 100 for r in historical_ratios]
            roa_values = [r.get("returnOnAssets", 0) * 100 for r in historical_ratios]
            roic_values = [r.get("returnOnCapitalEmployed", 0) * 100 for r in historical_ratios]

            # 3-year averages
            if len(historical_ratios) >= 3:
                metrics["roe_3yr"] = sum(roe_values[:3]) / 3
                metrics["roa_3yr"] = sum(roa_values[:3]) / 3
                metrics["roic_3yr"] = sum(roic_values[:3]) / 3
            else:
                metrics["roe_3yr"] = 0
                metrics["roa_3yr"] = 0
                metrics["roic_3yr"] = 0

            # 5-year averages
            if len(historical_ratios) >= 5:
                metrics["roe_5yr"] = sum(roe_values[:5]) / 5
                metrics["roa_5yr"] = sum(roa_values[:5]) / 5
                metrics["roic_5yr"] = sum(roic_values[:5]) / 5
            else:
                metrics["roe_5yr"] = 0
                metrics["roa_5yr"] = 0
                metrics["roic_5yr"] = 0
        else:
            # Set defaults if historical ratios not available
            metrics["roe_3yr"] = 0
            metrics["roa_3yr"] = 0
            metrics["roic_3yr"] = 0
            metrics["roe_5yr"] = 0
            metrics["roa_5yr"] = 0
            metrics["roic_5yr"] = 0

        # Calculate revenue growth rates and margin averages
        # Get historical income statements (6 years for 5-year avg)
        historical_income = fmp_get(f"income-statement/{symbol}", {"limit": 6})
        if historical_income and len(historical_income) >= 2:
            revenues = [stmt.get("revenue", 0) for stmt in historical_income]
            gross_margins = [stmt.get("grossProfitRatio", 0) * 100 for stmt in historical_income]
            operating_margins = [stmt.get("operatingIncomeRatio", 0) * 100 for stmt in historical_income]
            # Calculate net income margins from historical data
            net_income_margins = []
            for stmt in historical_income:
                rev = stmt.get("revenue", 0)
                ni = stmt.get("netIncome", 0)
                if rev > 0:
                    net_income_margins.append((ni / rev) * 100)
                else:
                    net_income_margins.append(0)

            # TTM growth (most recent vs previous year)
            if len(revenues) >= 2:
                metrics["revenue_growth_ttm"] = ((revenues[0] - revenues[1]) / revenues[1] * 100) if revenues[1] != 0 else 0

            # 3-year average growth
            if len(revenues) >= 4:
                growth_rates = []
                for i in range(3):
                    if revenues[i+1] != 0:
                        growth_rates.append((revenues[i] - revenues[i+1]) / revenues[i+1] * 100)
                metrics["revenue_growth_3yr"] = sum(growth_rates) / len(growth_rates) if growth_rates else 0

            # 5-year average growth
            if len(revenues) >= 6:
                growth_rates = []
                for i in range(5):
                    if revenues[i+1] != 0:
                        growth_rates.append((revenues[i] - revenues[i+1]) / revenues[i+1] * 100)
                metrics["revenue_growth_5yr"] = sum(growth_rates) / len(growth_rates) if growth_rates else 0

            # Calculate margin averages
            # 3-year average margins
            if len(gross_margins) >= 3:
                metrics["gross_margin_3yr"] = sum(gross_margins[:3]) / 3
                metrics["operating_margin_3yr"] = sum(operating_margins[:3]) / 3
                metrics["net_income_margin_3yr"] = sum(net_income_margins[:3]) / 3
            else:
                metrics["gross_margin_3yr"] = 0
                metrics["operating_margin_3yr"] = 0
                metrics["net_income_margin_3yr"] = 0

            # 5-year average margins
            if len(gross_margins) >= 5:
                metrics["gross_margin_5yr"] = sum(gross_margins[:5]) / 5
                metrics["operating_margin_5yr"] = sum(operating_margins[:5]) / 5
                metrics["net_income_margin_5yr"] = sum(net_income_margins[:5]) / 5
            else:
                metrics["gross_margin_5yr"] = 0
                metrics["operating_margin_5yr"] = 0
                metrics["net_income_margin_5yr"] = 0

        # Get analyst revenue estimates for future growth and margins
        try:
            from datetime import datetime
            today = datetime.now().strftime('%Y-%m-%d')

            # Fetch analyst estimates and filter for future dates only
            all_estimates = fmp_get(f"analyst-estimates/{symbol}", {"limit": 10})
            analyst_estimates = []
            if all_estimates:
                # Filter for future dates only and sort by date ascending (nearest first)
                future_estimates = [e for e in all_estimates if e.get('date', '') > today]
                analyst_estimates = sorted(future_estimates, key=lambda x: x.get('date', ''))[:2]
                logger.debug(f"Key Metrics: Filtered {len(all_estimates)} estimates to {len(analyst_estimates)} future estimates")

            if analyst_estimates and len(analyst_estimates) >= 1:
                # Get estimated revenue for next 1-2 years
                current_revenue = metrics.get("revenue", 0)

                # Get current TTM margins for projection baseline
                current_gross_margin = metrics.get("gross_margin", 0) * 100
                current_operating_margin = metrics.get("operating_margin", 0) * 100

                # 1-year estimated growth and margins
                if len(analyst_estimates) >= 1:
                    est_revenue_1yr = analyst_estimates[0].get("estimatedRevenueAvg", 0)
                    est_ebitda_1yr = analyst_estimates[0].get("estimatedEbitdaAvg", 0)
                    est_net_income_1yr = analyst_estimates[0].get("estimatedNetIncomeAvg", 0)

                    if current_revenue > 0 and est_revenue_1yr > 0:
                        metrics["revenue_growth_est_1yr"] = ((est_revenue_1yr - current_revenue) / current_revenue * 100)
                    else:
                        metrics["revenue_growth_est_1yr"] = 0

                    # Calculate estimated operating margin from EBITDA (EBITDA is close to operating income)
                    if est_revenue_1yr > 0 and est_ebitda_1yr:
                        metrics["operating_margin_est_1yr"] = (est_ebitda_1yr / est_revenue_1yr) * 100
                    else:
                        # If no EBITDA estimate, project from current margin
                        metrics["operating_margin_est_1yr"] = current_operating_margin

                    # Project gross margin (assume stable or slight improvement based on 3yr trend)
                    if len(gross_margins) >= 3:
                        # Calculate trend from last 3 years
                        recent_trend = (gross_margins[0] - gross_margins[2]) / 2  # Average annual change
                        metrics["gross_margin_est_1yr"] = gross_margins[0] + recent_trend
                    else:
                        # If no trend available, assume current margin continues
                        metrics["gross_margin_est_1yr"] = current_gross_margin

                    # Calculate estimated net income margin for year 1
                    if est_revenue_1yr > 0 and est_net_income_1yr:
                        metrics["net_income_margin_est_1yr"] = (est_net_income_1yr / est_revenue_1yr) * 100
                    else:
                        metrics["net_income_margin_est_1yr"] = 0

                # 2-year estimated growth and margins (from year 1 to year 2)
                if len(analyst_estimates) >= 2:
                    est_revenue_1yr = analyst_estimates[0].get("estimatedRevenueAvg", 0)
                    est_revenue_2yr = analyst_estimates[1].get("estimatedRevenueAvg", 0)
                    est_ebitda_2yr = analyst_estimates[1].get("estimatedEbitdaAvg", 0)
                    est_net_income_2yr = analyst_estimates[1].get("estimatedNetIncomeAvg", 0)

                    if est_revenue_1yr > 0 and est_revenue_2yr > 0:
                        metrics["revenue_growth_est_2yr"] = ((est_revenue_2yr - est_revenue_1yr) / est_revenue_1yr * 100)
                    else:
                        metrics["revenue_growth_est_2yr"] = 0

                    # Calculate estimated operating margin from EBITDA
                    if est_revenue_2yr > 0 and est_ebitda_2yr:
                        metrics["operating_margin_est_2yr"] = (est_ebitda_2yr / est_revenue_2yr) * 100
                    else:
                        # Project from year 1 estimate
                        metrics["operating_margin_est_2yr"] = metrics.get("operating_margin_est_1yr", current_operating_margin)

                    # Project gross margin for year 2
                    if len(gross_margins) >= 3:
                        recent_trend = (gross_margins[0] - gross_margins[2]) / 2
                        metrics["gross_margin_est_2yr"] = gross_margins[0] + (recent_trend * 2)
                    else:
                        metrics["gross_margin_est_2yr"] = current_gross_margin

                    # Calculate estimated net income margin for year 2
                    if est_revenue_2yr > 0 and est_net_income_2yr:
                        metrics["net_income_margin_est_2yr"] = (est_net_income_2yr / est_revenue_2yr) * 100
                    else:
                        metrics["net_income_margin_est_2yr"] = 0
                else:
                    # Only 1 estimate available, set year 2 defaults
                    metrics["revenue_growth_est_2yr"] = 0
                    metrics["gross_margin_est_2yr"] = 0
                    metrics["operating_margin_est_2yr"] = 0
                    metrics["net_income_margin_est_2yr"] = 0
            else:
                # Set defaults if no estimates available
                metrics["revenue_growth_est_1yr"] = 0
                metrics["revenue_growth_est_2yr"] = 0
                metrics["gross_margin_est_1yr"] = 0
                metrics["gross_margin_est_2yr"] = 0
                metrics["operating_margin_est_1yr"] = 0
                metrics["operating_margin_est_2yr"] = 0
                metrics["net_income_margin_est_1yr"] = 0
                metrics["net_income_margin_est_2yr"] = 0
        except (requests.RequestException, KeyError, IndexError, TypeError, ValueError) as e:
            # If analyst estimates not available, set to 0
            logger.warning(f" Could not fetch analyst estimates: {e}")
            metrics["revenue_growth_est_1yr"] = 0
            metrics["revenue_growth_est_2yr"] = 0
            metrics["gross_margin_est_1yr"] = 0
            metrics["gross_margin_est_2yr"] = 0
            metrics["operating_margin_est_1yr"] = 0
            metrics["operating_margin_est_2yr"] = 0
            metrics["net_income_margin_est_1yr"] = 0
            metrics["net_income_margin_est_2yr"] = 0

    except Exception as e:
        logger.error(f" fetching key metrics: {e}")

    return metrics


def get_risks(symbol: str, language: str = "en") -> Dict[str, List[str]]:
    """AI-enhanced risk analysis focusing on company-specific and general risks"""
    # Get language instruction for AI prompts
    lang_instruction = get_translation("ai_language_instruction", language)
    company_specific_risks = []
    general_risks = []

    try:
        # Get comprehensive financial data
        ratios = fmp_get(f"ratios-ttm/{symbol}")
        key_metrics = fmp_get(f"key-metrics-ttm/{symbol}")
        profile = fmp_get(f"profile/{symbol}")

        # Get historical data for trend analysis
        historical_ratios = fmp_get(f"ratios/{symbol}", {"limit": 8})
        income_statements = fmp_get(f"income-statement/{symbol}", {"limit": 8})
        balance_sheets = fmp_get(f"balance-sheet-statement/{symbol}", {"limit": 8})
        cash_flow_statements = fmp_get(f"cash-flow-statement/{symbol}", {"limit": 8})
        key_executives = fmp_get(f"key-executives/{symbol}")

        # === COMPANY SPECIFIC RISKS ===

        # 1. Declining Margins Analysis
        if historical_ratios and len(historical_ratios) >= 4:
            gross_margins = [r.get("grossProfitMargin", 0) * 100 for r in historical_ratios[:4]]
            operating_margins = [r.get("operatingProfitMargin", 0) * 100 for r in historical_ratios[:4]]
            net_margins = [r.get("netProfitMargin", 0) * 100 for r in historical_ratios[:4]]

            # Check for declining trend (comparing most recent 2 years vs previous 2 years)
            if len(gross_margins) >= 4:
                recent_gross = sum(gross_margins[:2]) / 2
                older_gross = sum(gross_margins[2:4]) / 2
                if older_gross > 0 and (recent_gross - older_gross) < -2:
                    company_specific_risks.append(f"Declining gross margin trend: {recent_gross:.1f}% (recent) vs {older_gross:.1f}% (prior period)")

            if len(operating_margins) >= 4:
                recent_op = sum(operating_margins[:2]) / 2
                older_op = sum(operating_margins[2:4]) / 2
                if older_op > 0 and (recent_op - older_op) < -2:
                    company_specific_risks.append(f"Declining operating margin trend: {recent_op:.1f}% (recent) vs {older_op:.1f}% (prior period)")

        # 2. Declining Operating Cash Flow
        if cash_flow_statements and len(cash_flow_statements) >= 4:
            ocf_values = [cf.get("operatingCashFlow", 0) for cf in cash_flow_statements[:4]]
            if ocf_values[0] < ocf_values[1] and ocf_values[1] < ocf_values[2]:
                decline_pct = ((ocf_values[0] - ocf_values[2]) / abs(ocf_values[2]) * 100) if ocf_values[2] != 0 else 0
                company_specific_risks.append(f"Operating cash flow declining: ${ocf_values[0]/1e9:.2f}B vs ${ocf_values[2]/1e9:.2f}B two years ago ({decline_pct:.1f}% change)")

        # 3. High Net Debt
        if balance_sheets and len(balance_sheets) > 0:
            bs = balance_sheets[0]
            total_debt = bs.get("totalDebt", 0)
            cash = bs.get("cashAndCashEquivalents", 0)
            net_debt = total_debt - cash
            total_equity = bs.get("totalStockholdersEquity", 0)

            if net_debt > 0 and total_equity > 0:
                net_debt_to_equity = net_debt / total_equity
                if net_debt_to_equity > 1.0:
                    company_specific_risks.append(f"High net debt of ${net_debt/1e9:.2f}B with Net Debt/Equity ratio of {net_debt_to_equity:.2f}")

        # 4. Large Increase in Short-Term Debt
        if balance_sheets and len(balance_sheets) >= 3:
            current_st_debt = balance_sheets[0].get("shortTermDebt", 0)
            prev_st_debt = balance_sheets[2].get("shortTermDebt", 0)

            if prev_st_debt > 0 and current_st_debt > prev_st_debt * 1.5:
                increase_pct = ((current_st_debt - prev_st_debt) / prev_st_debt * 100)
                company_specific_risks.append(f"Significant increase in short-term debt: ${current_st_debt/1e9:.2f}B vs ${prev_st_debt/1e9:.2f}B ({increase_pct:.1f}% increase)")

        # 5. Management Changes (CEO, CFO, COO) - AI-powered analysis
        # Scan 8-K filings and recent 10-K/10-Q for management changes
        logger.info(f"Scanning filings for management changes and auditor changes...")
        try:
            # Get recent 8-K filings (Item 5.02 is for executive officer departures/appointments)
            filings_8k = fmp_get(f"sec_filings/{symbol}", {"type": "8-K", "limit": 10})

            # Get recent 10-K and 10-Q
            filings_10k = fmp_get(f"sec_filings/{symbol}", {"type": "10-K", "limit": 2})
            filings_10q = fmp_get(f"sec_filings/{symbol}", {"type": "10-Q", "limit": 4})

            # Combine all filings for AI analysis
            filing_contents = []

            # Process 8-K filings (most likely to contain management change announcements)
            if filings_8k and isinstance(filings_8k, list):
                for filing in filings_8k[:5]:  # Check last 5 8-K filings
                    filing_url = filing.get("finalLink", "")
                    filing_date = filing.get("fillingDate", "")
                    if filing_url:
                        try:
                            response = requests.get(filing_url, timeout=20)
                            if response.status_code == 200:
                                filing_contents.append({
                                    "type": "8-K",
                                    "date": filing_date,
                                    "content": response.text[:20000]  # First 20k chars
                                })
                        except requests.RequestException as e:
                            logger.warning(f" Could not fetch 8-K filing: {e}")
                            continue

            # Process recent 10-K
            if filings_10k and isinstance(filings_10k, list):
                for filing in filings_10k[:2]:
                    filing_url = filing.get("finalLink", "")
                    filing_date = filing.get("fillingDate", "")
                    if filing_url:
                        try:
                            response = requests.get(filing_url, timeout=20)
                            if response.status_code == 200:
                                filing_contents.append({
                                    "type": "10-K",
                                    "date": filing_date,
                                    "content": response.text[:30000]
                                })
                        except requests.RequestException as e:
                            logger.warning(f" Could not fetch 10-K filing: {e}")
                            continue

            # Use AI to analyze filings for management and auditor changes
            if filing_contents:
                combined_filings = "\n\n=== FILING SEPARATOR ===\n\n".join([
                    f"Filing Type: {f['type']}\nDate: {f['date']}\nContent: {f['content']}"
                    for f in filing_contents
                ])

                ai_prompt = f"""You are analyzing SEC filings (8-K, 10-K, 10-Q) for {symbol} to identify material changes.

Analyze the provided filings and identify:

1. **Management Changes**: Any departures, appointments, or resignations of:
   - CEO (Chief Executive Officer)
   - CFO (Chief Financial Officer)
   - COO (Chief Operating Officer)
   - Other C-suite executives

2. **Auditor Changes**: Any changes in the independent registered public accounting firm (auditor)

For each change found, provide:
- Type of change (Management or Auditor)
- Position/role affected
- Date (if mentioned)
- Brief context (resignation, retirement, appointment, etc.)

Format your response as:
MANAGEMENT_CHANGE: [Position] - [Name if available] - [Date] - [Context]
AUDITOR_CHANGE: [Old Auditor] to [New Auditor] - [Date] - [Reason if stated]

If no significant changes found, respond with: NO_CHANGES_FOUND

Be concise and focus only on C-suite management changes and auditor changes."""

                # Try OpenAI first for this analysis (good at structured extraction)
                ai_analysis = analyze_with_ai(ai_prompt, combined_filings[:50000], use_claude=False, language=language)

                # If OpenAI fails, try Claude
                if "Error" in ai_analysis or "unavailable" in ai_analysis:
                    ai_analysis = analyze_with_ai(ai_prompt, combined_filings[:50000], use_claude=True, language=language)

                # Parse AI response and add to risks
                if ai_analysis and "NO_CHANGES_FOUND" not in ai_analysis:
                    for line in ai_analysis.split('\n'):
                        if "MANAGEMENT_CHANGE:" in line:
                            change_details = line.replace("MANAGEMENT_CHANGE:", "").strip()
                            company_specific_risks.append(f"Recent management change: {change_details}")
                        elif "AUDITOR_CHANGE:" in line:
                            change_details = line.replace("AUDITOR_CHANGE:", "").strip()
                            company_specific_risks.append(f"Auditor change detected: {change_details}")

        except Exception as e:
            logger.error(f" analyzing filings for management/auditor changes: {e}")

        # 6. Unusual Increase in Accounts Receivable or DSO
        if balance_sheets and len(balance_sheets) >= 3 and income_statements and len(income_statements) >= 3:
            current_ar = balance_sheets[0].get("netReceivables", 0)
            prev_ar = balance_sheets[2].get("netReceivables", 0)
            current_revenue = income_statements[0].get("revenue", 0)
            prev_revenue = income_statements[2].get("revenue", 0)

            # Calculate DSO (Days Sales Outstanding)
            if current_revenue > 0:
                current_dso = (current_ar / current_revenue) * 365
                prev_dso = (prev_ar / prev_revenue) * 365 if prev_revenue > 0 else 0

                if prev_dso > 0 and (current_dso - prev_dso) > 10:
                    company_specific_risks.append(f"Increasing Days Sales Outstanding (DSO): {current_dso:.0f} days vs {prev_dso:.0f} days, indicating potential collection issues")

            # Check for unusual AR growth vs revenue growth
            if prev_ar > 0 and prev_revenue > 0:
                ar_growth = (current_ar - prev_ar) / prev_ar * 100
                revenue_growth = (current_revenue - prev_revenue) / prev_revenue * 100

                if ar_growth > revenue_growth + 15:
                    company_specific_risks.append(f"Accounts receivable growing faster than revenue: AR up {ar_growth:.1f}% vs revenue up {revenue_growth:.1f}%")

        # 7. Traditional checks
        if ratios and len(ratios) > 0:
            ratio_data = ratios[0]

            # High debt levels
            debt_to_equity = ratio_data.get("debtEquityRatioTTM", 0)
            if debt_to_equity > 1.5:
                company_specific_risks.append(f"High leverage with Debt/Equity ratio of {debt_to_equity:.2f} may limit financial flexibility")

            # Low current ratio
            current_ratio = ratio_data.get("currentRatioTTM", 0)
            if current_ratio < 1.0:
                company_specific_risks.append(f"Current ratio of {current_ratio:.2f} indicates potential liquidity concerns")

        if key_metrics and len(key_metrics) > 0:
            metrics_data = key_metrics[0]

            # Negative FCF
            fcf_per_share = metrics_data.get("freeCashFlowPerShareTTM", 0)
            if fcf_per_share < 0:
                company_specific_risks.append("Negative free cash flow may require external financing for operations")

        # === GENERAL RISKS ===
        # AI-powered analysis of Industry, Technology, Regulatory, and Competition risks

        logger.info(f"Analyzing general risks (Industry, Technology, Regulatory, Competition) for {symbol}...")
        try:
            # Fetch annual report (10-K) for risk factor analysis
            annual_report_content = ""
            filings_10k_general = fmp_get(f"sec_filings/{symbol}", {"type": "10-K", "limit": 1})
            if filings_10k_general and isinstance(filings_10k_general, list) and len(filings_10k_general) > 0:
                filing_url = filings_10k_general[0].get("finalLink", "")
                if filing_url:
                    try:
                        response = requests.get(filing_url, timeout=20)
                        if response.status_code == 200:
                            # Extract Risk Factors section from 10-K
                            annual_report_content = response.text[:100000]  # First 100k chars
                    except requests.RequestException as e:
                        logger.warning(f" Could not fetch annual report for risk analysis: {e}")

            # Fetch recent news articles
            news_content = ""
            try:
                news_articles = fmp_get(f"stock_news", {"tickers": symbol, "limit": 20})
                if news_articles and isinstance(news_articles, list):
                    news_summaries = []
                    for article in news_articles[:20]:
                        title = article.get("title", "")
                        text = article.get("text", "")
                        published = article.get("publishedDate", "")
                        if title and text:
                            news_summaries.append(f"[{published}] {title}: {text[:500]}")

                    if news_summaries:
                        news_content = "\n\n".join(news_summaries)
            except Exception as e:
                logger.error(f" fetching news: {e}")

            # Combine sources for AI analysis
            if annual_report_content or news_content:
                combined_sources = f"""
=== ANNUAL REPORT (10-K) RISK FACTORS ===
{annual_report_content}

=== RECENT NEWS ARTICLES ===
{news_content}
"""

                ai_prompt = f"""You are analyzing risks for {symbol} focusing on GENERAL/EXTERNAL risks (Industry, Technology, Regulatory, Competition).

Based on the annual report Risk Factors section and recent news articles, identify key general risks in these categories:

1. **Industry Risks**: Macro trends, market conditions, cyclicality, demand shifts affecting the entire industry
2. **Technology Risks**: Technological disruption, obsolescence, rapid innovation, emerging technologies threatening the business model
3. **Regulatory Risks**: Government regulations, policy changes, compliance requirements, legal risks, trade policies
4. **Competition Risks**: Competitive landscape, market share threats, new entrants, pricing pressure from competitors

For each risk identified, provide:
- Category (Industry/Technology/Regulatory/Competition)
- Specific risk description (one concise sentence)

Format your response as:
INDUSTRY_RISK: [Specific risk description]
TECHNOLOGY_RISK: [Specific risk description]
REGULATORY_RISK: [Specific risk description]
COMPETITION_RISK: [Specific risk description]

Focus on material, actionable risks. Limit to the 3-5 most significant risks across all categories.
If no significant general risks found in a category, skip it."""

                # Use Claude for this analysis (better at nuanced risk interpretation)
                general_risk_analysis = analyze_with_ai(ai_prompt, combined_sources[:80000], use_claude=True, language=language)

                # If Claude fails, try OpenAI
                if "Error" in general_risk_analysis or "unavailable" in general_risk_analysis:
                    logger.info("Claude failed for general risk analysis, trying OpenAI...")
                    general_risk_analysis = analyze_with_ai(ai_prompt, combined_sources[:80000], use_claude=False, language=language)

                # Parse AI response and categorize risks
                if general_risk_analysis:
                    for line in general_risk_analysis.split('\n'):
                        line = line.strip()
                        if "INDUSTRY_RISK:" in line:
                            risk = line.replace("INDUSTRY_RISK:", "").strip()
                            if risk:
                                general_risks.append(f"Industry: {risk}")
                        elif "TECHNOLOGY_RISK:" in line:
                            risk = line.replace("TECHNOLOGY_RISK:", "").strip()
                            if risk:
                                general_risks.append(f"Technology: {risk}")
                        elif "REGULATORY_RISK:" in line:
                            risk = line.replace("REGULATORY_RISK:", "").strip()
                            if risk:
                                general_risks.append(f"Regulatory: {risk}")
                        elif "COMPETITION_RISK:" in line:
                            risk = line.replace("COMPETITION_RISK:", "").strip()
                            if risk:
                                general_risks.append(f"Competition: {risk}")

        except Exception as e:
            logger.error(f" analyzing general risks: {e}")
            import traceback
            traceback.print_exc()

        # Add traditional general risk metrics
        if profile and len(profile) > 0:
            data = profile[0]

            # Beta risk
            beta = data.get("beta", 1.0)
            if beta > 1.5:
                general_risks.append(f"Market Volatility: High beta of {beta:.2f} indicates elevated volatility relative to market")

        if key_metrics and len(key_metrics) > 0:
            metrics_data = key_metrics[0]

            # High P/E ratio
            pe_ratio = metrics_data.get("peRatioTTM", 0)
            if pe_ratio > 40:
                general_risks.append(f"Valuation: Elevated P/E ratio of {pe_ratio:.1f} suggests high valuation expectations with limited margin for disappointment")

    except Exception as e:
        logger.error(f" analyzing risks: {e}")
        import traceback
        traceback.print_exc()

    if not company_specific_risks and not general_risks:
        company_specific_risks.append("No significant risk factors identified based on current financial metrics")

    return {
        "company_specific": company_specific_risks,
        "general": general_risks
    }


def get_recent_highlights(symbol: str, language: str = "en") -> Dict[str, Any]:
    """Get highlights from recent quarters with structured table data and QoQ commentary"""
    result = {
        "quarterly_data": [],  # Structured data for table
        "qoq_commentary": [],  # QoQ change commentary
        "highlights": [],      # Legacy format
        "ai_summary": ""
    }

    try:
        # Get quarterly earnings data
        earnings = fmp_get(f"income-statement/{symbol}", {"period": "quarter", "limit": 5})

        # Get quarterly balance sheets (for deferred revenue, etc.)
        balance_sheets_q = fmp_get(f"balance-sheet-statement/{symbol}", {"period": "quarter", "limit": 5})

        # Get quarterly cash flow statements
        cash_flows_q = fmp_get(f"cash-flow-statement/{symbol}", {"period": "quarter", "limit": 5})

        if earnings and isinstance(earnings, list):
            # Build structured quarterly data
            for i, quarter in enumerate(earnings[:4]):
                date = quarter.get("date", "")
                period = quarter.get("period", "")
                fiscal_year = quarter.get("calendarYear", "")
                quarter_label = f"{period} {fiscal_year}"

                revenue = quarter.get("revenue", 0)
                gross_profit = quarter.get("grossProfit", 0)
                operating_income = quarter.get("operatingIncome", 0)
                net_income = quarter.get("netIncome", 0)
                eps = quarter.get("eps", 0)

                # Calculate margins
                gross_margin = (gross_profit / revenue * 100) if revenue > 0 else 0
                operating_margin = (operating_income / revenue * 100) if revenue > 0 else 0
                net_margin = (net_income / revenue * 100) if revenue > 0 else 0

                # Get deferred revenue from balance sheet
                deferred_revenue = 0
                if balance_sheets_q and isinstance(balance_sheets_q, list) and i < len(balance_sheets_q):
                    deferred_revenue = balance_sheets_q[i].get("deferredRevenue", 0)

                # Get operating cash flow
                ocf = 0
                if cash_flows_q and isinstance(cash_flows_q, list) and i < len(cash_flows_q):
                    ocf = cash_flows_q[i].get("operatingCashFlow", 0)

                result["quarterly_data"].append({
                    "quarter": quarter_label,
                    "date": date,
                    "revenue": revenue,
                    "gross_profit": gross_profit,
                    "gross_margin": gross_margin,
                    "operating_income": operating_income,
                    "operating_margin": operating_margin,
                    "net_income": net_income,
                    "net_margin": net_margin,
                    "eps": eps,
                    "deferred_revenue": deferred_revenue,
                    "operating_cash_flow": ocf
                })

            # Calculate QoQ changes and generate commentary
            positive_changes = []
            negative_changes = []

            # QoQ commentary translations
            qoq_texts = {
                "en": {
                    "rev_increased": "Revenue increased {:.1f}% QoQ ({} to {})",
                    "rev_declined": "Revenue declined {:.1f}% QoQ ({} to {})",
                    "eps_grew": "EPS grew {:.1f}% QoQ to ${:.2f}",
                    "eps_declined": "EPS declined {:.1f}% QoQ to ${:.2f}",
                    "gross_expanded": "Gross margin expanded {:.1f}pp to {:.1f}%",
                    "gross_contracted": "Gross margin contracted {:.1f}pp to {:.1f}%",
                    "op_improved": "Operating margin improved {:.1f}pp to {:.1f}%",
                    "op_declined": "Operating margin declined {:.1f}pp to {:.1f}%",
                    "def_rev_grew": "Deferred revenue grew {:.1f}% QoQ (future revenue indicator)",
                    "def_rev_declined": "Deferred revenue declined {:.1f}% QoQ"
                },
                "it": {
                    "rev_increased": "Ricavi aumentati del {:.1f}% QoQ ({} a {})",
                    "rev_declined": "Ricavi diminuiti del {:.1f}% QoQ ({} a {})",
                    "eps_grew": "EPS cresciuto del {:.1f}% QoQ a ${:.2f}",
                    "eps_declined": "EPS diminuito del {:.1f}% QoQ a ${:.2f}",
                    "gross_expanded": "Margine lordo espanso di {:.1f}pp a {:.1f}%",
                    "gross_contracted": "Margine lordo contratto di {:.1f}pp a {:.1f}%",
                    "op_improved": "Margine operativo migliorato di {:.1f}pp a {:.1f}%",
                    "op_declined": "Margine operativo diminuito di {:.1f}pp a {:.1f}%",
                    "def_rev_grew": "Ricavi differiti cresciuti del {:.1f}% QoQ (indicatore di ricavi futuri)",
                    "def_rev_declined": "Ricavi differiti diminuiti del {:.1f}% QoQ"
                }
            }
            qoq = qoq_texts.get(language, qoq_texts["en"])

            for i in range(len(result["quarterly_data"]) - 1):
                current = result["quarterly_data"][i]
                previous = result["quarterly_data"][i + 1]
                curr_q = current["quarter"]
                prev_q = previous["quarter"]

                # Revenue change
                if previous["revenue"] > 0:
                    rev_change = ((current["revenue"] - previous["revenue"]) / previous["revenue"]) * 100
                    if rev_change > 0:
                        positive_changes.append(qoq["rev_increased"].format(rev_change, prev_q, curr_q))
                    elif rev_change < -1:
                        negative_changes.append(qoq["rev_declined"].format(abs(rev_change), prev_q, curr_q))

                # EPS change
                if previous["eps"] != 0:
                    eps_change = ((current["eps"] - previous["eps"]) / abs(previous["eps"])) * 100
                    if eps_change > 5:
                        positive_changes.append(qoq["eps_grew"].format(eps_change, current['eps']))
                    elif eps_change < -5:
                        negative_changes.append(qoq["eps_declined"].format(abs(eps_change), current['eps']))

                # Gross margin change
                margin_change = current["gross_margin"] - previous["gross_margin"]
                if margin_change > 0.5:
                    positive_changes.append(qoq["gross_expanded"].format(margin_change, current['gross_margin']))
                elif margin_change < -0.5:
                    negative_changes.append(qoq["gross_contracted"].format(abs(margin_change), current['gross_margin']))

                # Operating margin change
                op_margin_change = current["operating_margin"] - previous["operating_margin"]
                if op_margin_change > 0.5:
                    positive_changes.append(qoq["op_improved"].format(op_margin_change, current['operating_margin']))
                elif op_margin_change < -0.5:
                    negative_changes.append(qoq["op_declined"].format(abs(op_margin_change), current['operating_margin']))

                # Deferred revenue change (important for SaaS)
                if previous["deferred_revenue"] > 0 and current["deferred_revenue"] > 0:
                    def_rev_change = ((current["deferred_revenue"] - previous["deferred_revenue"]) / previous["deferred_revenue"]) * 100
                    if def_rev_change > 3:
                        positive_changes.append(qoq["def_rev_grew"].format(def_rev_change))
                    elif def_rev_change < -3:
                        negative_changes.append(qoq["def_rev_declined"].format(abs(def_rev_change)))

                # Only report most recent quarter changes (i=0)
                if i == 0:
                    break

            result["qoq_commentary"] = {
                "positive": positive_changes[:5],  # Top 5 positive changes
                "negative": negative_changes[:5]   # Top 5 negative changes
            }

            # Build legacy highlights format for backward compatibility
            for i, quarter in enumerate(earnings[:4]):
                date = quarter.get("date", "")
                period = quarter.get("period", "")
                fiscal_year = quarter.get("calendarYear", "")
                revenue = quarter.get("revenue", 0)
                net_income = quarter.get("netIncome", 0)
                eps = quarter.get("eps", 0)

                growth_text = ""
                if i < len(earnings) - 1:
                    prev_quarter = earnings[i + 1]
                    prev_revenue = prev_quarter.get("revenue", 0)
                    if prev_revenue > 0:
                        growth = ((revenue - prev_revenue) / prev_revenue) * 100
                        growth_text = f"Revenue: ${revenue/1e9:.2f}B ({growth:+.1f}% QoQ)"
                    else:
                        growth_text = f"Revenue: ${revenue/1e9:.2f}B"
                else:
                    growth_text = f"Revenue: ${revenue/1e9:.2f}B"

                details = [
                    growth_text,
                    f"Net Income: ${net_income/1e9:.2f}B",
                    f"EPS: ${eps:.2f}"
                ]

                if balance_sheets_q and isinstance(balance_sheets_q, list) and i < len(balance_sheets_q):
                    bs = balance_sheets_q[i]
                    deferred_revenue = bs.get("deferredRevenue", 0)
                    if deferred_revenue > 0:
                        details.append(f"Deferred Revenue: ${deferred_revenue/1e9:.2f}B")

                if cash_flows_q and isinstance(cash_flows_q, list) and i < len(cash_flows_q):
                    cf = cash_flows_q[i]
                    ocf = cf.get("operatingCashFlow", 0)
                    if ocf != 0:
                        details.append(f"Operating Cash Flow: ${ocf/1e9:.2f}B")

                result["highlights"].append({
                    "quarter": f"{period} {fiscal_year}",
                    "date": date,
                    "details": details
                })

        # Get earnings surprises for additional context
        try:
            surprises = fmp_get(f"earnings-surprises/{symbol}", {"limit": 4})
            if surprises and isinstance(surprises, list):
                for i, surprise in enumerate(surprises[:len(result["highlights"])]):
                    actual_eps = surprise.get("actualEarningResult", 0)
                    estimated_eps = surprise.get("estimatedEarning", 0)
                    if estimated_eps != 0:
                        surprise_pct = ((actual_eps - estimated_eps) / abs(estimated_eps)) * 100
                        if abs(surprise_pct) > 1:
                            result["highlights"][i]["details"].append(
                                f"EPS Surprise: {surprise_pct:+.1f}% vs estimates"
                            )
                        # Also add to quarterly_data
                        if i < len(result["quarterly_data"]):
                            result["quarterly_data"][i]["eps_surprise"] = surprise_pct
        except (requests.RequestException, KeyError, IndexError, TypeError, ZeroDivisionError) as e:
            logger.warning(f" Could not fetch earnings surprises: {e}")

        # Add AI-enhanced quarterly trends analysis
        logger.info(f"Fetching quarterly reports and transcripts for trends analysis...")
        quarterly_reports = fetch_quarterly_reports(symbol, limit=4)
        transcripts = fetch_earnings_transcripts(symbol, limit=4)

        analysis_sources = []
        if quarterly_reports:
            for qreport in quarterly_reports:
                analysis_sources.append((f"10-Q {qreport['date']}", qreport['content']))

        if transcripts:
            for transcript in transcripts:
                analysis_sources.append((f"Earnings Call {transcript['quarter']}", transcript['content']))

        if analysis_sources:
            logger.info(f"Analyzing quarterly trends with AI from {len(analysis_sources)} sources...")

            combined_content = "\n\n=== NEXT SOURCE ===\n\n".join(
                [f"Source: {source[0]}\n{source[1]}" for source in analysis_sources]
            )

            ai_prompt = f"""You are analyzing quarterly reports (10-Q) and earnings call transcripts for {symbol}.

CRITICAL: Extract and highlight ALL important quarterly metrics and business drivers. DO NOT miss key metrics like RPO, deferred revenue, backlog, etc.

Based on the provided sources, create a comprehensive quarterly analysis covering:

1. **Critical Financial Metrics** (MUST INCLUDE if mentioned):
   - RPO (Remaining Performance Obligations) - with dollar amounts and % growth
   - Deferred Revenue - current and changes
   - Backlog or Unbilled Revenue
   - Cloud/Subscription Revenue - trends and growth rates
   - Contract Values (TCV, ACV, ARR, MRR)
   - Billings and bookings
   - Customer metrics (new customers, churn, retention, NRR)

2. **Segment Performance Trends**:
   - Revenue by segment with specific numbers
   - Growth rates by segment (QoQ and YoY)
   - Segment mix changes

3. **Key Highlights**:
   - Notable achievements, product launches, milestones
   - Record metrics or all-time highs
   - Strategic wins (major customer deals, partnerships)

4. **Guidance and Outlook**:
   - Updated guidance for future quarters/year
   - Management commentary on trends
   - Forward-looking statements

5. **Challenges** (if any):
   - Headwinds or issues mentioned
   - Areas of concern

FORMAT: For each quarter, provide specific metrics with dollar amounts and percentages. Be quantitative, not qualitative.

Example format:
"Q3 2024: RPO increased 25% YoY to $80.5B, indicating strong future revenue. Cloud revenue grew 30% to $15.2B..."

Focus on being comprehensive and specific with numbers. DO NOT generalize - provide exact figures when available."""

            quarterly_analysis = analyze_with_ai(ai_prompt, combined_content, use_claude=False, language=language)

            if "Error" in quarterly_analysis or "unavailable" in quarterly_analysis:
                logger.info("OpenAI analysis failed, trying Claude...")
                quarterly_analysis = analyze_with_ai(ai_prompt, combined_content, use_claude=True, language=language)

            # Add AI analysis as a summary
            if quarterly_analysis:
                result["ai_summary"] = quarterly_analysis
                # Also add to legacy format for backward compatibility
                if result["highlights"]:
                    result["highlights"][0]["ai_summary"] = quarterly_analysis

            # Extract key business drivers specific to this company
            logger.info(f"Extracting key business drivers for {symbol}...")
            drivers_prompt = f"""Analyze this company ({symbol}) and identify the 3-5 MOST IMPORTANT key performance indicators (KPIs) that drive this specific business.

Different companies have different key drivers:
- Cloud/SaaS companies: RPO, ARR, NRR, Cloud Revenue, Subscription Growth
- Industrial companies: Backlog, Book-to-Bill, Orders, Unit Shipments
- Retail: Same-store sales, Comparable sales, Traffic, Average ticket
- Financial services: AUM, Net flows, NIM, Loan growth
- Healthcare: Patient volumes, Procedures, Rx volumes
- Semiconductors: Wafer starts, ASPs, Utilization rates

Based on the earnings transcripts and reports, extract the KEY BUSINESS DRIVERS for {symbol}.

Return in this EXACT format (one per line):
DRIVER: [Metric Name] | VALUE: [Current Value] | CHANGE: [% change or trend] | INSIGHT: [Brief 10-word max insight]

Example for Oracle:
DRIVER: RPO (Remaining Performance Obligations) | VALUE: $80.5B | CHANGE: +25% YoY | INSIGHT: Strong future revenue visibility
DRIVER: Cloud Revenue | VALUE: $15.2B | CHANGE: +30% YoY | INSIGHT: Cloud transition accelerating

Only include metrics that are ACTUALLY MENTIONED in the sources. Do not make up data.
Return 3-5 drivers maximum, focusing on the MOST IMPORTANT ones for this specific company."""

            drivers_response = analyze_with_ai(drivers_prompt, combined_content, use_claude=False, language=language)

            if "Error" in drivers_response or "unavailable" in drivers_response:
                drivers_response = analyze_with_ai(drivers_prompt, combined_content, use_claude=True, language=language)

            # Parse the drivers response
            key_drivers = []
            if drivers_response and "DRIVER:" in drivers_response:
                for line in drivers_response.split('\n'):
                    if line.strip().startswith('DRIVER:'):
                        try:
                            parts = line.split('|')
                            if len(parts) >= 3:
                                driver_name = parts[0].replace('DRIVER:', '').strip()
                                value = parts[1].replace('VALUE:', '').strip() if len(parts) > 1 else ''
                                change = parts[2].replace('CHANGE:', '').strip() if len(parts) > 2 else ''
                                insight = parts[3].replace('INSIGHT:', '').strip() if len(parts) > 3 else ''
                                key_drivers.append({
                                    'name': driver_name,
                                    'value': value,
                                    'change': change,
                                    'insight': insight
                                })
                        except Exception as e:
                            logger.error(f" parsing driver line: {e}")
                            continue

            result["key_drivers"] = key_drivers
            logger.info(f"Extracted {len(key_drivers)} key business drivers")

    except Exception as e:
        logger.error(f" fetching recent highlights: {e}")

    return result


def get_competition(symbol: str) -> List[Dict[str, Any]]:
    """Get competitor information"""
    competitors = []

    try:
        # Get company profile to find industry
        profile = fmp_get(f"profile/{symbol}")
        if not profile or len(profile) == 0:
            return competitors

        industry = profile[0].get("industry", "")
        sector = profile[0].get("sector", "")

        # Get stock peers (competitors)
        peers = fmp_get(f"stock_peers/{symbol}")

        if peers and isinstance(peers, list) and len(peers) > 0:
            peer_list = peers[0].get("peersList", [])

            # Get profile for each peer (limit to top 5)
            for peer_symbol in peer_list[:5]:
                try:
                    peer_profile = fmp_get(f"profile/{peer_symbol}")
                    if peer_profile and len(peer_profile) > 0:
                        peer_data = peer_profile[0]
                        competitors.append({
                            "symbol": peer_symbol,
                            "name": peer_data.get("companyName", peer_symbol),
                            "market_cap": peer_data.get("mktCap", 0),
                            "industry": peer_data.get("industry", "N/A")
                        })
                except (requests.RequestException, KeyError, IndexError, TypeError) as e:
                    logger.warning(f" Could not fetch peer profile for {peer_symbol}: {e}")
                    continue

        # Sort by market cap
        competitors.sort(key=lambda x: x.get("market_cap", 0), reverse=True)

    except Exception as e:
        logger.error(f" fetching competition: {e}")

    return competitors


def get_management(symbol: str) -> List[Dict[str, Any]]:
    """Get key executives and management team with tenure and stock holdings"""
    management = []

    try:
        # Get key executives
        executives = fmp_get(f"key-executives/{symbol}")

        if not executives or not isinstance(executives, list):
            return management

        # Fetch annual report to extract employment history
        logger.info(f"Fetching annual report for {symbol} management background...")
        annual_report = fetch_annual_report_text(symbol)

        # Extract employment history using AI if annual report is available
        employment_data = {}
        if annual_report:
            try:
                # Get list of executive names
                exec_names = [exec.get("name", "") for exec in executives[:8]]
                names_list = ", ".join(exec_names)

                ai_prompt = f"""You are analyzing an annual report for executive employment history.

Executive names to find: {names_list}

Look for sections like "Executive Officers", "Management", "Board of Directors", or biographical information.

For each executive, extract their last 2 previous employers (before current company). Format as:
Executive Name: Company1, Company2

If employment history is not found for an executive, skip them.
Only return executives where you found employment history. Be concise."""

                employment_text = analyze_with_ai(ai_prompt, annual_report[:30000], use_claude=False)

                # Parse the AI response to extract employment data
                if employment_text and "Error" not in employment_text:
                    for line in employment_text.split('\n'):
                        if ':' in line:
                            parts = line.split(':', 1)
                            if len(parts) == 2:
                                exec_name = parts[0].strip()
                                employers = parts[1].strip()
                                if employers and employers.lower() not in ['n/a', 'not found', 'none']:
                                    employment_data[exec_name] = employers
            except Exception as e:
                logger.error(f" extracting employment history: {e}")

        # Build management list
        for exec in executives[:8]:  # Top 8 executives
            name = exec.get("name", "N/A")
            title = exec.get("title", "N/A")
            pay = exec.get("pay", 0)

            # Calculate tenure if available
            tenure = "N/A"
            # Note: FMP doesn't provide tenure directly in key-executives endpoint

            # Get stock holdings from insider trading data
            stock_held = None
            try:
                # Try to get insider trading data for this executive
                insider_trades = fmp_get(f"insider-trading", {"symbol": symbol, "limit": 200})
                if insider_trades:
                    # Find most recent trade by this executive
                    exec_trades = [t for t in insider_trades
                                 if name.upper() in t.get("reportingName", "").upper()]
                    if exec_trades:
                        # Get most recent securities owned
                        latest_trade = exec_trades[0]
                        securities_owned = latest_trade.get("securitiesOwned", 0)
                        if securities_owned > 0:
                            stock_held = f"{securities_owned:,} shares"
            except Exception as e:
                logger.error(f" fetching stock holdings for {name}: {e}")

            # Get employment history from AI extraction
            prior_employers = employment_data.get(name)

            exec_data = {
                "name": name,
                "title": title,
                "pay": pay,
            }

            # Only add fields if they have valid data
            if stock_held:
                exec_data["stock_held"] = stock_held

            if prior_employers:
                exec_data["prior_employers"] = prior_employers

            management.append(exec_data)

    except Exception as e:
        logger.error(f" fetching management: {e}")

    return management


def get_balance_sheet_metrics(symbol: str) -> Dict[str, Any]:
    """Get balance sheet and credit metrics including debt, liquidity, and solvency ratios"""
    metrics = {
        "current": {},
        "historical": [],
        "credit_ratios": {},
        "credit_ratios_historical": [],
        "liquidity_ratios": {},
        "liquidity_ratios_historical": [],
        "debt_schedule": []
    }

    try:
        # Get current balance sheet - fetch 10 years
        balance_sheets = fmp_get(f"balance-sheet-statement/{symbol}", {"limit": 10})

        if balance_sheets and len(balance_sheets) > 0:
            current_bs = balance_sheets[0]

            # Current balance sheet items
            metrics["current"] = {
                "date": current_bs.get("date", ""),
                "total_assets": current_bs.get("totalAssets", 0),
                "total_liabilities": current_bs.get("totalLiabilities", 0),
                "total_equity": current_bs.get("totalStockholdersEquity", 0),
                "cash_and_equivalents": current_bs.get("cashAndCashEquivalents", 0),
                "short_term_investments": current_bs.get("shortTermInvestments", 0),
                "total_cash": current_bs.get("cashAndCashEquivalents", 0) + current_bs.get("shortTermInvestments", 0),
                "accounts_receivable": current_bs.get("netReceivables", 0),
                "inventory": current_bs.get("inventory", 0),
                "current_assets": current_bs.get("totalCurrentAssets", 0),
                "current_liabilities": current_bs.get("totalCurrentLiabilities", 0),
                "long_term_debt": current_bs.get("longTermDebt", 0),
                "short_term_debt": current_bs.get("shortTermDebt", 0),
                "total_debt": current_bs.get("totalDebt", 0),
                "net_debt": current_bs.get("netDebt", 0),
                "goodwill": current_bs.get("goodwill", 0),
                "intangible_assets": current_bs.get("intangibleAssets", 0),
                "retained_earnings": current_bs.get("retainedEarnings", 0),
                "working_capital": current_bs.get("totalCurrentAssets", 0) - current_bs.get("totalCurrentLiabilities", 0)
            }

            # Build historical data
            for bs in balance_sheets:
                year = bs.get("calendarYear") or (bs.get("date", "")[:4] if bs.get("date") else "")
                if year:
                    metrics["historical"].append({
                        "year": str(year),
                        "total_assets": bs.get("totalAssets", 0),
                        "total_liabilities": bs.get("totalLiabilities", 0),
                        "total_equity": bs.get("totalStockholdersEquity", 0),
                        "total_debt": bs.get("totalDebt", 0),
                        "net_debt": bs.get("netDebt", 0),
                        "cash_and_equivalents": bs.get("cashAndCashEquivalents", 0)
                    })

            # Sort historical by year ascending
            metrics["historical"].sort(key=lambda x: x["year"])

        # Get financial ratios for credit metrics - fetch 10 years
        ratios = fmp_get(f"ratios-ttm/{symbol}")
        historical_ratios = fmp_get(f"ratios/{symbol}", {"limit": 10})

        if ratios and len(ratios) > 0:
            r = ratios[0]
            metrics["credit_ratios"] = {
                "debt_to_equity": r.get("debtEquityRatioTTM", 0),
                "debt_to_assets": r.get("debtRatioTTM", 0),
                "long_term_debt_to_capitalization": r.get("longTermDebtToCapitalizationTTM", 0),
                "total_debt_to_capitalization": r.get("totalDebtToCapitalizationTTM", 0),
                "interest_coverage": r.get("interestCoverageTTM", 0),
                "cash_flow_to_debt": r.get("cashFlowToDebtRatioTTM", 0),
                "equity_multiplier": r.get("companyEquityMultiplierTTM", 0)
            }

            metrics["liquidity_ratios"] = {
                "current_ratio": r.get("currentRatioTTM", 0),
                "quick_ratio": r.get("quickRatioTTM", 0),
                "cash_ratio": r.get("cashRatioTTM", 0),
                "operating_cash_flow_ratio": r.get("operatingCashFlowPerShareTTM", 0),
                "days_sales_outstanding": r.get("daysOfSalesOutstandingTTM", 0),
                "days_inventory_outstanding": r.get("daysOfInventoryOutstandingTTM", 0),
                "days_payables_outstanding": r.get("daysOfPayablesOutstandingTTM", 0),
                "cash_conversion_cycle": r.get("cashConversionCycleTTM", 0)
            }

        # Build 10-year historical data for liquidity and credit ratios
        # Calculate from financial statements for reliability
        logger.info(f"Calculating historical ratios from financial statements for {symbol}...")

        # Fetch 10 years of financial statements
        balance_sheets_hist = fmp_get(f"balance-sheet-statement/{symbol}", {"limit": 10})
        income_statements_hist = fmp_get(f"income-statement/{symbol}", {"limit": 10})
        cash_flow_hist = fmp_get(f"cash-flow-statement/{symbol}", {"limit": 10})

        if balance_sheets_hist and len(balance_sheets_hist) > 0:
            # Create dictionaries indexed by year for easy lookup
            income_by_year = {}
            if income_statements_hist:
                for inc in income_statements_hist:
                    year = inc.get("calendarYear") or (inc.get("date", "")[:4] if inc.get("date") else "")
                    if year:
                        income_by_year[str(year)] = inc

            cash_flow_by_year = {}
            if cash_flow_hist:
                for cf in cash_flow_hist:
                    year = cf.get("calendarYear") or (cf.get("date", "")[:4] if cf.get("date") else "")
                    if year:
                        cash_flow_by_year[str(year)] = cf

            # Calculate ratios from balance sheet data
            for bs in balance_sheets_hist:
                year = bs.get("calendarYear") or (bs.get("date", "")[:4] if bs.get("date") else "")
                if not year:
                    continue

                year = str(year)

                # Get corresponding income statement and cash flow
                inc = income_by_year.get(year, {})
                cf = cash_flow_by_year.get(year, {})

                # Balance sheet items
                current_assets = bs.get("totalCurrentAssets", 0) or 0
                current_liabilities = bs.get("totalCurrentLiabilities", 0) or 0
                inventory = bs.get("inventory", 0) or 0
                cash = bs.get("cashAndCashEquivalents", 0) or 0
                short_term_investments = bs.get("shortTermInvestments", 0) or 0
                total_assets = bs.get("totalAssets", 0) or 0
                total_liabilities = bs.get("totalLiabilities", 0) or 0
                total_equity = bs.get("totalStockholdersEquity", 0) or 0
                total_debt = bs.get("totalDebt", 0) or 0
                long_term_debt = bs.get("longTermDebt", 0) or 0
                accounts_receivable = bs.get("netReceivables", 0) or 0
                accounts_payable = bs.get("accountPayables", 0) or 0

                # Income statement items
                revenue = inc.get("revenue", 0) or 0
                cost_of_revenue = inc.get("costOfRevenue", 0) or 0
                operating_income = inc.get("operatingIncome", 0) or 0
                interest_expense = inc.get("interestExpense", 0) or 0
                ebitda = inc.get("ebitda", 0) or 0

                # Cash flow items
                operating_cash_flow = cf.get("operatingCashFlow", 0) or 0

                # Calculate Liquidity Ratios
                current_ratio = current_assets / current_liabilities if current_liabilities > 0 else 0
                quick_ratio = (current_assets - inventory) / current_liabilities if current_liabilities > 0 else 0
                cash_ratio = (cash + short_term_investments) / current_liabilities if current_liabilities > 0 else 0

                # Days calculations (using 365 days)
                dso = (accounts_receivable / revenue * 365) if revenue > 0 else 0
                dio = (inventory / cost_of_revenue * 365) if cost_of_revenue > 0 else 0
                dpo = (accounts_payable / cost_of_revenue * 365) if cost_of_revenue > 0 else 0
                ccc = dso + dio - dpo

                # Calculate Credit Ratios
                debt_to_equity = total_debt / total_equity if total_equity > 0 else 0
                debt_to_assets = total_debt / total_assets if total_assets > 0 else 0
                lt_debt_to_cap = long_term_debt / (long_term_debt + total_equity) if (long_term_debt + total_equity) > 0 else 0
                total_debt_to_cap = total_debt / (total_debt + total_equity) if (total_debt + total_equity) > 0 else 0
                interest_coverage = operating_income / interest_expense if interest_expense > 0 else 0
                cash_flow_to_debt = operating_cash_flow / total_debt if total_debt > 0 else 0

                # Add liquidity ratios for this year
                metrics["liquidity_ratios_historical"].append({
                    "year": year,
                    "current_ratio": round(current_ratio, 2),
                    "quick_ratio": round(quick_ratio, 2),
                    "cash_ratio": round(cash_ratio, 2),
                    "days_sales_outstanding": round(dso, 0),
                    "days_inventory_outstanding": round(dio, 0),
                    "days_payables_outstanding": round(dpo, 0),
                    "cash_conversion_cycle": round(ccc, 0)
                })

                # Add credit ratios for this year
                metrics["credit_ratios_historical"].append({
                    "year": year,
                    "debt_to_equity": round(debt_to_equity, 2),
                    "debt_to_assets": round(debt_to_assets, 2),
                    "long_term_debt_to_capitalization": round(lt_debt_to_cap, 2),
                    "total_debt_to_capitalization": round(total_debt_to_cap, 2),
                    "interest_coverage": round(interest_coverage, 2),
                    "cash_flow_to_debt": round(cash_flow_to_debt, 2)
                })

            # Sort by year ascending
            metrics["liquidity_ratios_historical"].sort(key=lambda x: x["year"])
            metrics["credit_ratios_historical"].sort(key=lambda x: x["year"])

            logger.info(f"Calculated {len(metrics['liquidity_ratios_historical'])} years of liquidity ratios")
            logger.info(f"Calculated {len(metrics['credit_ratios_historical'])} years of credit ratios")
        else:
            logger.warning(f"No balance sheet data available for {symbol}")

        # Get cash flow data for additional metrics
        cash_flows = fmp_get(f"cash-flow-statement/{symbol}", {"limit": 1})
        if cash_flows and len(cash_flows) > 0:
            cf = cash_flows[0]
            operating_cf = cf.get("operatingCashFlow", 0)
            total_debt = metrics["current"].get("total_debt", 0)

            # Calculate debt service coverage if we have the data
            if total_debt > 0 and operating_cf > 0:
                metrics["credit_ratios"]["debt_service_coverage"] = operating_cf / (total_debt * 0.1)  # Assuming 10% annual debt service

            # Free cash flow to debt
            fcf = cf.get("freeCashFlow", 0)
            if total_debt > 0:
                metrics["credit_ratios"]["fcf_to_debt"] = fcf / total_debt

        # Get income statement for EBITDA-based ratios
        income_stmt = fmp_get(f"income-statement/{symbol}", {"limit": 1})
        if income_stmt and len(income_stmt) > 0:
            inc = income_stmt[0]
            ebitda = inc.get("ebitda", 0)
            interest_expense = inc.get("interestExpense", 0)
            total_debt = metrics["current"].get("total_debt", 0)
            net_debt = metrics["current"].get("net_debt", 0)

            if ebitda and ebitda > 0:
                metrics["credit_ratios"]["net_debt_to_ebitda"] = net_debt / ebitda if net_debt else 0
                metrics["credit_ratios"]["total_debt_to_ebitda"] = total_debt / ebitda if total_debt else 0

            if interest_expense and interest_expense > 0 and ebitda:
                metrics["credit_ratios"]["ebitda_to_interest"] = ebitda / interest_expense

        # Get enterprise value metrics
        key_metrics = fmp_get(f"key-metrics-ttm/{symbol}")
        if key_metrics and len(key_metrics) > 0:
            km = key_metrics[0]
            metrics["current"]["enterprise_value"] = km.get("enterpriseValueTTM", 0)
            metrics["current"]["tangible_book_value"] = km.get("tangibleBookValuePerShareTTM", 0)
            metrics["current"]["book_value_per_share"] = km.get("bookValuePerShareTTM", 0)

        logger.info(f"Successfully fetched balance sheet metrics for {symbol}")
        logger.debug(f"Final liquidity_ratios_historical count: {len(metrics['liquidity_ratios_historical'])}")
        logger.debug(f"Final credit_ratios_historical count: {len(metrics['credit_ratios_historical'])}")
        if len(metrics['liquidity_ratios_historical']) > 0:
            logger.debug(f"Sample liquidity data: {metrics['liquidity_ratios_historical'][0]}")

    except Exception as e:
        logger.error(f" fetching balance sheet metrics: {e}")
        import traceback
        traceback.print_exc()

    return metrics


def get_technical_analysis(symbol: str, language: str = "en") -> Dict[str, Any]:
    """Get technical analysis metrics including moving averages, RSI, MACD, and other indicators"""
    technical = {
        "price_data": {},
        "moving_averages": {},
        "momentum_indicators": {},
        "volatility_indicators": {},
        "volume_analysis": {},
        "support_resistance": {},
        "trend_analysis": {}
    }

    # Technical signal translations
    tech_signals = {
        "en": {
            "bullish": "Bullish", "bearish": "Bearish", "neutral": "Neutral",
            "extended": "Extended", "oversold": "Oversold", "overbought": "Overbought",
            "above_avg": "Above Average", "below_avg": "Below Average",
            "strong_uptrend": "Strong Uptrend", "uptrend": "Uptrend",
            "strong_downtrend": "Strong Downtrend", "downtrend": "Downtrend",
            "sideways": "Sideways/Consolidating", "buy": "Buy", "sell": "Sell", "hold": "Hold"
        },
        "it": {
            "bullish": "Rialzista", "bearish": "Ribassista", "neutral": "Neutrale",
            "extended": "Esteso", "oversold": "Ipervenduto", "overbought": "Ipercomprato",
            "above_avg": "Sopra la Media", "below_avg": "Sotto la Media",
            "strong_uptrend": "Forte Trend Rialzista", "uptrend": "Trend Rialzista",
            "strong_downtrend": "Forte Trend Ribassista", "downtrend": "Trend Ribassista",
            "sideways": "Laterale/Consolidamento", "buy": "Acquista", "sell": "Vendi", "hold": "Mantieni"
        }
    }
    sig = tech_signals.get(language, tech_signals["en"])

    try:
        # Get current quote data
        quote = fmp_get(f"quote/{symbol}")
        if quote and len(quote) > 0:
            q = quote[0]
            technical["price_data"] = {
                "current_price": q.get("price", 0),
                "change": q.get("change", 0),
                "change_percent": q.get("changesPercentage", 0),
                "day_high": q.get("dayHigh", 0),
                "day_low": q.get("dayLow", 0),
                "year_high": q.get("yearHigh", 0),
                "year_low": q.get("yearLow", 0),
                "volume": q.get("volume", 0),
                "avg_volume": q.get("avgVolume", 0),
                "open": q.get("open", 0),
                "previous_close": q.get("previousClose", 0),
                "eps": q.get("eps", 0),
                "pe": q.get("pe", 0),
                "market_cap": q.get("marketCap", 0)
            }

            # Calculate distance from 52-week high/low
            current_price = q.get("price", 0)
            year_high = q.get("yearHigh", 0)
            year_low = q.get("yearLow", 0)

            if year_high and current_price:
                technical["price_data"]["pct_from_52w_high"] = ((current_price - year_high) / year_high) * 100
            if year_low and current_price:
                technical["price_data"]["pct_from_52w_low"] = ((current_price - year_low) / year_low) * 100

        # Get historical prices for technical calculations
        logger.info(f"Fetching historical prices for {symbol} technical analysis...")
        historical = fmp_get(f"historical-price-full/{symbol}", {"timeseries": 252})  # ~1 year of trading days

        if historical and "historical" in historical and len(historical["historical"]) > 0:
            prices = historical["historical"]

            # Extract closing prices (most recent first in FMP data)
            closes = [day.get("close", 0) for day in prices]
            volumes = [day.get("volume", 0) for day in prices]
            highs = [day.get("high", 0) for day in prices]
            lows = [day.get("low", 0) for day in prices]

            # Reverse to have oldest first for calculations
            closes_asc = list(reversed(closes))
            volumes_asc = list(reversed(volumes))
            highs_asc = list(reversed(highs))
            lows_asc = list(reversed(lows))

            current_price = closes[0] if closes else 0

            # Calculate Simple Moving Averages
            def calc_sma(data, period):
                if len(data) >= period:
                    return sum(data[-period:]) / period
                return 0

            sma_10 = calc_sma(closes_asc, 10)
            sma_20 = calc_sma(closes_asc, 20)
            sma_50 = calc_sma(closes_asc, 50)
            sma_100 = calc_sma(closes_asc, 100)
            sma_200 = calc_sma(closes_asc, 200)

            technical["moving_averages"] = {
                "sma_10": round(sma_10, 2),
                "sma_20": round(sma_20, 2),
                "sma_50": round(sma_50, 2),
                "sma_100": round(sma_100, 2),
                "sma_200": round(sma_200, 2),
                "price_vs_sma_10": round(((current_price - sma_10) / sma_10) * 100, 2) if sma_10 else 0,
                "price_vs_sma_20": round(((current_price - sma_20) / sma_20) * 100, 2) if sma_20 else 0,
                "price_vs_sma_50": round(((current_price - sma_50) / sma_50) * 100, 2) if sma_50 else 0,
                "price_vs_sma_200": round(((current_price - sma_200) / sma_200) * 100, 2) if sma_200 else 0
            }

            # Calculate Exponential Moving Averages
            def calc_ema(data, period):
                if len(data) < period:
                    return 0
                multiplier = 2 / (period + 1)
                ema = sum(data[:period]) / period  # Start with SMA
                for price in data[period:]:
                    ema = (price * multiplier) + (ema * (1 - multiplier))
                return ema

            ema_12 = calc_ema(closes_asc, 12)
            ema_26 = calc_ema(closes_asc, 26)

            technical["moving_averages"]["ema_12"] = round(ema_12, 2)
            technical["moving_averages"]["ema_26"] = round(ema_26, 2)

            # MACD Calculation
            macd_line = ema_12 - ema_26

            # Calculate signal line (9-day EMA of MACD)
            # First, calculate MACD history
            macd_history = []
            for i in range(26, len(closes_asc)):
                ema_12_temp = calc_ema(closes_asc[:i+1], 12)
                ema_26_temp = calc_ema(closes_asc[:i+1], 26)
                macd_history.append(ema_12_temp - ema_26_temp)

            signal_line = calc_ema(macd_history, 9) if len(macd_history) >= 9 else 0
            macd_histogram = macd_line - signal_line

            technical["momentum_indicators"]["macd"] = {
                "macd_line": round(macd_line, 4),
                "signal_line": round(signal_line, 4),
                "histogram": round(macd_histogram, 4),
                "signal": sig["bullish"] if macd_line > signal_line else sig["bearish"]
            }

            # RSI Calculation (14-day)
            def calc_rsi(data, period=14):
                if len(data) < period + 1:
                    return 50  # Default neutral

                gains = []
                losses = []

                for i in range(1, len(data)):
                    change = data[i] - data[i-1]
                    if change > 0:
                        gains.append(change)
                        losses.append(0)
                    else:
                        gains.append(0)
                        losses.append(abs(change))

                # Calculate average gain/loss for first period
                avg_gain = sum(gains[:period]) / period
                avg_loss = sum(losses[:period]) / period

                # Calculate smoothed averages
                for i in range(period, len(gains)):
                    avg_gain = (avg_gain * (period - 1) + gains[i]) / period
                    avg_loss = (avg_loss * (period - 1) + losses[i]) / period

                if avg_loss == 0:
                    return 100

                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                return rsi

            rsi_14 = calc_rsi(closes_asc, 14)

            # RSI Signal: 45-70 Bullish, 70+ Extended, <30 Oversold, else Neutral
            if rsi_14 >= 70:
                rsi_signal = sig["extended"]
            elif 45 <= rsi_14 < 70:
                rsi_signal = sig["bullish"]
            elif rsi_14 < 30:
                rsi_signal = sig["oversold"]
            else:
                rsi_signal = sig["neutral"]

            technical["momentum_indicators"]["rsi"] = {
                "value": round(rsi_14, 2),
                "signal": rsi_signal
            }

            # Stochastic Oscillator (14-day)
            if len(closes_asc) >= 14:
                period = 14
                lowest_low = min(lows_asc[-period:])
                highest_high = max(highs_asc[-period:])

                if highest_high - lowest_low != 0:
                    stoch_k = ((current_price - lowest_low) / (highest_high - lowest_low)) * 100
                else:
                    stoch_k = 50

                # %D is 3-day SMA of %K (simplified)
                stoch_d = stoch_k  # Simplified - would need more history for proper calculation

                technical["momentum_indicators"]["stochastic"] = {
                    "k": round(stoch_k, 2),
                    "d": round(stoch_d, 2),
                    "signal": sig["overbought"] if stoch_k > 80 else (sig["oversold"] if stoch_k < 20 else sig["neutral"])
                }

            # Volatility Indicators
            # Average True Range (ATR)
            def calc_atr(highs, lows, closes, period=14):
                if len(highs) < period + 1:
                    return 0

                true_ranges = []
                for i in range(1, len(highs)):
                    tr = max(
                        highs[i] - lows[i],
                        abs(highs[i] - closes[i-1]),
                        abs(lows[i] - closes[i-1])
                    )
                    true_ranges.append(tr)

                if len(true_ranges) < period:
                    return 0

                return sum(true_ranges[-period:]) / period

            atr = calc_atr(highs_asc, lows_asc, closes_asc, 14)

            technical["volatility_indicators"]["atr"] = {
                "value": round(atr, 2),
                "atr_percent": round((atr / current_price) * 100, 2) if current_price else 0
            }

            # Bollinger Bands (20-day, 2 std dev)
            if len(closes_asc) >= 20:
                bb_period = 20
                bb_closes = closes_asc[-bb_period:]
                bb_sma = sum(bb_closes) / bb_period
                variance = sum((x - bb_sma) ** 2 for x in bb_closes) / bb_period
                std_dev = variance ** 0.5

                upper_band = bb_sma + (2 * std_dev)
                lower_band = bb_sma - (2 * std_dev)

                # Bollinger Band Width
                bb_width = ((upper_band - lower_band) / bb_sma) * 100

                # %B - where price is relative to bands
                percent_b = ((current_price - lower_band) / (upper_band - lower_band)) * 100 if (upper_band - lower_band) != 0 else 50

                technical["volatility_indicators"]["bollinger_bands"] = {
                    "upper": round(upper_band, 2),
                    "middle": round(bb_sma, 2),
                    "lower": round(lower_band, 2),
                    "width": round(bb_width, 2),
                    "percent_b": round(percent_b, 2),
                    "signal": sig["overbought"] if percent_b > 100 else (sig["oversold"] if percent_b < 0 else sig["neutral"])
                }

            # Volume Analysis
            if volumes_asc:
                avg_volume_20 = sum(volumes_asc[-20:]) / min(20, len(volumes_asc))
                avg_volume_50 = sum(volumes_asc[-50:]) / min(50, len(volumes_asc))
                current_volume = volumes_asc[-1] if volumes_asc else 0

                technical["volume_analysis"] = {
                    "current_volume": current_volume,
                    "avg_volume_20": round(avg_volume_20),
                    "avg_volume_50": round(avg_volume_50),
                    "volume_ratio": round(current_volume / avg_volume_20, 2) if avg_volume_20 else 0,
                    "volume_trend": sig["above_avg"] if current_volume > avg_volume_20 else sig["below_avg"]
                }

            # Support and Resistance (simplified - based on recent highs/lows)
            if len(prices) >= 20:
                recent_20_highs = highs[:20]  # Most recent 20 days
                recent_20_lows = lows[:20]

                # Pivot points
                pivot = (highs[0] + lows[0] + closes[0]) / 3
                r1 = (2 * pivot) - lows[0]
                s1 = (2 * pivot) - highs[0]
                r2 = pivot + (highs[0] - lows[0])
                s2 = pivot - (highs[0] - lows[0])

                technical["support_resistance"] = {
                    "pivot": round(pivot, 2),
                    "resistance_1": round(r1, 2),
                    "resistance_2": round(r2, 2),
                    "support_1": round(s1, 2),
                    "support_2": round(s2, 2),
                    "recent_high_20d": round(max(recent_20_highs), 2),
                    "recent_low_20d": round(min(recent_20_lows), 2)
                }

            # Trend Analysis
            golden_cross = sma_50 > sma_200 if sma_50 and sma_200 else None
            death_cross = sma_50 < sma_200 if sma_50 and sma_200 else None

            # Price trend determination
            if current_price > sma_20 > sma_50 > sma_200:
                trend = "Strong Uptrend"
            elif current_price > sma_50 > sma_200:
                trend = "Uptrend"
            elif current_price < sma_20 < sma_50 < sma_200:
                trend = "Strong Downtrend"
            elif current_price < sma_50 < sma_200:
                trend = "Downtrend"
            else:
                trend = "Sideways/Consolidation"

            technical["trend_analysis"] = {
                "overall_trend": trend,
                "golden_cross": golden_cross,
                "death_cross": death_cross,
                "above_sma_20": current_price > sma_20 if sma_20 else None,
                "above_sma_50": current_price > sma_50 if sma_50 else None,
                "above_sma_200": current_price > sma_200 if sma_200 else None
            }

        logger.info(f"Successfully calculated technical analysis for {symbol}")

    except Exception as e:
        logger.error(f" calculating technical analysis: {e}")
        import traceback
        traceback.print_exc()

    return technical


def get_investment_thesis(symbol: str, report_data: Dict[str, Any], language: str = "en") -> Dict[str, Any]:
    """Generate AI-powered investment thesis with bull/bear cases"""
    thesis = {
        "summary": "",
        "bull_case": [],
        "bear_case": [],
        "key_metrics_to_watch": [],
        "catalysts": []
    }

    try:
        # Gather all available data for comprehensive analysis
        business_overview = report_data.get("business_overview", {})
        key_metrics = report_data.get("key_metrics", {})
        valuations = report_data.get("valuations", {})
        risks = report_data.get("risks", {})
        revenue_data = report_data.get("revenue_data", {})
        recent_highlights = report_data.get("recent_highlights", [])

        company_name = business_overview.get("company_name", symbol)
        description = business_overview.get("description", "")

        # Build context from all report data
        context = f"""
COMPANY: {company_name} ({symbol})
SECTOR: {business_overview.get('sector', 'N/A')}
INDUSTRY: {business_overview.get('industry', 'N/A')}
MARKET CAP: ${business_overview.get('market_cap', 0)/1e9:.2f}B

BUSINESS DESCRIPTION:
{description[:3000]}

KEY FINANCIALS:
- Revenue Growth (TTM): {key_metrics.get('revenue_growth_ttm', 0):.1f}%
- Revenue Growth (3yr avg): {key_metrics.get('revenue_growth_3yr', 0):.1f}%
- Gross Margin: {key_metrics.get('gross_margin', 0)*100:.1f}%
- Operating Margin: {key_metrics.get('operating_margin', 0)*100:.1f}%
- ROE: {key_metrics.get('roe', 0):.1f}%
- ROIC: {key_metrics.get('roic', 0):.1f}%
- Free Cash Flow: ${key_metrics.get('free_cash_flow', 0)/1e9:.2f}B

VALUATION:
- P/E Ratio: {valuations.get('pe_ratio', 0):.1f}
- Price/Sales: {valuations.get('price_to_sales', 0):.1f}
- EV/EBITDA: {valuations.get('ev_to_ebitda', 0):.1f}
- PEG Ratio: {valuations.get('peg_ratio', 0):.2f}

COMPANY-SPECIFIC RISKS:
{chr(10).join(['- ' + r for r in risks.get('company_specific', [])[:5]])}

GENERAL RISKS:
{chr(10).join(['- ' + r for r in risks.get('general', [])[:5]])}

MARGINS:
- Gross: {revenue_data.get('margins', {}).get('gross_margin', 0):.1f}%
- Operating: {revenue_data.get('margins', {}).get('operating_margin', 0):.1f}%
- Net: {revenue_data.get('margins', {}).get('net_margin', 0):.1f}%
"""

        # Add recent highlights if available
        if recent_highlights and recent_highlights[0].get("ai_summary"):
            context += f"\nRECENT QUARTERLY TRENDS:\n{recent_highlights[0].get('ai_summary', '')[:2000]}"

        ai_prompt = f"""You are a senior equity research analyst creating an investment thesis for {company_name} ({symbol}).

Based on the company data provided, generate a comprehensive investment analysis in the following JSON-like format:

INVESTMENT_THESIS_SUMMARY:
[Write a 2-3 paragraph executive summary (150-200 words) covering: 1) What makes this company interesting as an investment, 2) Current valuation context, 3) Overall recommendation stance (positive/neutral/cautious) with reasoning]

BULL_CASE:
1. [First bull case argument - specific, quantifiable where possible]
2. [Second bull case argument]
3. [Third bull case argument]
4. [Fourth bull case argument - optional]
5. [Fifth bull case argument - optional]

BEAR_CASE:
1. [First bear case argument - specific, quantifiable where possible]
2. [Second bear case argument]
3. [Third bear case argument]
4. [Fourth bear case argument - optional]
5. [Fifth bear case argument - optional]

KEY_METRICS_TO_WATCH:
1. [Metric 1]: [Why it matters for this specific company]
2. [Metric 2]: [Why it matters]
3. [Metric 3]: [Why it matters]
4. [Metric 4]: [Why it matters - optional]

CATALYSTS:
1. [Upcoming catalyst 1 - earnings, product launches, regulatory decisions, etc.]
2. [Upcoming catalyst 2]
3. [Upcoming catalyst 3]

Be specific to THIS company. Avoid generic statements. Reference actual numbers from the data provided.
Each bull/bear case should be a complete, standalone argument (1-2 sentences).
For metrics to watch, explain WHY that metric matters specifically for this company's investment case."""

        logger.info(f"Generating investment thesis for {symbol}...")
        analysis = analyze_with_ai(ai_prompt, context, use_claude=True, language=language)

        if "Error" in analysis or "unavailable" in analysis:
            logger.info("Claude failed for investment thesis, trying OpenAI...")
            analysis = analyze_with_ai(ai_prompt, context, use_claude=False, language=language)

        # Parse the AI response
        if analysis:
            current_section = None
            current_items = []

            for line in analysis.split('\n'):
                line = line.strip()

                if "INVESTMENT_THESIS_SUMMARY:" in line:
                    current_section = "summary"
                    current_items = []
                elif "BULL_CASE:" in line:
                    if current_section == "summary":
                        thesis["summary"] = ' '.join(current_items)
                    current_section = "bull"
                    current_items = []
                elif "BEAR_CASE:" in line:
                    if current_section == "bull":
                        thesis["bull_case"] = current_items
                    current_section = "bear"
                    current_items = []
                elif "KEY_METRICS_TO_WATCH:" in line:
                    if current_section == "bear":
                        thesis["bear_case"] = current_items
                    current_section = "metrics"
                    current_items = []
                elif "CATALYSTS:" in line:
                    if current_section == "metrics":
                        thesis["key_metrics_to_watch"] = current_items
                    current_section = "catalysts"
                    current_items = []
                elif line and current_section:
                    # Remove numbering if present
                    clean_line = line
                    if line[0].isdigit() and '.' in line[:3]:
                        clean_line = line.split('.', 1)[1].strip() if '.' in line else line

                    if clean_line:
                        current_items.append(clean_line)

            # Capture the last section
            if current_section == "catalysts":
                thesis["catalysts"] = current_items
            elif current_section == "summary":
                thesis["summary"] = ' '.join(current_items)

    except Exception as e:
        logger.error(f" generating investment thesis: {e}")
        thesis["summary"] = f"Unable to generate investment thesis: {str(e)}"

    return thesis


def get_industry_specific_metrics_prompt(symbol: str, company_name: str, industry: str, sector: str, business_description: str) -> str:
    """Generate a dynamic AI prompt to identify company-specific KPIs and metrics.

    This approach uses AI to determine what metrics matter most for THIS specific company,
    with guidance on critical metrics by business type. Meta's metrics are different from Best Buy's,
    even though both might be classified similarly.
    """

    # Build industry-specific guidance based on detected business type
    industry_guidance = """
INDUSTRY-SPECIFIC METRIC GUIDANCE (use as reference, but prioritize what's relevant to THIS company):

**FOR ADVERTISING/SOCIAL MEDIA COMPANIES (Meta, Google, Snap, Pinterest):**
- DAU (Daily Active Users) and MAU (Monthly Active Users) by platform/region
- DAU/MAU ratio (stickiness/engagement)
- ARPU (Average Revenue Per User) by region
- Ad impressions served and growth
- Average price per ad / CPM trends
- Time spent per user / engagement metrics
- Ad load (ads per session)
- Advertiser count and retention
- Family of Apps / cross-platform metrics

**FOR SAAS/SUBSCRIPTION SOFTWARE (Salesforce, Workday, ServiceNow):**
- ARR (Annual Recurring Revenue) and growth rate
- RPO (Remaining Performance Obligations) - total and current
- cRPO (Current RPO due within 12 months)
- Deferred Revenue - current and long-term
- NRR (Net Revenue Retention) / DBNER (Dollar-Based Net Expansion Rate)
- GRR (Gross Revenue Retention)
- Customer count by tier (>$100K ACV, >$1M ACV)
- Logo churn and revenue churn
- CAC, LTV, LTV/CAC ratio, CAC payback
- Billings growth
- Free-to-paid conversion (if PLG model)

**FOR BIOTECHNOLOGY/PHARMACEUTICAL:**
- DRUG PIPELINE (CRITICAL): For EACH drug, extract:
  * Drug name/code
  * Therapeutic area and indication
  * Development phase (Preclinical, Phase 1, 2, 3, NDA Filed, Approved)
  * Mechanism of action
  * Expected readout dates / PDUFA dates
  * Partnership status (owned vs licensed)
- Total Addressable Market (TAM) for each indication
- Competing drugs already approved (names, companies, sales)
- Competitor pipeline drugs in same indications
- Commercial product sales (if any)
- Patent expiration dates
- Cash runway (months/years)
- R&D spending
- Partnership milestones and royalties

**FOR BANKS/FINANCIAL SERVICES:**
- CREDIT QUALITY (CRITICAL):
  * Non-Performing Loans (NPL) ratio by category
  * 30-day, 60-day, 90+ day delinquencies by loan type
  * Delinquency TRENDS (improving or worsening QoQ)
  * Net Charge-Offs (NCO) rate
  * Allowance for Credit Losses (ACL) as % of loans
  * ACL coverage ratio (ACL / NPLs)
- BALANCE SHEET RISKS:
  * CRE exposure (especially Office) as % of loans and capital
  * Unrealized losses in securities portfolio (HTM and AFS)
  * Uninsured deposits as % of total deposits
- NIM (Net Interest Margin) and trend
- NII (Net Interest Income) growth
- ROTCE (Return on Tangible Common Equity)
- CET1 Capital Ratio
- Deposit beta and cost of deposits
- Loan-to-Deposit Ratio

**FOR RETAIL/CONSUMER (Best Buy, Target, Walmart):**
- Same-store sales (Comps) growth
- Traffic vs Ticket breakdown
- E-commerce as % of sales and growth rate
- Sales per square foot
- Gross margin and markdown activity
- Inventory turnover and days on hand
- Store count changes (openings/closures)
- Loyalty program metrics
- Shrink/theft impact

**FOR SEMICONDUCTORS/HARDWARE (NVIDIA, AMD, Intel):**
- Revenue by end market (Data Center, Gaming, Auto, PC, etc.)
- AI/accelerator revenue specifically
- Gross margin trends
- Design wins and backlog
- Market share by segment
- Book-to-bill ratio
- Inventory weeks of supply

**FOR E-COMMERCE/MARKETPLACES (Amazon, Airbnb, Uber):**
- GMV (Gross Merchandise Value) or GBV (Gross Booking Value)
- Take rate / commission rate
- Active buyers/sellers and growth
- Orders per customer / frequency
- Fulfillment costs as % of revenue
- Contribution margin by segment
"""

    return f"""You are a senior equity research analyst. Your task is to identify and extract the KEY PERFORMANCE INDICATORS (KPIs) and metrics that matter MOST for analyzing {company_name} ({symbol}).

COMPANY CONTEXT:
- Company: {company_name} ({symbol})
- Industry: {industry}
- Sector: {sector}
- Business Description: {business_description[:2000]}

STEP 1: IDENTIFY THIS COMPANY'S ACTUAL BUSINESS MODEL
Determine what type of business this ACTUALLY is based on how it makes money. DO NOT rely on industry classification alone.
- Is it advertising-driven (like Meta, Google)?
- Is it subscription/SaaS (like Salesforce, Netflix)?
- Is it e-commerce/retail (like Amazon retail, Best Buy)?
- Is it cloud infrastructure (like AWS, Azure)?
- Is it hardware (like Apple devices, NVIDIA chips)?
- Is it a marketplace/platform (like Airbnb, Uber)?
- Is it biotech with drug pipeline?
- Is it a bank with loans/deposits?
- Is it a hybrid (multiple business models)?

CRITICAL: A company like Meta is NOT like Best Buy even if both sell to consumers. Meta is an ADVERTISING business - its KPIs are DAU, MAU, ARPU, ad pricing. Best Buy is a RETAILER - its KPIs are same-store sales, inventory turnover, e-commerce %.

{industry_guidance}

STEP 2: IDENTIFY THE 10-15 MOST CRITICAL KPIS FOR THIS SPECIFIC COMPANY
Based on the actual business model AND the guidance above, identify the metrics that:
1. Drive revenue growth
2. Indicate competitive strength
3. Show unit economics health
4. Signal future performance
5. Are unique to THIS company's business

STEP 3: EXTRACT ACTUAL VALUES WITH SPECIFICITY
For each KPI you identify, extract:
- Current value (with SPECIFIC numbers - not "strong" or "healthy")
- Year-over-year change (with %)
- Quarter-over-quarter change if relevant
- Trend direction (improving/stable/declining)
- Context (vs guidance, vs peers, vs historical)

FORMAT YOUR RESPONSE AS:

BUSINESS_MODEL_ASSESSMENT:
[2-3 sentences describing what this company actually does, its primary revenue streams, and how it makes money. Be specific about the business model type.]

KEY_METRICS_FOR_{symbol}:

1. [METRIC NAME]: [Current Value with units]
   - YoY Change: [+X% or -X%]
   - QoQ Change: [+X% or -X%] (if relevant)
   - Trend: [Improving/Stable/Declining]
   - Why It Matters: [1-2 sentences on why this metric is critical for THIS company's investment thesis]

2. [METRIC NAME]: [Current Value]
   ... (continue for 10-15 metrics, prioritizing the most important)

SEGMENT_BREAKDOWN:
[If the company has multiple segments, show revenue/profit by segment with growth rates]

COMPETITIVE_KPIS:
[3-5 metrics that show competitive position vs peers, with specific comparisons if available]

LEADING_INDICATORS:
[3-5 forward-looking metrics that predict future performance - like RPO, backlog, pipeline, bookings]

RED_FLAG_METRICS:
[Any metrics showing concerning trends - be specific about what's concerning and why]

CRITICAL REQUIREMENTS:
- Be SPECIFIC to this company. Do not use generic analysis.
- Extract ACTUAL numbers from the source documents.
- If a metric is not disclosed, state "Not disclosed" rather than guessing.
- For biotech: List EVERY drug in pipeline with phase and indication.
- For banks: Report delinquency rates by category and trends.
- For SaaS: Report RPO, ARR, NRR with actual figures.
- Focus on what Wall Street analysts tracking this stock would care about most.
"""


def get_competitive_analysis_ai(symbol: str, language: str = "en") -> Dict[str, Any]:
    """AI-powered deep competitive analysis with industry-specific focus"""
    analysis = {
        "moat_analysis": "",
        "competitive_position": "",
        "market_dynamics": "",
        "competitive_advantages": [],
        "key_competitors": [],
        "emerging_competitors": [],
        "industry_analysis": ""
    }

    try:
        # First, get FMP peers as baseline competitors
        logger.info(f"Fetching FMP peers for {symbol}...")
        fmp_competitors = get_competition(symbol)

        # Build FMP competitor context for AI
        fmp_competitor_text = ""
        if fmp_competitors:
            fmp_competitor_text = "\nKNOWN COMPETITORS FROM MARKET DATA:\n"
            for comp in fmp_competitors[:8]:
                mc = comp.get('market_cap', 0)
                mc_str = f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
                fmp_competitor_text += f"- {comp.get('name', 'N/A')} ({comp.get('symbol', 'N/A')}) - Market Cap: {mc_str}, Industry: {comp.get('industry', 'N/A')}\n"

        # Fetch annual report for comprehensive analysis
        logger.info(f"Fetching annual report for {symbol} competitive analysis...")
        annual_report = fetch_annual_report_text(symbol)

        # Get company profile
        profile = fmp_get(f"profile/{symbol}")
        company_name = profile[0].get("companyName", symbol) if profile else symbol
        industry = profile[0].get("industry", "N/A") if profile else "N/A"
        sector = profile[0].get("sector", "N/A") if profile else "N/A"
        description = profile[0].get("description", "") if profile else ""

        # Get financial metrics for context
        ratios = fmp_get(f"ratios-ttm/{symbol}")
        key_metrics = fmp_get(f"key-metrics-ttm/{symbol}")

        # Build financial context
        financial_context = ""
        if ratios and len(ratios) > 0:
            r = ratios[0]
            financial_context = f"""
FINANCIAL INDICATORS OF COMPETITIVE STRENGTH:
- Gross Margin: {r.get('grossProfitMarginTTM', 0)*100:.1f}% (high margins may indicate pricing power)
- Operating Margin: {r.get('operatingProfitMarginTTM', 0)*100:.1f}%
- ROE: {r.get('returnOnEquityTTM', 0)*100:.1f}%
- ROA: {r.get('returnOnAssetsTTM', 0)*100:.1f}%
"""
        if key_metrics and len(key_metrics) > 0:
            m = key_metrics[0]
            financial_context += f"""- ROIC: {m.get('roicTTM', 0)*100:.1f}% (high ROIC may indicate moat)
- FCF per Share: ${m.get('freeCashFlowPerShareTTM', 0):.2f}
"""

        content = f"""
COMPANY: {company_name} ({symbol})
INDUSTRY: {industry}
SECTOR: {sector}

{financial_context}
{fmp_competitor_text}

ANNUAL REPORT CONTENT:
{annual_report[:40000] if annual_report else 'Annual report not available'}
"""

        # Get AI-driven industry-specific analysis prompt
        industry_prompt = get_industry_specific_metrics_prompt(symbol, company_name, industry, sector, description)

        ai_prompt = f"""You are a senior equity research analyst analyzing the competitive position of {company_name} ({symbol}) in the {industry} industry.

Based on the annual report and financial metrics, provide a comprehensive competitive analysis:

MOAT_ANALYSIS:
[Analyze the company's economic moat using Warren Buffett's framework. Identify which types of moats exist:
- Network Effects: Does the product become more valuable as more people use it?
- Switching Costs: How difficult/costly is it for customers to switch to competitors?
- Cost Advantages: Does the company have structural cost advantages (scale, location, unique assets)?
- Intangible Assets: Strong brands, patents, regulatory licenses?
- Efficient Scale: Is the market only big enough for limited competitors?
Rate the moat as: Wide (sustainable 20+ years), Narrow (sustainable 10+ years), or None.
Provide specific evidence from the business.]

COMPETITIVE_POSITION:
[Analyze the company's market position:
- Market share and whether it's gaining or losing share
- How the company differentiates from competitors
- Pricing power - can they raise prices without losing customers?
- Customer relationships and retention
- Geographic or segment dominance]

MARKET_DYNAMICS:
[Analyze the competitive landscape:
- Industry structure (fragmented, oligopoly, monopoly)
- Threat of new entrants and barriers to entry
- Threat of substitutes
- Supplier and buyer power
- Industry growth rate and competitive intensity]

COMPETITIVE_ADVANTAGES:
[List 3-5 specific competitive advantages as bullet points, each with evidence]

KEY_COMPETITORS:
IMPORTANT: Every company has competitors. You MUST list 3-5 direct competitors even if they are in adjacent markets.
Use the known competitors from market data provided above, research from the annual report, and your knowledge.
For each competitor, use EXACTLY this format on a single line:
COMPETITOR: CompanyName | TICKER: SYMBOL | THREAT: Why they compete | STRENGTH: Their advantage

Example format:
COMPETITOR: Microsoft Corporation | TICKER: MSFT | THREAT: Competes in cloud computing and productivity software | STRENGTH: Azure growth and enterprise relationships
COMPETITOR: Alphabet Inc | TICKER: GOOGL | THREAT: Competes in AI, cloud, and digital advertising | STRENGTH: Search dominance and AI research

EMERGING_COMPETITORS:
List 2-3 emerging or disruptive competitors. Use EXACTLY this format:
EMERGING: CompanyName | THREAT: What makes them dangerous | DISRUPTION: How they could win

Example format:
EMERGING: Palantir Technologies | THREAT: AI-powered data analytics gaining enterprise traction | DISRUPTION: Could capture data infrastructure market

Be specific. Use real company names from the annual report, market data, and your knowledge. Every company faces competition."""

        logger.info(f"Analyzing competitive position for {symbol}...")
        ai_analysis = analyze_with_ai(ai_prompt, content, use_claude=True, language=language)

        if "Error" in ai_analysis or "unavailable" in ai_analysis:
            logger.info("Claude failed, trying OpenAI...")
            ai_analysis = analyze_with_ai(ai_prompt, content, use_claude=False, language=language)

        # Now run industry-specific analysis
        logger.info(f"Running industry-specific analysis for {symbol} ({industry})...")
        industry_analysis_prompt = f"""You are a senior equity research analyst specializing in the {industry} industry.

{industry_prompt}

IMPORTANT: Extract SPECIFIC numbers, percentages, and data points. Do NOT provide generic analysis.
If a metric is not available in the source documents, state "Not disclosed" rather than guessing.

Format your response as a clear, structured analysis with specific data points."""

        industry_ai_analysis = analyze_with_ai(industry_analysis_prompt, content, use_claude=True, language=language)

        if "Error" in industry_ai_analysis or "unavailable" in industry_ai_analysis:
            logger.info("Claude failed for industry analysis, trying OpenAI...")
            industry_ai_analysis = analyze_with_ai(industry_analysis_prompt, content, use_claude=False, language=language)

        # Store industry analysis
        if industry_ai_analysis and "Error" not in industry_ai_analysis:
            analysis["industry_analysis"] = industry_ai_analysis

        # Parse the moat/competitive response
        if ai_analysis:
            current_section = None
            current_content = []
            advantages = []
            key_competitors = []
            emerging_competitors = []

            for line in ai_analysis.split('\n'):
                line_stripped = line.strip()

                if "MOAT_ANALYSIS:" in line_stripped:
                    current_section = "moat"
                    current_content = []
                elif "COMPETITIVE_POSITION:" in line_stripped:
                    if current_section == "moat":
                        analysis["moat_analysis"] = ' '.join(current_content)
                    current_section = "position"
                    current_content = []
                elif "MARKET_DYNAMICS:" in line_stripped:
                    if current_section == "position":
                        analysis["competitive_position"] = ' '.join(current_content)
                    current_section = "dynamics"
                    current_content = []
                elif "COMPETITIVE_ADVANTAGES:" in line_stripped:
                    if current_section == "dynamics":
                        analysis["market_dynamics"] = ' '.join(current_content)
                    current_section = "advantages"
                    advantages = []
                elif "KEY_COMPETITORS:" in line_stripped:
                    if current_section == "advantages":
                        analysis["competitive_advantages"] = advantages
                    current_section = "key_competitors"
                    key_competitors = []
                elif "EMERGING_COMPETITORS:" in line_stripped:
                    if current_section == "key_competitors":
                        analysis["key_competitors"] = key_competitors
                    current_section = "emerging_competitors"
                    emerging_competitors = []
                elif line_stripped and current_section:
                    if current_section == "advantages":
                        if line_stripped.startswith('-') or line_stripped.startswith('•'):
                            advantages.append(line_stripped.lstrip('-•').strip())
                        elif line_stripped[0].isdigit() and '.' in line_stripped[:3]:
                            advantages.append(line_stripped.split('.', 1)[1].strip())
                    elif current_section == "key_competitors":
                        if "COMPETITOR:" in line_stripped:
                            # Parse structured competitor format
                            competitor = {"name": "", "ticker": "", "threat": "", "strength": ""}
                            parts = line_stripped.split('|')
                            for part in parts:
                                part = part.strip()
                                if part.startswith("COMPETITOR:"):
                                    competitor["name"] = part.replace("COMPETITOR:", "").strip()
                                elif part.startswith("TICKER:"):
                                    competitor["ticker"] = part.replace("TICKER:", "").strip()
                                elif part.startswith("THREAT:"):
                                    competitor["threat"] = part.replace("THREAT:", "").strip()
                                elif part.startswith("STRENGTH:"):
                                    competitor["strength"] = part.replace("STRENGTH:", "").strip()
                            if competitor["name"]:
                                key_competitors.append(competitor)
                        elif line_stripped.startswith('-') or line_stripped.startswith('•'):
                            # Fallback for simpler format
                            key_competitors.append({"name": line_stripped.lstrip('-•').strip(), "ticker": "", "threat": "", "strength": ""})
                    elif current_section == "emerging_competitors":
                        if "EMERGING:" in line_stripped:
                            # Parse structured emerging competitor format
                            competitor = {"name": "", "threat": "", "disruption": ""}
                            parts = line_stripped.split('|')
                            for part in parts:
                                part = part.strip()
                                if part.startswith("EMERGING:"):
                                    competitor["name"] = part.replace("EMERGING:", "").strip()
                                elif part.startswith("THREAT:"):
                                    competitor["threat"] = part.replace("THREAT:", "").strip()
                                elif part.startswith("DISRUPTION:"):
                                    competitor["disruption"] = part.replace("DISRUPTION:", "").strip()
                            if competitor["name"]:
                                emerging_competitors.append(competitor)
                        elif line_stripped.startswith('-') or line_stripped.startswith('•'):
                            emerging_competitors.append({"name": line_stripped.lstrip('-•').strip(), "threat": "", "disruption": ""})
                    else:
                        current_content.append(line_stripped)

            # Capture last section
            if current_section == "emerging_competitors":
                analysis["emerging_competitors"] = emerging_competitors
            elif current_section == "key_competitors":
                analysis["key_competitors"] = key_competitors
            elif current_section == "advantages":
                analysis["competitive_advantages"] = advantages
            elif current_section == "dynamics":
                analysis["market_dynamics"] = ' '.join(current_content)

        # FALLBACK: If AI didn't return competitors, use FMP peers
        if not analysis["key_competitors"] and fmp_competitors:
            logger.info(f"Using FMP peers as fallback for {symbol} competitors")
            for comp in fmp_competitors[:5]:
                mc = comp.get('market_cap', 0)
                mc_str = f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
                analysis["key_competitors"].append({
                    "name": comp.get('name', 'N/A'),
                    "ticker": comp.get('symbol', 'N/A'),
                    "threat": f"Direct competitor in {comp.get('industry', 'same industry')}",
                    "strength": f"Market cap: {mc_str}"
                })

    except Exception as e:
        logger.error(f" in competitive analysis: {e}")
        analysis["moat_analysis"] = f"Analysis unavailable: {str(e)}"

    return analysis


def get_valuations(symbol: str) -> Dict[str, Any]:
    """Get valuation metrics with 10 years historical data and forward estimates"""
    valuations = {
        "current": {},
        "historical": [],
        "forward_estimates": {}
    }

    try:
        # Get current TTM metrics
        metrics = fmp_get(f"key-metrics-ttm/{symbol}")
        ratios = fmp_get(f"ratios-ttm/{symbol}")

        if metrics and len(metrics) > 0:
            metrics_data = metrics[0]
            valuations["current"] = {
                "pe_ratio": metrics_data.get("peRatioTTM", 0),
                "price_to_sales": metrics_data.get("priceToSalesRatioTTM", 0),
                "price_to_book": metrics_data.get("pbRatioTTM", 0),
                "ev_to_ebitda": metrics_data.get("enterpriseValueOverEBITDATTM", 0),
                "peg_ratio": metrics_data.get("pegRatioTTM", 0),
                "price_to_fcf": metrics_data.get("priceToFreeCashFlowsRatioTTM", 0)
            }

        if ratios and len(ratios) > 0:
            ratio_data = ratios[0]
            if valuations["current"].get("pe_ratio", 0) == 0:
                valuations["current"]["pe_ratio"] = ratio_data.get("priceEarningsRatioTTM", 0)

        # Get 8 years of historical ratios
        logger.info(f"Fetching 8 years of historical valuations for {symbol}...")
        historical_ratios = fmp_get(f"ratios/{symbol}", {"period": "annual", "limit": 8})

        if historical_ratios and len(historical_ratios) > 0:
            logger.info(f"Retrieved {len(historical_ratios)} years of historical ratios")

            for ratio in historical_ratios:
                year = ratio.get("calendarYear") or (ratio.get("date", "")[:4] if ratio.get("date") else "")
                if year:
                    valuations["historical"].append({
                        "year": str(year),
                        "pe_ratio": ratio.get("priceEarningsRatio") or 0,
                        "ev_to_ebitda": ratio.get("enterpriseValueMultiple") or 0,
                        "price_to_sales": ratio.get("priceToSalesRatio") or 0,
                        "price_to_book": ratio.get("priceToBookRatio") or ratio.get("priceBookValueRatio") or 0,
                        "price_to_fcf": ratio.get("priceToFreeCashFlowsRatio") or 0,
                        "peg_ratio": ratio.get("priceEarningsToGrowthRatio") or 0,
                        "dividend_yield": ratio.get("dividendYield") or 0
                    })

            # Sort by year ascending (oldest first)
            valuations["historical"].sort(key=lambda x: x["year"])
            logger.debug(f"Historical years: {[h['year'] for h in valuations['historical']]}")
        else:
            logger.warning(f"No historical ratios returned for {symbol}")

        # Get forward estimates (analyst estimates)
        logger.info(f"Fetching forward estimates for {symbol}...")
        analyst_estimates = fmp_get(f"analyst-estimates/{symbol}", {"limit": 10})

        if analyst_estimates:
            from datetime import datetime as dt
            today = dt.now().strftime('%Y-%m-%d')

            # Filter for future dates only and sort by date (nearest first)
            future_estimates = [e for e in analyst_estimates if e.get('date', '') > today]
            future_estimates = sorted(future_estimates, key=lambda x: x.get('date', ''))

            # Get current valuation data for forward calculations
            profile = fmp_get(f"profile/{symbol}")
            current_price = profile[0].get("price", 0) if profile else 0
            shares_outstanding = profile[0].get("sharesOutstanding", 0) if profile else 0
            market_cap = profile[0].get("mktCap", 0) if profile else 0

            # Get enterprise value from key metrics
            key_metrics = fmp_get(f"key-metrics-ttm/{symbol}")
            enterprise_value = 0
            if key_metrics and len(key_metrics) > 0:
                enterprise_value = key_metrics[0].get("enterpriseValueTTM", 0) or 0

            # If EV not available, calculate from market cap + debt - cash
            if not enterprise_value and market_cap:
                balance = fmp_get(f"balance-sheet-statement/{symbol}", {"limit": 1})
                if balance and len(balance) > 0:
                    total_debt = balance[0].get("totalDebt", 0) or 0
                    cash = balance[0].get("cashAndCashEquivalents", 0) or 0
                    enterprise_value = market_cap + total_debt - cash

            # Get latest FCF for estimating forward FCF margin
            cf_statement = fmp_get(f"cash-flow-statement/{symbol}", {"limit": 1})
            latest_fcf = 0
            latest_revenue = 0
            fcf_margin = 0
            if cf_statement and len(cf_statement) > 0:
                latest_fcf = cf_statement[0].get("freeCashFlow", 0) or 0
            income_stmt = fmp_get(f"income-statement/{symbol}", {"limit": 1})
            if income_stmt and len(income_stmt) > 0:
                latest_revenue = income_stmt[0].get("revenue", 0) or 0
                if latest_revenue > 0 and latest_fcf:
                    fcf_margin = latest_fcf / latest_revenue  # FCF as % of revenue

            for estimate in future_estimates[:2]:  # Only +1Y and +2Y
                year = estimate.get("date", "")[:4] if estimate.get("date") else ""
                if not year:
                    continue

                est_eps = estimate.get("estimatedEpsAvg", 0) or 0
                est_revenue = estimate.get("estimatedRevenueAvg", 0) or 0
                est_ebitda = estimate.get("estimatedEbitdaAvg", 0) or 0
                est_net_income = estimate.get("estimatedNetIncomeAvg", 0) or 0

                # Calculate forward P/E = Price / Estimated EPS
                forward_pe = round(current_price / est_eps, 2) if est_eps and est_eps > 0 and current_price else 0

                # Calculate forward P/S = Market Cap / Estimated Revenue
                forward_ps = round(market_cap / est_revenue, 2) if est_revenue and est_revenue > 0 and market_cap else 0

                # Calculate forward EV/EBITDA = Enterprise Value / Estimated EBITDA
                forward_ev_ebitda = round(enterprise_value / est_ebitda, 2) if est_ebitda and est_ebitda > 0 and enterprise_value else 0

                # Estimate forward FCF using historical FCF margin applied to estimated revenue
                est_fcf = est_revenue * fcf_margin if fcf_margin and est_revenue else 0
                forward_price_fcf = round(market_cap / est_fcf, 2) if est_fcf and est_fcf > 0 and market_cap else 0

                # Calculate forward P/B (use current book value as approximation)
                # Book value doesn't change drastically year-over-year
                current_pb = valuations["current"].get("price_to_book", 0)
                forward_pb = current_pb  # Approximate - book value relatively stable

                valuations["forward_estimates"][year] = {
                    "year": year,
                    "estimated_eps": est_eps,
                    "estimated_revenue": est_revenue,
                    "estimated_ebitda": est_ebitda,
                    "estimated_net_income": est_net_income,
                    "forward_pe": forward_pe,
                    "forward_ps": forward_ps,
                    "forward_ev_ebitda": forward_ev_ebitda,
                    "forward_price_fcf": forward_price_fcf,
                    "forward_pb": forward_pb,
                    "estimated_eps_low": estimate.get("estimatedEpsLow", 0) or 0,
                    "estimated_eps_high": estimate.get("estimatedEpsHigh", 0) or 0,
                    "estimated_revenue_low": estimate.get("estimatedRevenueLow", 0) or 0,
                    "estimated_revenue_high": estimate.get("estimatedRevenueHigh", 0) or 0,
                    "num_analysts_eps": estimate.get("numberAnalystsEstimatedEps", 0) or 0,
                    "num_analysts_revenue": estimate.get("numberAnalystsEstimatedRevenue", 0) or 0
                }

        # Also keep legacy format for backward compatibility
        if valuations["current"]:
            valuations.update(valuations["current"])

    except Exception as e:
        logger.error(f" fetching valuations: {e}")
        import traceback
        traceback.print_exc()

    return valuations


def markdown_to_html(text: str) -> str:
    """Convert markdown formatting to HTML for ReportLab"""
    import re
    if not text:
        return text
    # Escape HTML special characters first (except for our conversions)
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    # Convert markdown headers ### and ## to bold (remove # symbols)
    text = re.sub(r'^###\s*(.+?)$', r'<b>\1</b>', text, flags=re.MULTILINE)
    text = re.sub(r'^##\s*(.+?)$', r'<b>\1</b>', text, flags=re.MULTILINE)
    # Convert markdown bold **text** to HTML <b>text</b>
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    # Convert markdown italic *text* to HTML <i>text</i> (single asterisks)
    text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
    return text


def get_standard_table_style(has_row_headers=True):
    """
    Returns a standardized TableStyle for consistent formatting across all tables.
    - White background throughout
    - Black text throughout
    - Bold only on header row (row 0)
    - Row headers: white background, black text (not bold)
    """
    style_commands = [
        # Header row (row 0) - white background, black bold text
        ('BACKGROUND', (0, 0), (-1, 0), colors.white),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),

        # Data cells - white background, black text
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),

        # General formatting
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#dee2e6')),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
    ]

    # Row headers: left-align first column, but keep white background and regular text
    if has_row_headers:
        style_commands.extend([
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ])

    return TableStyle(style_commands)


def generate_pdf_report(report_data: Dict[str, Any], language: str = "en") -> io.BytesIO:
    """Generate a PDF report with company logo"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                           topMargin=0.75*inch, bottomMargin=0.75*inch,
                           leftMargin=0.75*inch, rightMargin=0.75*inch)

    # Helper function for translations
    def t(key: str) -> str:
        return get_translation(key, language)

    # Container for PDF elements
    elements = []
    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#2c2c2c'),
        spaceAfter=30,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )

    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=16,
        textColor=colors.HexColor('#2c2c2c'),
        spaceAfter=12,
        spaceBefore=12,
        fontName='Helvetica-Bold'
    )

    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['BodyText'],
        fontSize=10,
        spaceAfter=12,
        leading=14
    )

    subheading_style = ParagraphStyle(
        'CustomSubheading',
        parent=styles['Heading3'],
        fontSize=12,
        textColor=colors.HexColor('#2c2c2c'),
        spaceAfter=8,
        spaceBefore=8,
        fontName='Helvetica-Bold'
    )

    # Table cell style for text wrapping
    cell_style = ParagraphStyle(
        'TableCell',
        parent=styles['BodyText'],
        fontSize=8,
        leading=10,
        spaceAfter=0,
        spaceBefore=0
    )

    cell_style_bold = ParagraphStyle(
        'TableCellBold',
        parent=cell_style,
        fontName='Helvetica-Bold'
    )

    # Add company logo if it exists (1.2x larger)
    logo_path = 'company_logo.png'
    if os.path.exists(logo_path):
        try:
            img = Image(logo_path, width=4.68*inch, height=1.56*inch)
            img.hAlign = 'CENTER'
            elements.append(img)
            elements.append(Spacer(1, 0.1*inch))
        except (IOError, OSError, ValueError) as e:
            logger.warning(f" Could not load company logo: {e}")

    # Tagline below logo
    tagline_style = ParagraphStyle(
        'Tagline',
        parent=styles['BodyText'],
        fontSize=11,
        textColor=colors.HexColor('#666666'),
        alignment=TA_CENTER,
        spaceAfter=20,
        fontName='Helvetica-Oblique'
    )
    elements.append(Paragraph(t("tagline"), tagline_style))
    elements.append(Spacer(1, 0.2*inch))

    # Title - Company Report
    symbol = report_data.get('symbol', 'N/A')
    business_overview = report_data.get('business_overview', {})
    company_name = business_overview.get('company_name', symbol)

    elements.append(Paragraph(t("company_report"), title_style))
    elements.append(Paragraph(f"{company_name} ({symbol})",
                             ParagraphStyle('CompanyName', parent=styles['Heading1'], fontSize=18,
                                          alignment=TA_CENTER, textColor=colors.HexColor('#2c2c2c'),
                                          spaceAfter=10, fontName='Helvetica-Bold')))
    elements.append(Paragraph(f"{t('generated')}: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                             ParagraphStyle('Timestamp', parent=body_style, alignment=TA_CENTER, fontSize=9, textColor=colors.grey)))
    elements.append(Spacer(1, 0.3*inch))

    # ============ SECTION 1: Company Details ============
    company_name = business_overview.get('company_name', symbol)
    elements.append(Paragraph(f"{t('section_1')} — {company_name} ({symbol})", heading_style))

    # Add date line
    date_str = datetime.now().strftime("%B %d, %Y")
    date_style = ParagraphStyle('DateStyle', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#666666'), spaceAfter=8)
    elements.append(Paragraph(f"{t('as_of')} {date_str}", date_style))

    # Format values
    price_str = f"${business_overview.get('price', 0):.2f}" if business_overview.get('price') else 'N/A'
    market_cap = business_overview.get('market_cap', 0)
    # Format market cap with T for trillions, B for billions
    if market_cap and market_cap >= 1e12:
        market_cap_str = f"${market_cap/1e12:.2f}T"
    elif market_cap and market_cap >= 1e9:
        market_cap_str = f"${market_cap/1e9:.2f}B"
    else:
        market_cap_str = f"${market_cap/1e6:.2f}M" if market_cap else 'N/A'
    high_52 = business_overview.get('week_52_high')
    high_52_str = f"${high_52:.2f}" if isinstance(high_52, (int, float)) else 'N/A'
    low_52 = business_overview.get('week_52_low')
    low_52_str = f"${low_52:.2f}" if isinstance(low_52, (int, float)) else 'N/A'
    beta = business_overview.get('beta')
    beta_str = f"{beta:.2f}" if isinstance(beta, (int, float)) and beta else 'N/A'
    employees = business_overview.get('employees')
    # Handle int, float, or string
    if isinstance(employees, int):
        employees_str = f"{employees:,}"
    elif isinstance(employees, float):
        employees_str = f"{int(employees):,}"
    elif isinstance(employees, str) and employees.isdigit():
        employees_str = f"{int(employees):,}"
    else:
        employees_str = str(employees) if employees and employees != "N/A" else 'N/A'

    # Format enterprise value with T for trillions
    ev = business_overview.get('enterprise_value', 0)
    if ev and ev > 0:
        if ev >= 1e12:
            ev_str = f"${ev/1e12:.2f}T"
        elif ev >= 1e9:
            ev_str = f"${ev/1e9:.2f}B"
        elif ev >= 1e6:
            ev_str = f"${ev/1e6:.2f}M"
        else:
            ev_str = f"${ev:,.0f}"
    else:
        ev_str = "N/A"

    # Format dividend yield
    div_yield = business_overview.get('dividend_yield', 0)
    div_yield_str = f"{div_yield:.2f}%" if isinstance(div_yield, (int, float)) and div_yield else 'N/A'

    # Company Details - 6-column layout (keep together on one page)
    headquarters = business_overview.get('headquarters', 'N/A')

    # 6-column table with headers and values
    details_data = [
        [t('ticker'), t('price'), t('market_cap'), t('enterprise_value'), t('52w_high'), t('52w_low')],
        [symbol, price_str, market_cap_str, ev_str, high_52_str, low_52_str],
        [t('sector'), t('industry'), t('headquarters'), t('employees'), t('beta'), t('div_yield')],
        [business_overview.get('sector', 'N/A'), business_overview.get('industry', 'N/A'), headquarters, employees_str, beta_str, div_yield_str],
    ]

    col_width = 1.1*inch  # 6 columns fit within page width
    details_table = Table(details_data, colWidths=[col_width]*6)
    details_table.setStyle(TableStyle([
        # Header rows styling (rows 0 and 2) - white background, black bold text
        ('BACKGROUND', (0, 0), (-1, 0), colors.white),
        ('BACKGROUND', (0, 2), (-1, 2), colors.white),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('TEXTCOLOR', (0, 2), (-1, 2), colors.black),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 2), (-1, 2), 'Helvetica-Bold'),
        # Value rows styling (rows 1 and 3)
        ('BACKGROUND', (0, 1), (-1, 1), colors.white),
        ('BACKGROUND', (0, 3), (-1, 3), colors.white),
        ('TEXTCOLOR', (0, 1), (-1, 1), colors.black),
        ('TEXTCOLOR', (0, 3), (-1, 3), colors.black),
        ('FONTNAME', (0, 1), (-1, 1), 'Helvetica'),
        ('FONTNAME', (0, 3), (-1, 3), 'Helvetica'),
        # General styling
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#dee2e6')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    elements.append(KeepTogether([
        details_table,
        Spacer(1, 0.2*inch)
    ]))

    # ============ SECTION 2: Business Overview ============
    elements.append(Paragraph(t("section_2"), heading_style))
    description = business_overview.get('description', 'N/A')

    # Split description into paragraphs for better formatting (handle both \n\n and ## headers)
    if description and description != 'N/A':
        # Split by double newlines or markdown headers
        import re
        # First, ensure headers have paragraph breaks before them
        description = re.sub(r'\n(##\s)', r'\n\n\1', description)
        paragraphs = description.split('\n\n')

        for para in paragraphs:
            para = para.strip()
            if para:
                # Check if it's a section header (starts with ##)
                if para.startswith('##'):
                    # Remove ## and make it a subheading
                    header_text = para.lstrip('#').strip()
                    elements.append(Spacer(1, 0.1*inch))
                    elements.append(Paragraph(f"<b>{header_text}</b>", body_style))
                else:
                    elements.append(Paragraph(markdown_to_html(para), body_style))
    else:
        elements.append(Paragraph("No detailed description available", body_style))

    elements.append(Spacer(1, 0.2*inch))

    # ============ SECTION 3: Competitive Landscape ============
    elements.append(Paragraph(t("section_3"), heading_style))

    # Add competitive analysis details if available
    competitive_analysis = report_data.get('competitive_analysis', {})

    # Key Competitors Table
    key_competitors = competitive_analysis.get('key_competitors', [])
    if key_competitors:
        elements.append(Paragraph("<b>Key Competitors:</b>", body_style))

        # Build competitors table with Paragraphs for wrapping
        comp_header = [
            Paragraph('Competitor', cell_style_bold),
            Paragraph('Ticker', cell_style_bold),
            Paragraph('Competitive Threat', cell_style_bold),
            Paragraph('Their Strength', cell_style_bold)
        ]
        comp_rows = [comp_header]
        for comp in key_competitors[:5]:
            comp_rows.append([
                Paragraph(comp.get('name', 'N/A'), cell_style),
                Paragraph(comp.get('ticker', 'N/A'), cell_style),
                Paragraph(comp.get('threat', 'N/A')[:80], cell_style),
                Paragraph(comp.get('strength', 'N/A')[:80], cell_style)
            ])

        if len(comp_rows) > 1:
            comp_table = Table(comp_rows, colWidths=[1.3*inch, 0.7*inch, 2.0*inch, 2.0*inch])
            comp_table.setStyle(get_standard_table_style(has_row_headers=True))
            elements.append(comp_table)
            elements.append(Spacer(1, 0.15*inch))

    # Emerging Competitors Table
    emerging_competitors = competitive_analysis.get('emerging_competitors', [])
    if emerging_competitors:
        elements.append(Paragraph("<b>Emerging Competitors:</b>", body_style))

        # Build emerging competitors table
        emerg_header = [
            Paragraph('Competitor', cell_style_bold),
            Paragraph('Threat Level', cell_style_bold),
            Paragraph('Disruption Potential', cell_style_bold)
        ]
        emerg_rows = [emerg_header]
        for emerg in emerging_competitors[:3]:
            emerg_rows.append([
                Paragraph(emerg.get('name', 'N/A'), cell_style),
                Paragraph(emerg.get('threat', 'N/A')[:100], cell_style),
                Paragraph(emerg.get('disruption', 'N/A')[:100], cell_style)
            ])

        if len(emerg_rows) > 1:
            emerg_table = Table(emerg_rows, colWidths=[1.5*inch, 2.25*inch, 2.25*inch])
            emerg_table.setStyle(get_standard_table_style(has_row_headers=True))
            elements.append(emerg_table)
            elements.append(Spacer(1, 0.15*inch))

    # Competitive Advantages
    advantages = report_data.get('competitive_advantages', [])
    if not advantages:
        advantages = competitive_analysis.get('competitive_advantages', [])
    if advantages:
        elements.append(Paragraph("<b>Competitive Advantages:</b>", body_style))
        for i, advantage in enumerate(advantages[:5], 1):
            elements.append(Paragraph(f"{i}. {markdown_to_html(advantage)}", body_style))
        elements.append(Spacer(1, 0.1*inch))

    # Moat Analysis
    moat = competitive_analysis.get('moat_analysis', '')
    if moat and len(moat) > 10:
        elements.append(Paragraph("<b>Moat Analysis:</b>", body_style))
        for para in moat.split('\n\n')[:2]:
            if para.strip():
                elements.append(Paragraph(markdown_to_html(para.strip()[:400]), body_style))
        elements.append(Spacer(1, 0.1*inch))

    # Market Dynamics
    dynamics = competitive_analysis.get('market_dynamics', '')
    if dynamics and len(dynamics) > 10:
        elements.append(Paragraph("<b>Market Dynamics:</b>", body_style))
        for para in dynamics.split('\n\n')[:2]:
            if para.strip():
                elements.append(Paragraph(markdown_to_html(para.strip()[:400]), body_style))
        elements.append(Spacer(1, 0.1*inch))

    elements.append(Spacer(1, 0.2*inch))

    # ============ SECTION 4: Risks and Red Flags ============
    elements.append(Paragraph(t("section_4"), heading_style))
    risks = report_data.get('risks', {})

    # Company Red Flag
    company_specific = risks.get('company_specific', [])
    if company_specific:
        elements.append(Paragraph("A) Company Red Flags", subheading_style))
        for i, risk in enumerate(company_specific[:8], 1):
            elements.append(Paragraph(f"{i}. {markdown_to_html(risk)}", body_style))
        elements.append(Spacer(1, 0.1*inch))

    # General Risk
    general = risks.get('general', [])
    if general:
        elements.append(Paragraph("B) General Risks", subheading_style))
        for i, risk in enumerate(general[:8], 1):
            elements.append(Paragraph(f"{i}. {markdown_to_html(risk)}", body_style))

    elements.append(Spacer(1, 0.2*inch))

    # ============ SECTION 5: Revenue and Margins ============
    elements.append(Paragraph(t("section_5"), heading_style))
    revenue_data = report_data.get('revenue_data', {})

    # Historical Revenue & Margins Table (8 years + estimates)
    historical_margins = revenue_data.get('historical_margins', [])
    estimates = revenue_data.get('estimates', {})
    if historical_margins:
        elements.append(Paragraph("<b>Revenue & Margins - 8 Year History + Estimates</b>", body_style))

        # Reverse historical data for chronological order (oldest first, newest last)
        # Then estimates (future) will naturally appear at the right end
        hist_data = list(reversed(historical_margins[:8]))

        # Build header row with periods using Paragraphs for wrapping
        periods = [m.get('period', 'N/A') for m in hist_data]
        header_row = [Paragraph('Metric', cell_style_bold)] + [Paragraph(str(p), cell_style_bold) for p in periods]

        # Add estimate columns to header (future years at the end)
        est1 = estimates.get('year_1', {})
        est2 = estimates.get('year_2', {})
        if est1:
            header_row.append(Paragraph(est1.get('period', '+1Y'), cell_style_bold))
        if est2:
            header_row.append(Paragraph(est2.get('period', '+2Y'), cell_style_bold))

        # Helper function to format revenue
        def format_rev(rev):
            if rev is None or rev == 0:
                return "N/A"
            if rev >= 1e9:
                return f"${rev/1e9:.1f}B"
            elif rev >= 1e6:
                return f"${rev/1e6:.0f}M"
            else:
                return f"${rev:,.0f}"

        # Build Revenue row (at top)
        revenue_row = [Paragraph('Revenue', cell_style)]
        for m in hist_data:
            revenue_row.append(Paragraph(format_rev(m.get('revenue', 0)), cell_style))
        if est1:
            revenue_row.append(Paragraph(format_rev(est1.get('revenue')), cell_style))
        if est2:
            revenue_row.append(Paragraph(format_rev(est2.get('revenue')), cell_style))

        # Build margin rows with Paragraphs
        gross_row = [Paragraph('Gross Margin', cell_style)]
        for m in hist_data:
            gross_row.append(Paragraph(f"{m.get('gross_margin', 0):.1f}%", cell_style))
        if est1:
            gm1 = est1.get('gross_margin')
            gross_row.append(Paragraph(f"{gm1:.1f}%" if gm1 else "N/A", cell_style))
        if est2:
            gm2 = est2.get('gross_margin')
            gross_row.append(Paragraph(f"{gm2:.1f}%" if gm2 else "N/A", cell_style))

        operating_row = [Paragraph('Op. Margin', cell_style)]
        for m in hist_data:
            operating_row.append(Paragraph(f"{m.get('operating_margin', 0):.1f}%", cell_style))
        if est1:
            om1 = est1.get('operating_margin')
            operating_row.append(Paragraph(f"{om1:.1f}%" if om1 else "N/A", cell_style))
        if est2:
            om2 = est2.get('operating_margin')
            operating_row.append(Paragraph(f"{om2:.1f}%" if om2 else "N/A", cell_style))

        net_row = [Paragraph('Net Margin', cell_style)]
        for m in hist_data:
            net_row.append(Paragraph(f"{m.get('net_margin', 0):.1f}%", cell_style))
        if est1:
            nm1 = est1.get('net_margin')
            net_row.append(Paragraph(f"{nm1:.1f}%" if nm1 else "N/A", cell_style))
        if est2:
            nm2 = est2.get('net_margin')
            net_row.append(Paragraph(f"{nm2:.1f}%" if nm2 else "N/A", cell_style))

        margins_history_data = [header_row, revenue_row, gross_row, operating_row, net_row]

        # Calculate column widths based on number of periods (fit within 7 inch page)
        num_cols = len(header_row)
        data_col_width = 5.5 * inch / (num_cols - 1) if num_cols > 1 else 0.5 * inch
        col_widths = [1.0*inch] + [data_col_width] * (num_cols - 1)

        margins_history_table = Table(margins_history_data, colWidths=col_widths)
        margins_history_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(KeepTogether([margins_history_table, Spacer(1, 0.2*inch)]))
    else:
        # Fallback to simple margins if no historical data
        margins = revenue_data.get('margins', {})
        if margins:
            margins_data = [
                ['Margin Type', 'Value'],
                ['Gross Margin', f"{margins.get('gross_margin', 0):.2f}%"],
                ['Operating Margin', f"{margins.get('operating_margin', 0):.2f}%"],
                ['Net Margin', f"{margins.get('net_margin', 0):.2f}%"],
            ]

            margins_table = Table(margins_data, colWidths=[3*inch, 3*inch])
            margins_table.setStyle(get_standard_table_style(has_row_headers=True))
            elements.append(KeepTogether([margins_table, Spacer(1, 0.2*inch)]))

    # Segments - display as table with revenue and percentage
    segments = revenue_data.get('segments', [])
    if segments:
        # Calculate total revenue for percentages
        total_revenue = sum(s.get('revenue', 0) or 0 for s in segments)

        # Build segment table with Paragraph objects for text wrapping
        segment_table_data = [[
            Paragraph('Segment', cell_style_bold),
            Paragraph('Revenue', cell_style_bold),
            Paragraph('% of Total', cell_style_bold)
        ]]
        for segment in segments[:10]:  # Show up to 10 segments
            segment_name = segment.get('name', 'N/A')
            segment_revenue = segment.get('revenue') or 0
            if segment_revenue > 0:
                # Format revenue
                if segment_revenue >= 1e9:
                    rev_str = f"${segment_revenue/1e9:.2f}B"
                elif segment_revenue >= 1e6:
                    rev_str = f"${segment_revenue/1e6:.2f}M"
                else:
                    rev_str = f"${segment_revenue:,.0f}"
                # Calculate percentage
                pct = (segment_revenue / total_revenue * 100) if total_revenue > 0 else 0
                segment_table_data.append([
                    Paragraph(segment_name, cell_style),
                    Paragraph(rev_str, cell_style),
                    Paragraph(f"{pct:.1f}%", cell_style)
                ])

        if len(segment_table_data) > 1:  # Has data beyond header
            segment_table = Table(segment_table_data, colWidths=[3*inch, 1.5*inch, 1.5*inch])
            segment_table.setStyle(get_standard_table_style(has_row_headers=True))
            elements.append(KeepTogether([segment_table, Spacer(1, 0.15*inch)]))

    # AI Segment Analysis
    if segments and segments[0].get('ai_analysis'):
        elements.append(Paragraph("<b>Segment Analysis:</b>", body_style))
        ai_analysis = segments[0].get('ai_analysis', '')
        # Split into paragraphs for better formatting
        for para in ai_analysis.split('\n\n'):
            if para.strip():
                elements.append(Paragraph(markdown_to_html(para.strip()), body_style))
                elements.append(Spacer(1, 0.1*inch))

    elements.append(Spacer(1, 0.2*inch))

    # ============ SECTION 6: Highlights from Recent Quarters ============
    elements.append(Paragraph(t("section_6"), heading_style))
    highlights_data = report_data.get('recent_highlights', {})

    # Handle both old list format and new dict format
    if isinstance(highlights_data, dict):
        quarterly_data = highlights_data.get('quarterly_data', [])
        qoq_commentary = highlights_data.get('qoq_commentary', {"positive": [], "negative": []})
    else:
        quarterly_data = []
        qoq_commentary = {"positive": [], "negative": []}

    if quarterly_data:
        # Build quarterly metrics table with dates across top
        metrics = [
            ('Revenue', 'revenue', 'B'),
            ('Gross Margin', 'gross_margin', '%'),
            ('Op. Income', 'operating_income', 'B'),
            ('Op. Margin', 'operating_margin', '%'),
            ('Net Income', 'net_income', 'B'),
            ('EPS', 'eps', '$'),
            ('Op. Cash Flow', 'operating_cash_flow', 'B'),
        ]

        # Check for unique company-specific data availability
        has_deferred_revenue = any(q.get('deferred_revenue', 0) > 0 for q in quarterly_data)
        has_eps_surprise = any(q.get('eps_surprise') is not None for q in quarterly_data)

        # Add unique metrics if available
        if has_deferred_revenue:
            metrics.append(('Deferred Rev', 'deferred_revenue', 'B'))
        if has_eps_surprise:
            metrics.append(('EPS Surprise', 'eps_surprise', 'surprise'))

        # Build header row with Paragraphs for wrapping
        quarters = [q.get('quarter', '') for q in quarterly_data]
        header = [Paragraph('Metric', cell_style_bold)] + [Paragraph(qtr, cell_style_bold) for qtr in quarters]

        # Build data rows with Paragraphs
        table_rows = [header]
        for metric_name, metric_key, fmt_type in metrics:
            row = [Paragraph(metric_name, cell_style)]
            for q in quarterly_data:
                val = q.get(metric_key, 0) if metric_key != 'eps_surprise' else q.get(metric_key)
                if fmt_type == 'B':
                    cell_val = f"${val/1e9:.2f}B" if val else 'N/A'
                elif fmt_type == '%':
                    cell_val = f"{val:.1f}%" if val else 'N/A'
                elif fmt_type == '$':
                    cell_val = f"${val:.2f}" if val else 'N/A'
                elif fmt_type == 'surprise':
                    cell_val = f"{val:+.1f}%" if val is not None else 'N/A'
                else:
                    cell_val = 'N/A'
                row.append(Paragraph(cell_val, cell_style))
            table_rows.append(row)

        # Calculate column widths (fit within page)
        num_cols = len(header)
        data_col_width = 5.5 * inch / (num_cols - 1) if num_cols > 1 else 1.0 * inch
        col_widths = [1.0*inch] + [data_col_width] * (num_cols - 1)

        highlights_table = Table(table_rows, colWidths=col_widths)
        highlights_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(highlights_table)
        elements.append(Spacer(1, 0.15*inch))

        # QoQ Commentary
        elements.append(Paragraph("<b>Quarter-over-Quarter Changes:</b>", body_style))

        positive = qoq_commentary.get('positive', [])
        negative = qoq_commentary.get('negative', [])

        if positive:
            elements.append(Paragraph("<b>Positive Trends:</b>", body_style))
            for change in positive[:4]:
                elements.append(Paragraph(f"  • {markdown_to_html(change)}", body_style))

        if negative:
            elements.append(Paragraph("<b>Areas of Concern:</b>", body_style))
            for change in negative[:4]:
                elements.append(Paragraph(f"  • {markdown_to_html(change)}", body_style))

    # Key Business Drivers (AI-extracted, company-specific KPIs)
    key_drivers = highlights_data.get('key_drivers', []) if isinstance(highlights_data, dict) else []
    if key_drivers:
        elements.append(Spacer(1, 0.15*inch))
        elements.append(Paragraph("<b>Key Business Drivers:</b>", body_style))
        elements.append(Paragraph("<i>AI-identified metrics most important to this company</i>", body_style))

        # Build drivers table with Paragraph objects for text wrapping
        drivers_header = [
            Paragraph('Metric', cell_style_bold),
            Paragraph('Value', cell_style_bold),
            Paragraph('Change', cell_style_bold),
            Paragraph('Insight', cell_style_bold)
        ]
        drivers_rows = [drivers_header]
        for driver in key_drivers[:5]:  # Limit to 5 drivers
            insight_text = driver.get('insight', '')
            if len(insight_text) > 60:
                insight_text = insight_text[:60] + '...'
            drivers_rows.append([
                Paragraph(driver.get('name', ''), cell_style),
                Paragraph(driver.get('value', ''), cell_style),
                Paragraph(driver.get('change', ''), cell_style),
                Paragraph(insight_text, cell_style)
            ])

        drivers_table = Table(drivers_rows, colWidths=[1.5*inch, 1.0*inch, 0.8*inch, 2.2*inch])
        drivers_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(drivers_table)

    elements.append(Spacer(1, 0.2*inch))

    # ============ SECTION 7: Key Metrics ============
    elements.append(Paragraph(t("section_7"), heading_style))
    key_metrics = report_data.get('key_metrics', {})
    if key_metrics:
        # Helper function to format percentage or show N/A
        def fmt_pct(val):
            if val is None or (isinstance(val, (int, float)) and val == 0):
                return 'N/A'
            return f"{val:.1f}%"

        # Build the key metrics table
        key_metrics_data = [
            ['', '5 Year Avg', '3 Yr Avg', 'TTM', 'Estimated 1 Yr', 'Estimated 2 Yr'],
            [
                'Revenue Growth',
                fmt_pct(key_metrics.get('revenue_growth_5yr')),
                fmt_pct(key_metrics.get('revenue_growth_3yr')),
                fmt_pct(key_metrics.get('revenue_growth_ttm')),
                fmt_pct(key_metrics.get('revenue_growth_est_1yr')),
                fmt_pct(key_metrics.get('revenue_growth_est_2yr'))
            ],
            [
                'Gross Margin',
                fmt_pct(key_metrics.get('gross_margin_5yr')),
                fmt_pct(key_metrics.get('gross_margin_3yr')),
                fmt_pct(key_metrics.get('gross_margin', 0) * 100),
                fmt_pct(key_metrics.get('gross_margin_est_1yr')),
                fmt_pct(key_metrics.get('gross_margin_est_2yr'))
            ],
            [
                'Operating Margin',
                fmt_pct(key_metrics.get('operating_margin_5yr')),
                fmt_pct(key_metrics.get('operating_margin_3yr')),
                fmt_pct(key_metrics.get('operating_margin', 0) * 100),
                fmt_pct(key_metrics.get('operating_margin_est_1yr')),
                fmt_pct(key_metrics.get('operating_margin_est_2yr'))
            ],
            [
                'Net Income Margin',
                fmt_pct(key_metrics.get('net_income_margin_5yr')),
                fmt_pct(key_metrics.get('net_income_margin_3yr')),
                fmt_pct(key_metrics.get('net_margin', 0) * 100),
                fmt_pct(key_metrics.get('net_income_margin_est_1yr')),
                fmt_pct(key_metrics.get('net_income_margin_est_2yr'))
            ]
        ]

        key_metrics_table = Table(key_metrics_data, colWidths=[1.5*inch, 1*inch, 1*inch, 1*inch, 1.2*inch, 1.2*inch])
        key_metrics_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(KeepTogether([key_metrics_table, Spacer(1, 0.15*inch)]))

        # Build the second table for ROIC, ROE, ROA, WACC
        returns_data = [
            ['', '5 Year Avg', '3 Yr Avg', 'TTM'],
            [
                'ROIC',
                fmt_pct(key_metrics.get('roic_5yr')),
                fmt_pct(key_metrics.get('roic_3yr')),
                fmt_pct(key_metrics.get('roic'))
            ],
            [
                'ROE',
                fmt_pct(key_metrics.get('roe_5yr')),
                fmt_pct(key_metrics.get('roe_3yr')),
                fmt_pct(key_metrics.get('roe'))
            ],
            [
                'ROA',
                fmt_pct(key_metrics.get('roa_5yr')),
                fmt_pct(key_metrics.get('roa_3yr')),
                fmt_pct(key_metrics.get('roa'))
            ],
            [
                'WACC',
                '-',
                '-',
                fmt_pct(key_metrics.get('wacc'))
            ]
        ]

        returns_table = Table(returns_data, colWidths=[1.5*inch, 1.5*inch, 1.5*inch, 1.5*inch])
        returns_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(KeepTogether([returns_table, Spacer(1, 0.2*inch)]))

    # ============ SECTION 8: Valuations ============
    elements.append(Paragraph(t("section_8"), heading_style))
    valuations = report_data.get('valuations', {})
    if valuations:
        current_val = valuations.get('current', valuations)
        historical = valuations.get('historical', [])
        forward_estimates = valuations.get('forward_estimates', {})

        # Sort historical by year ascending (chronological: oldest first)
        sorted_historical = sorted(historical, key=lambda x: x.get('year', ''))

        # Sort forward estimates by year (nearest first)
        sorted_forward = sorted(forward_estimates.values(), key=lambda x: x.get('year', ''))[:2]

        # Define metrics: (display_name, key, forward_key, format_spec)
        val_metrics = [
            ('P/E', 'pe_ratio', 'forward_pe', '.2f'),
            ('EV/EBITDA', 'ev_to_ebitda', 'forward_ev_ebitda', '.2f'),
            ('P/S', 'price_to_sales', 'forward_ps', '.2f'),
            ('P/B', 'price_to_book', 'forward_pb', '.2f'),
            ('Price/FCF', 'price_to_fcf', 'forward_price_fcf', '.2f'),
            ('PEG Ratio', 'peg_ratio', None, '.2f'),
            ('Div Yld', 'dividend_yield', None, '.2f')
        ]

        # Build header row: Metric, historical years, TTM, then forward estimates
        header = ['Metric'] + [h.get('year', '') for h in sorted_historical] + ['TTM']
        for fwd in sorted_forward:
            header.append(f"FY{fwd.get('year', '')}E")

        # Build data rows
        val_data = [header]
        for metric_name, metric_key, forward_key, fmt in val_metrics:
            row = [metric_name]
            # Historical values (chronological)
            for h in sorted_historical:
                val = h.get(metric_key, 0)
                if metric_key == 'dividend_yield' and val:
                    row.append(f"{val * 100:.2f}%" if val < 1 else f"{val:.2f}%")
                else:
                    row.append(f"{val:{fmt}}" if val else 'N/A')
            # TTM value
            ttm_val = current_val.get(metric_key, 0)
            if metric_key == 'dividend_yield' and ttm_val:
                row.append(f"{ttm_val * 100:.2f}%" if ttm_val < 1 else f"{ttm_val:.2f}%")
            else:
                row.append(f"{ttm_val:{fmt}}" if ttm_val else 'N/A')
            # Forward estimates
            for fwd in sorted_forward:
                if forward_key:
                    fwd_val = fwd.get(forward_key, 0)
                    row.append(f"{fwd_val:{fmt}}" if fwd_val else 'N/A')
                else:
                    row.append('N/A')
            val_data.append(row)

        # Calculate column widths - narrower for many columns
        num_cols = len(header)
        if num_cols <= 5:
            col_widths = [1.2*inch] + [1.0*inch] * (num_cols - 1)
        else:
            col_widths = [0.9*inch] + [0.55*inch] * (num_cols - 1)

        val_table = Table(val_data, colWidths=col_widths)
        val_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(KeepTogether([val_table, Spacer(1, 0.2*inch)]))

    # ============ SECTION 9: Balance Sheet / Credit Metrics ============
    elements.append(Paragraph(t("section_9"), heading_style))
    balance_sheet = report_data.get('balance_sheet_metrics', {})

    # Helper functions
    def fmt_billions(val):
        if val is None or val == 0:
            return 'N/A'
        return f"${val/1e9:.2f}B"

    def fmt_ratio(val, decimals=2):
        if val is None or val == 0:
            return 'N/A'
        return f"{val:.{decimals}f}"

    # Balance Sheet Summary
    bs_current = balance_sheet.get('current', {})
    if bs_current:
        elements.append(Paragraph("Balance Sheet Summary", subheading_style))

        bs_data = [
            ['Item', 'Value'],
            ['Total Assets', fmt_billions(bs_current.get('total_assets'))],
            ['Total Liabilities', fmt_billions(bs_current.get('total_liabilities'))],
            ['Total Equity', fmt_billions(bs_current.get('total_equity'))],
            ['Cash & Equivalents', fmt_billions(bs_current.get('cash_and_equivalents'))],
            ['Total Debt', fmt_billions(bs_current.get('total_debt'))],
            ['Net Debt', fmt_billions(bs_current.get('net_debt'))],
            ['Working Capital', fmt_billions(bs_current.get('working_capital'))],
        ]

        bs_table = Table(bs_data, colWidths=[3*inch, 3*inch])
        bs_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(KeepTogether([bs_table, Spacer(1, 0.15*inch)]))

    # Liquidity Ratios (10-Year Historical) - TTM first, then years descending
    liquidity_hist = balance_sheet.get('liquidity_ratios_historical', [])
    liquidity_ratios = balance_sheet.get('liquidity_ratios', {})
    if liquidity_hist:
        elements.append(Paragraph("Liquidity Ratios (10-Year History)", subheading_style))

        # Sort by year descending (most recent first)
        sorted_liq = sorted(liquidity_hist, key=lambda x: x.get('year', ''), reverse=True)

        # Build header row: Metric, TTM, then years descending
        liq_header = ['Metric', 'TTM'] + [h.get('year', '') for h in sorted_liq]

        # Build data rows
        liq_rows = [
            ['Current Ratio', fmt_ratio(liquidity_ratios.get('current_ratio'))] + [fmt_ratio(h.get('current_ratio')) for h in sorted_liq],
            ['Quick Ratio', fmt_ratio(liquidity_ratios.get('quick_ratio'))] + [fmt_ratio(h.get('quick_ratio')) for h in sorted_liq],
            ['Cash Ratio', fmt_ratio(liquidity_ratios.get('cash_ratio'))] + [fmt_ratio(h.get('cash_ratio')) for h in sorted_liq],
            ['DSO', f"{liquidity_ratios.get('days_sales_outstanding', 0):.0f}"] + [f"{h.get('days_sales_outstanding', 0):.0f}" for h in sorted_liq],
            ['DIO', f"{liquidity_ratios.get('days_inventory_outstanding', 0):.0f}"] + [f"{h.get('days_inventory_outstanding', 0):.0f}" for h in sorted_liq],
            ['DPO', f"{liquidity_ratios.get('days_payables_outstanding', 0):.0f}"] + [f"{h.get('days_payables_outstanding', 0):.0f}" for h in sorted_liq],
            ['CCC', f"{liquidity_ratios.get('cash_conversion_cycle', 0):.0f}"] + [f"{h.get('cash_conversion_cycle', 0):.0f}" for h in sorted_liq],
        ]

        liq_table_data = [liq_header] + liq_rows
        num_cols = len(liq_header)
        col_widths = [0.9*inch] + [0.55*inch] * (num_cols - 1)

        liq_table = Table(liq_table_data, colWidths=col_widths)
        liq_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(KeepTogether([liq_table, Spacer(1, 0.15*inch)]))

    # Credit Ratios (10-Year Historical) - TTM first, then years descending
    credit_hist = balance_sheet.get('credit_ratios_historical', [])
    credit_ratios = balance_sheet.get('credit_ratios', {})
    if credit_hist:
        elements.append(Paragraph("Credit Ratios (10-Year History)", subheading_style))

        # Sort by year descending (most recent first)
        sorted_credit = sorted(credit_hist, key=lambda x: x.get('year', ''), reverse=True)

        # Build header row: Metric, TTM, then years descending
        credit_header = ['Metric', 'TTM'] + [h.get('year', '') for h in sorted_credit]

        # Build data rows
        credit_rows = [
            ['Debt/Equity', fmt_ratio(credit_ratios.get('debt_to_equity'))] + [fmt_ratio(h.get('debt_to_equity')) for h in sorted_credit],
            ['Debt/Assets', fmt_ratio(credit_ratios.get('debt_to_assets'))] + [fmt_ratio(h.get('debt_to_assets')) for h in sorted_credit],
            ['LT Debt/Cap', fmt_ratio(credit_ratios.get('long_term_debt_to_capitalization'))] + [fmt_ratio(h.get('long_term_debt_to_capitalization')) for h in sorted_credit],
            ['Total Debt/Cap', fmt_ratio(credit_ratios.get('total_debt_to_capitalization'))] + [fmt_ratio(h.get('total_debt_to_capitalization')) for h in sorted_credit],
            ['Interest Cov', fmt_ratio(credit_ratios.get('interest_coverage'))] + [fmt_ratio(h.get('interest_coverage')) for h in sorted_credit],
            ['CF/Debt', fmt_ratio(credit_ratios.get('cash_flow_to_debt'))] + [fmt_ratio(h.get('cash_flow_to_debt')) for h in sorted_credit],
        ]

        credit_table_data = [credit_header] + credit_rows
        num_cols = len(credit_header)
        col_widths = [0.9*inch] + [0.55*inch] * (num_cols - 1)

        credit_table = Table(credit_table_data, colWidths=col_widths)
        credit_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(KeepTogether([credit_table, Spacer(1, 0.2*inch)]))

    # ============ SECTION 10: Technical Analysis ============
    elements.append(Paragraph(t("section_10"), heading_style))
    technical = report_data.get('technical_analysis', {})

    # Price Data
    price_data = technical.get('price_data', {})
    if price_data:
        elements.append(Paragraph("Price Summary", subheading_style))

        def fmt_price(val):
            if val is None or val == 0:
                return 'N/A'
            return f"${val:.2f}"

        def fmt_pct_tech(val):
            if val is None:
                return 'N/A'
            return f"{val:+.2f}%"

        price_summary = [
            ['Metric', 'Value'],
            ['Current Price', fmt_price(price_data.get('current_price'))],
            ['Day Change', fmt_pct_tech(price_data.get('change_percent'))],
            ['52-Week High', fmt_price(price_data.get('year_high'))],
            ['52-Week Low', fmt_price(price_data.get('year_low'))],
            ['% from 52W High', fmt_pct_tech(price_data.get('pct_from_52w_high'))],
            ['% from 52W Low', fmt_pct_tech(price_data.get('pct_from_52w_low'))],
        ]

        price_table = Table(price_summary, colWidths=[3*inch, 3*inch])
        price_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(price_table)
        elements.append(Spacer(1, 0.15*inch))

    # Moving Averages
    moving_avgs = technical.get('moving_averages', {})
    if moving_avgs:
        elements.append(Paragraph("Moving Averages", subheading_style))

        ma_data = [
            ['Indicator', 'Value', 'Price vs MA'],
            ['SMA 10', fmt_price(moving_avgs.get('sma_10')), fmt_pct_tech(moving_avgs.get('price_vs_sma_10'))],
            ['SMA 20', fmt_price(moving_avgs.get('sma_20')), fmt_pct_tech(moving_avgs.get('price_vs_sma_20'))],
            ['SMA 50', fmt_price(moving_avgs.get('sma_50')), fmt_pct_tech(moving_avgs.get('price_vs_sma_50'))],
            ['SMA 100', fmt_price(moving_avgs.get('sma_100')), 'N/A'],
            ['SMA 200', fmt_price(moving_avgs.get('sma_200')), fmt_pct_tech(moving_avgs.get('price_vs_sma_200'))],
        ]

        ma_table = Table(ma_data, colWidths=[2*inch, 2*inch, 2*inch])
        ma_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(ma_table)
        elements.append(Spacer(1, 0.15*inch))

    # Momentum Indicators
    momentum = technical.get('momentum_indicators', {})
    if momentum:
        elements.append(Paragraph("Momentum Indicators", subheading_style))

        rsi = momentum.get('rsi', {})
        macd = momentum.get('macd', {})
        stoch = momentum.get('stochastic', {})

        momentum_data = [
            ['Indicator', 'Value', 'Signal'],
            ['RSI (14)', f"{rsi.get('value', 'N/A')}", rsi.get('signal', 'N/A')],
            ['MACD Line', f"{macd.get('macd_line', 'N/A')}", macd.get('signal', 'N/A')],
            ['MACD Signal', f"{macd.get('signal_line', 'N/A')}", ''],
            ['MACD Histogram', f"{macd.get('histogram', 'N/A')}", ''],
            ['Stochastic %K', f"{stoch.get('k', 'N/A')}", stoch.get('signal', 'N/A')],
        ]

        momentum_table = Table(momentum_data, colWidths=[2*inch, 2*inch, 2*inch])
        momentum_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(momentum_table)
        elements.append(Spacer(1, 0.15*inch))

    # Volatility and Bollinger Bands
    volatility = technical.get('volatility_indicators', {})
    if volatility:
        elements.append(Paragraph("Volatility Indicators", subheading_style))

        atr = volatility.get('atr', {})
        bb = volatility.get('bollinger_bands', {})

        vol_data = [
            ['Indicator', 'Value'],
            ['ATR (14)', f"{atr.get('value', 'N/A')} ({atr.get('atr_percent', 'N/A')}%)"],
            ['Bollinger Upper', fmt_price(bb.get('upper'))],
            ['Bollinger Middle', fmt_price(bb.get('middle'))],
            ['Bollinger Lower', fmt_price(bb.get('lower'))],
            ['BB Width', f"{bb.get('width', 'N/A')}%"],
            ['BB %B', f"{bb.get('percent_b', 'N/A')}% ({bb.get('signal', 'N/A')})"],
        ]

        vol_table = Table(vol_data, colWidths=[3*inch, 3*inch])
        vol_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(vol_table)
        elements.append(Spacer(1, 0.15*inch))

    # Trend Analysis
    trend = technical.get('trend_analysis', {})
    support_resistance = technical.get('support_resistance', {})
    if trend or support_resistance:
        elements.append(Paragraph("Trend Analysis & Support/Resistance", subheading_style))

        # Determine signals based on trend data
        overall_trend = trend.get('overall_trend', 'N/A')
        if overall_trend and 'up' in overall_trend.lower():
            trend_signal = 'Bullish'
        elif overall_trend and 'down' in overall_trend.lower():
            trend_signal = 'Bearish'
        else:
            trend_signal = 'Neutral'

        golden_cross = trend.get('golden_cross')
        golden_cross_signal = 'Bullish' if golden_cross else 'Bearish'

        above_sma_50 = trend.get('above_sma_50')
        sma_50_signal = 'Bullish' if above_sma_50 else 'Bearish'

        above_sma_200 = trend.get('above_sma_200')
        sma_200_signal = 'Bullish' if above_sma_200 else 'Bearish'

        trend_data = [
            ['Metric', 'Value', 'Signal'],
            ['Overall Trend', overall_trend, trend_signal],
            ['Golden Cross (SMA50>200)', 'Yes' if golden_cross else 'No', golden_cross_signal],
            ['Above SMA 50', 'Yes' if above_sma_50 else 'No', sma_50_signal],
            ['Above SMA 200', 'Yes' if above_sma_200 else 'No', sma_200_signal],
            ['Pivot Point', fmt_price(support_resistance.get('pivot')), ''],
            ['Resistance 1', fmt_price(support_resistance.get('resistance_1')), ''],
            ['Support 1', fmt_price(support_resistance.get('support_1')), ''],
        ]

        trend_table = Table(trend_data, colWidths=[2.5*inch, 1.75*inch, 1.75*inch])
        trend_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(trend_table)

    elements.append(Spacer(1, 0.2*inch))

    # ============ SECTION 11: Management ============
    elements.append(Paragraph(t("section_11"), heading_style))
    management = report_data.get('management', [])

    # Key Executives Table - management can be a list or dict
    if isinstance(management, list):
        key_executives = management
    else:
        key_executives = management.get('key_executives', [])
    if key_executives:
        elements.append(Paragraph("Key Executives", subheading_style))
        exec_data = [['Name', 'Title', 'Pay']]
        for exec in key_executives[:10]:  # Limit to 10 executives
            pay = exec.get('pay')
            pay_str = f"${pay:,.0f}" if pay else 'N/A'
            exec_data.append([
                exec.get('name', 'N/A'),
                exec.get('title', 'N/A'),
                pay_str
            ])

        exec_table = Table(exec_data, colWidths=[2.2*inch, 2.6*inch, 1.2*inch])
        exec_table.setStyle(get_standard_table_style(has_row_headers=False))
        elements.append(KeepTogether([exec_table, Spacer(1, 0.15*inch)]))

    # Recent Changes - only if management is a dict
    if isinstance(management, dict):
        recent_changes = management.get('recent_changes', [])
        if recent_changes:
            elements.append(Paragraph("Recent Management Changes", subheading_style))
            for change in recent_changes[:5]:  # Limit to 5 changes
                change_text = f"• {change.get('date', 'N/A')}: {change.get('description', 'N/A')}"
                elements.append(Paragraph(change_text, body_style))
            elements.append(Spacer(1, 0.1*inch))

    # Insider Trading Summary - only if management is a dict
    insider_trading = management.get('insider_trading', {}) if isinstance(management, dict) else {}
    if insider_trading:
        elements.append(Paragraph("Insider Trading (Last 3 Months)", subheading_style))
        insider_data = [
            ['Activity', 'Count', 'Value'],
            ['Buys', str(insider_trading.get('buys_count', 0)), f"${insider_trading.get('buys_value', 0):,.0f}"],
            ['Sells', str(insider_trading.get('sells_count', 0)), f"${insider_trading.get('sells_value', 0):,.0f}"],
        ]

        insider_table = Table(insider_data, colWidths=[2*inch, 2*inch, 2*inch])
        insider_table.setStyle(get_standard_table_style(has_row_headers=True))
        elements.append(insider_table)

    # ============ SECTION 12: Prior Analysis Insights ============
    prior_analysis = report_data.get('prior_analysis', {})
    earnings_analysis = prior_analysis.get('earnings_analysis', '')
    annual_report_analysis = prior_analysis.get('annual_report_analysis', '')

    if earnings_analysis or annual_report_analysis:
        elements.append(Spacer(1, 0.2*inch))
        elements.append(Paragraph(t("section_12"), heading_style))
        elements.append(Paragraph(
            "Key findings synthesized from prior Earnings Call and Annual Report (10-K) analyses:",
            body_style
        ))
        elements.append(Spacer(1, 0.1*inch))

        # Get the synthesizer agent results if available
        agent_analysis = report_data.get('agent_analysis', {})
        synthesizer_results = agent_analysis.get('prior_analysis_synthesizer', {})

        if synthesizer_results and synthesizer_results.get('status') == 'success':
            elements.append(Paragraph("Synthesized Key Insights", subheading_style))
            synthesis_text = synthesizer_results.get('analysis', '')
            # Clean up markdown formatting for PDF
            synthesis_text = synthesis_text.replace('**', '').replace('##', '').replace('# ', '')
            for para in synthesis_text.split('\n\n'):
                if para.strip():
                    clean_para = para.strip().replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    try:
                        elements.append(Paragraph(clean_para, body_style))
                        elements.append(Spacer(1, 0.05*inch))
                    except:
                        pass
        else:
            # Fallback: Show summary of what prior analysis was used
            if earnings_analysis:
                elements.append(Paragraph("Earnings Call Analysis", subheading_style))
                elements.append(Paragraph(
                    f"Incorporated {len(earnings_analysis):,} characters of earnings call analysis covering management commentary, guidance, and segment performance.",
                    body_style
                ))
                elements.append(Spacer(1, 0.1*inch))

            if annual_report_analysis:
                elements.append(Paragraph("Annual Report (10-K) Analysis", subheading_style))
                elements.append(Paragraph(
                    f"Incorporated {len(annual_report_analysis):,} characters of 10-K analysis covering business model, risk factors, and financial performance.",
                    body_style
                ))

    # ============ SIGNATURE ============
    elements.append(Spacer(1, 0.4*inch))
    signature_style = ParagraphStyle(
        'Signature',
        parent=styles['Normal'],
        fontSize=10,
        leading=14,
        alignment=TA_CENTER,
    )
    elements.append(Paragraph("David A Quinn - 617-905-7415", signature_style))
    elements.append(Paragraph("Targeted Equity Consulting Group", signature_style))
    elements.append(Paragraph("daquinn@targetedequityconsulting.com", signature_style))

    # ============ FOOTNOTES / GLOSSARY ============
    elements.append(Spacer(1, 0.3*inch))
    elements.append(Paragraph("Glossary", heading_style))

    footnote_style = ParagraphStyle(
        'Footnote',
        parent=styles['Normal'],
        fontSize=8,
        leading=10,
        spaceBefore=2,
        spaceAfter=2,
    )

    glossary_items = [
        "<b>DSO (Days Sales Outstanding)</b> - Average days to collect payment from customers after a sale. Lower is better.",
        "<b>DIO (Days Inventory Outstanding)</b> - Average days inventory is held before being sold. Lower is better.",
        "<b>DPO (Days Payable Outstanding)</b> - Average days to pay suppliers. Higher preserves cash longer.",
        "<b>CCC (Cash Conversion Cycle)</b> - Days from paying suppliers to collecting from customers (DSO + DIO - DPO). Lower or negative is better.",
        "<b>ROIC (Return on Invested Capital)</b> - Measures how efficiently a company uses capital to generate profits.",
        "<b>WACC (Weighted Average Cost of Capital)</b> - The average rate a company pays to finance its assets.",
        "<b>EV/EBITDA</b> - Enterprise Value to Earnings Before Interest, Taxes, Depreciation & Amortization.",
        "<b>FCF (Free Cash Flow)</b> - Cash generated after capital expenditures, available for dividends, buybacks, or debt repayment.",
        "<b>TTM</b> - Trailing Twelve Months.",
    ]

    for item in glossary_items:
        elements.append(Paragraph(item, footnote_style))

    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    return buffer


@app.route('/')
def serve_dashboard():
    """Serve the main dashboard HTML"""
    return send_from_directory('.', 'stock_report_dashboard.html')


@app.route('/<path:filename>')
def serve_static(filename):
    """Serve static files like images"""
    if filename.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico')):
        return send_from_directory('.', filename)
    return jsonify({"error": "File not found"}), 404


@app.route('/api/report/<symbol>')
def get_company_report(symbol: str):
    """Main endpoint to get complete company report"""
    symbol = symbol.upper()

    try:
        # First gather base report data
        report = {
            "symbol": symbol,
            "generated_at": datetime.now().isoformat(),
            "business_overview": get_business_overview(symbol),
            "revenue_data": get_revenue_segments(symbol),
            "competitive_advantages": get_competitive_advantages(symbol),
            "recent_highlights": get_recent_highlights(symbol),
            "key_metrics": get_key_metrics_data(symbol),
            "valuations": get_valuations(symbol),
            "risks": get_risks(symbol),
            "management": get_management(symbol),
            "balance_sheet_metrics": get_balance_sheet_metrics(symbol),
            "technical_analysis": get_technical_analysis(symbol)
        }

        # Add AI-powered competitive analysis (moat analysis)
        report["competitive_analysis"] = get_competitive_analysis_ai(symbol)

        # Generate investment thesis using all collected data
        report["investment_thesis"] = get_investment_thesis(symbol, report)

        return jsonify(report)

    except APIError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "fmp_configured": bool(FMP_API_KEY),
        "fiscal_configured": bool(FISCAL_AI_API_KEY)
    })


@app.route('/api/report/<symbol>/pdf')
def download_pdf_report(symbol: str):
    """Generate and download PDF report"""
    symbol = symbol.upper()

    try:
        # Get the complete report data
        report = {
            "symbol": symbol,
            "generated_at": datetime.now().isoformat(),
            "business_overview": get_business_overview(symbol),
            "revenue_data": get_revenue_segments(symbol),
            "competitive_advantages": get_competitive_advantages(symbol),
            "recent_highlights": get_recent_highlights(symbol),
            "key_metrics": get_key_metrics_data(symbol),
            "valuations": get_valuations(symbol),
            "risks": get_risks(symbol),
            "management": get_management(symbol),
            "balance_sheet_metrics": get_balance_sheet_metrics(symbol),
            "technical_analysis": get_technical_analysis(symbol)
        }

        # Generate PDF
        pdf_buffer = generate_pdf_report(report)

        # Return PDF fileuseinclaude
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'{symbol}_Company_Report_{datetime.now().strftime("%Y%m%d")}.pdf'
        )

    except APIError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


if __name__ == '__main__':
    logger.info("Starting Company Report Backend Server...")
    logger.info(f"FMP API Key configured: {bool(FMP_API_KEY)}")
    logger.info(f"Fiscal.ai API Key configured: {bool(FISCAL_AI_API_KEY)}")

    port = int(os.getenv("PORT", 5001))
    logger.info(f"Server running at http://localhost:{port}")
    logger.info(f"Access dashboard at http://localhost:{port}")

    app.run(debug=False, host='0.0.0.0', port=port)
