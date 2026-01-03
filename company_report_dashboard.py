"""
Company Report Dashboard - Streamlit Version
Imports all functions from company_report_backend.py to preserve exact functionality
"""
import streamlit as st
import os
import sys
from datetime import datetime
from typing import Dict, Any
import pandas as pd

# Import all functions from the Flask backend
# This preserves exact same metrics and PDF generation
from company_report_backend import (
    # API functions
    fmp_get,
    FMP_API_KEY,
    ANTHROPIC_API_KEY,
    OPENAI_API_KEY,
    # Data fetching functions
    get_business_overview,
    get_revenue_segments,
    get_competitive_advantages,
    get_key_metrics_data,
    get_valuations,
    get_risks,
    get_recent_highlights,
    get_competition,
    get_management,
    get_balance_sheet_metrics,
    get_technical_analysis,
    get_investment_thesis,
    get_competitive_analysis_ai,
    # PDF generation
    generate_pdf_report,
)

# Page configuration
st.set_page_config(
    page_title="Company Report Generator",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    .section-header {
        background-color: #2c2c2c;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)


def format_large_number(value):
    """Format large numbers with B/M suffix"""
    if value is None or value == 0:
        return "N/A"
    if abs(value) >= 1e9:
        return f"${value/1e9:.2f}B"
    elif abs(value) >= 1e6:
        return f"${value/1e6:.2f}M"
    else:
        return f"${value:,.0f}"


def format_percent(value, multiplier=1):
    """Format percentage values"""
    if value is None or value == 0:
        return "N/A"
    return f"{value * multiplier:.1f}%"


def display_company_details(overview: Dict[str, Any]):
    """Display Section 1: Company Details"""
    st.markdown("### 1. Company Details")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Current Price", f"${overview.get('price', 0):.2f}" if overview.get('price') else "N/A")
        st.metric("Market Cap", format_large_number(overview.get('market_cap', 0)))

    with col2:
        st.metric("52-Week High", f"${overview.get('week_52_high', 'N/A')}" if isinstance(overview.get('week_52_high'), (int, float)) else overview.get('week_52_high', 'N/A'))
        st.metric("52-Week Low", f"${overview.get('week_52_low', 'N/A')}" if isinstance(overview.get('week_52_low'), (int, float)) else overview.get('week_52_low', 'N/A'))

    with col3:
        st.metric("Industry", overview.get('industry', 'N/A'))
        st.metric("Sector", overview.get('sector', 'N/A'))

    with col4:
        employees = overview.get('employees', 'N/A')
        st.metric("Employees", f"{employees:,}" if isinstance(employees, int) else employees)
        st.metric("Headquarters", overview.get('headquarters', 'N/A'))


def display_business_overview(overview: Dict[str, Any]):
    """Display Section 2: Business Overview"""
    st.markdown("### 2. Business Overview")

    description = overview.get('description', 'No description available')
    st.markdown(description)


def display_revenue_segments(revenue_data: Dict[str, Any]):
    """Display Section 3: Revenue by Segment"""
    st.markdown("### 3. Revenue by Segment")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Margins**")
        margins = revenue_data.get('margins', {})
        if margins:
            margin_df = pd.DataFrame([
                {"Metric": "Gross Margin", "Value": f"{margins.get('gross_margin', 0):.2f}%"},
                {"Metric": "Operating Margin", "Value": f"{margins.get('operating_margin', 0):.2f}%"},
                {"Metric": "Net Margin", "Value": f"{margins.get('net_margin', 0):.2f}%"},
            ])
            st.dataframe(margin_df, use_container_width=True, hide_index=True)

    with col2:
        st.markdown("**Segments**")
        segments = revenue_data.get('segments', [])
        if segments:
            for segment in segments[:5]:
                name = segment.get('name', 'N/A')
                revenue = segment.get('revenue', 0)
                if revenue and revenue > 0:
                    st.write(f"**{name}:** {format_large_number(revenue)}")

        # Show AI analysis if available
        if segments and segments[0].get('ai_analysis'):
            with st.expander("AI Segment Analysis"):
                st.markdown(segments[0]['ai_analysis'])


def display_recent_highlights(highlights: list):
    """Display Section 4: Highlights from Recent Quarters"""
    st.markdown("### 4. Highlights from Recent Quarters")

    # Show AI summary if available
    if highlights and highlights[0].get('ai_summary'):
        with st.expander("AI Quarterly Trends Analysis", expanded=True):
            st.markdown(highlights[0]['ai_summary'])

    for highlight in highlights[:4]:
        quarter = highlight.get('quarter', 'N/A')
        details = highlight.get('details', [])

        with st.expander(f"**{quarter}**"):
            for detail in details:
                st.write(f"- {detail}")


def display_competitive_advantages(advantages: list):
    """Display Section 5: Competitive Advantages"""
    st.markdown("### 5. Competitive Advantages")

    for i, advantage in enumerate(advantages[:6], 1):
        st.write(f"{i}. {advantage}")


def display_key_metrics(metrics: Dict[str, Any]):
    """Display Section 6: Key Metrics"""
    st.markdown("### 6. Key Metrics")

    # Revenue Growth & Margins Table
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Growth & Margins**")

        def fmt_pct(val):
            if val is None or (isinstance(val, (int, float)) and val == 0):
                return 'N/A'
            return f"{val:.1f}%"

        growth_data = pd.DataFrame([
            {
                "Metric": "Revenue Growth",
                "5 Year Avg": fmt_pct(metrics.get('revenue_growth_5yr')),
                "3 Yr Avg": fmt_pct(metrics.get('revenue_growth_3yr')),
                "TTM": fmt_pct(metrics.get('revenue_growth_ttm')),
                "Est 1 Yr": fmt_pct(metrics.get('revenue_growth_est_1yr')),
                "Est 2 Yr": fmt_pct(metrics.get('revenue_growth_est_2yr')),
            },
            {
                "Metric": "Gross Margin",
                "5 Year Avg": fmt_pct(metrics.get('gross_margin_5yr')),
                "3 Yr Avg": fmt_pct(metrics.get('gross_margin_3yr')),
                "TTM": fmt_pct(metrics.get('gross_margin', 0) * 100),
                "Est 1 Yr": fmt_pct(metrics.get('gross_margin_est_1yr')),
                "Est 2 Yr": fmt_pct(metrics.get('gross_margin_est_2yr')),
            },
            {
                "Metric": "Operating Margin",
                "5 Year Avg": fmt_pct(metrics.get('operating_margin_5yr')),
                "3 Yr Avg": fmt_pct(metrics.get('operating_margin_3yr')),
                "TTM": fmt_pct(metrics.get('operating_margin', 0) * 100),
                "Est 1 Yr": fmt_pct(metrics.get('operating_margin_est_1yr')),
                "Est 2 Yr": fmt_pct(metrics.get('operating_margin_est_2yr')),
            },
            {
                "Metric": "Net Income Margin",
                "5 Year Avg": fmt_pct(metrics.get('net_income_margin_5yr')),
                "3 Yr Avg": fmt_pct(metrics.get('net_income_margin_3yr')),
                "TTM": fmt_pct(metrics.get('net_margin', 0) * 100),
                "Est 1 Yr": fmt_pct(metrics.get('net_income_margin_est_1yr')),
                "Est 2 Yr": fmt_pct(metrics.get('net_income_margin_est_2yr')),
            },
        ])
        st.dataframe(growth_data, use_container_width=True, hide_index=True)

    with col2:
        st.markdown("**Return Metrics**")

        returns_data = pd.DataFrame([
            {
                "Metric": "ROIC",
                "5 Year Avg": fmt_pct(metrics.get('roic_5yr')),
                "3 Yr Avg": fmt_pct(metrics.get('roic_3yr')),
                "TTM": fmt_pct(metrics.get('roic')),
            },
            {
                "Metric": "ROE",
                "5 Year Avg": fmt_pct(metrics.get('roe_5yr')),
                "3 Yr Avg": fmt_pct(metrics.get('roe_3yr')),
                "TTM": fmt_pct(metrics.get('roe')),
            },
            {
                "Metric": "ROA",
                "5 Year Avg": fmt_pct(metrics.get('roa_5yr')),
                "3 Yr Avg": fmt_pct(metrics.get('roa_3yr')),
                "TTM": fmt_pct(metrics.get('roa')),
            },
            {
                "Metric": "WACC",
                "5 Year Avg": "-",
                "3 Yr Avg": "-",
                "TTM": fmt_pct(metrics.get('wacc')),
            },
        ])
        st.dataframe(returns_data, use_container_width=True, hide_index=True)


def display_valuations(valuations: Dict[str, Any]):
    """Display Section 7: Valuations"""
    st.markdown("### 7. Valuations")

    current_val = valuations.get('current', valuations)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("P/E Ratio", f"{current_val.get('pe_ratio', 0):.2f}" if current_val.get('pe_ratio') else 'N/A')
        st.metric("Price/Sales", f"{current_val.get('price_to_sales', 0):.2f}" if current_val.get('price_to_sales') else 'N/A')

    with col2:
        st.metric("Price/Book", f"{current_val.get('price_to_book', 0):.2f}" if current_val.get('price_to_book') else 'N/A')
        st.metric("EV/EBITDA", f"{current_val.get('ev_to_ebitda', 0):.2f}" if current_val.get('ev_to_ebitda') else 'N/A')

    with col3:
        st.metric("PEG Ratio", f"{current_val.get('peg_ratio', 0):.2f}" if current_val.get('peg_ratio') else 'N/A')
        st.metric("Price/FCF", f"{current_val.get('price_to_fcf', 0):.2f}" if current_val.get('price_to_fcf') else 'N/A')

    # Historical valuations
    historical = valuations.get('historical', [])
    if historical:
        with st.expander("Historical Valuations (10 Years)"):
            hist_df = pd.DataFrame(historical)
            st.dataframe(hist_df, use_container_width=True, hide_index=True)


def display_balance_sheet(balance_sheet: Dict[str, Any]):
    """Display Section 8: Balance Sheet / Credit Metrics"""
    st.markdown("### 8. Balance Sheet / Credit Metrics")

    current = balance_sheet.get('current', {})

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Balance Sheet Summary**")
        if current:
            bs_data = pd.DataFrame([
                {"Item": "Total Assets", "Value": format_large_number(current.get('total_assets'))},
                {"Item": "Total Liabilities", "Value": format_large_number(current.get('total_liabilities'))},
                {"Item": "Total Equity", "Value": format_large_number(current.get('total_equity'))},
                {"Item": "Cash & Equivalents", "Value": format_large_number(current.get('cash_and_equivalents'))},
                {"Item": "Total Debt", "Value": format_large_number(current.get('total_debt'))},
                {"Item": "Net Debt", "Value": format_large_number(current.get('net_debt'))},
                {"Item": "Working Capital", "Value": format_large_number(current.get('working_capital'))},
            ])
            st.dataframe(bs_data, use_container_width=True, hide_index=True)

    with col2:
        st.markdown("**Liquidity Ratios (TTM)**")
        liquidity = balance_sheet.get('liquidity_ratios', {})
        if liquidity:
            liq_data = pd.DataFrame([
                {"Ratio": "Current Ratio", "Value": f"{liquidity.get('current_ratio', 0):.2f}"},
                {"Ratio": "Quick Ratio", "Value": f"{liquidity.get('quick_ratio', 0):.2f}"},
                {"Ratio": "Cash Ratio", "Value": f"{liquidity.get('cash_ratio', 0):.2f}"},
                {"Ratio": "DSO", "Value": f"{liquidity.get('days_sales_outstanding', 0):.0f} days"},
                {"Ratio": "DIO", "Value": f"{liquidity.get('days_inventory_outstanding', 0):.0f} days"},
                {"Ratio": "Cash Conversion Cycle", "Value": f"{liquidity.get('cash_conversion_cycle', 0):.0f} days"},
            ])
            st.dataframe(liq_data, use_container_width=True, hide_index=True)

    # Historical ratios
    liq_hist = balance_sheet.get('liquidity_ratios_historical', [])
    credit_hist = balance_sheet.get('credit_ratios_historical', [])

    if liq_hist:
        with st.expander("Liquidity Ratios (10-Year History)"):
            st.dataframe(pd.DataFrame(liq_hist), use_container_width=True, hide_index=True)

    if credit_hist:
        with st.expander("Credit Ratios (10-Year History)"):
            st.dataframe(pd.DataFrame(credit_hist), use_container_width=True, hide_index=True)


def display_technical_analysis(technical: Dict[str, Any]):
    """Display Section 9: Technical Analysis"""
    st.markdown("### 9. Technical Analysis")

    col1, col2, col3 = st.columns(3)

    # Price Data
    price_data = technical.get('price_data', {})
    with col1:
        st.markdown("**Price Summary**")
        if price_data:
            st.metric("Current Price", f"${price_data.get('current_price', 0):.2f}")
            st.metric("Day Change", f"{price_data.get('change_percent', 0):+.2f}%")
            st.metric("52W High", f"${price_data.get('year_high', 0):.2f}")
            st.metric("52W Low", f"${price_data.get('year_low', 0):.2f}")
            st.metric("% from 52W High", f"{price_data.get('pct_from_52w_high', 0):+.1f}%")

    # Moving Averages
    moving_avgs = technical.get('moving_averages', {})
    with col2:
        st.markdown("**Moving Averages**")
        if moving_avgs:
            ma_data = pd.DataFrame([
                {"MA": "SMA 10", "Value": f"${moving_avgs.get('sma_10', 0):.2f}", "vs Price": f"{moving_avgs.get('price_vs_sma_10', 0):+.2f}%"},
                {"MA": "SMA 20", "Value": f"${moving_avgs.get('sma_20', 0):.2f}", "vs Price": f"{moving_avgs.get('price_vs_sma_20', 0):+.2f}%"},
                {"MA": "SMA 50", "Value": f"${moving_avgs.get('sma_50', 0):.2f}", "vs Price": f"{moving_avgs.get('price_vs_sma_50', 0):+.2f}%"},
                {"MA": "SMA 100", "Value": f"${moving_avgs.get('sma_100', 0):.2f}", "vs Price": "N/A"},
                {"MA": "SMA 200", "Value": f"${moving_avgs.get('sma_200', 0):.2f}", "vs Price": f"{moving_avgs.get('price_vs_sma_200', 0):+.2f}%"},
            ])
            st.dataframe(ma_data, use_container_width=True, hide_index=True)

    # Momentum & Trend
    momentum = technical.get('momentum_indicators', {})
    trend = technical.get('trend_analysis', {})

    with col3:
        st.markdown("**Momentum & Trend**")

        rsi = momentum.get('rsi', {})
        if rsi:
            st.metric("RSI (14)", f"{rsi.get('value', 50):.1f} - {rsi.get('signal', 'Neutral')}")

        macd = momentum.get('macd', {})
        if macd:
            st.metric("MACD", f"{macd.get('macd_line', 0):.4f} ({macd.get('signal', 'N/A')})")

        stoch = momentum.get('stochastic', {})
        if stoch:
            st.metric("Stochastic %K", f"{stoch.get('k', 50):.1f} ({stoch.get('signal', 'Neutral')})")

        if trend:
            st.metric("Overall Trend", trend.get('overall_trend', 'N/A'))
            st.metric("Golden Cross", "Yes" if trend.get('golden_cross') else "No")

    # Volatility and Support/Resistance
    volatility = technical.get('volatility_indicators', {})
    support_resistance = technical.get('support_resistance', {})

    with st.expander("Volatility & Support/Resistance"):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Volatility Indicators**")
            atr = volatility.get('atr', {})
            bb = volatility.get('bollinger_bands', {})
            if atr:
                st.write(f"ATR (14): {atr.get('value', 0):.2f} ({atr.get('atr_percent', 0):.2f}%)")
            if bb:
                st.write(f"Bollinger Upper: ${bb.get('upper', 0):.2f}")
                st.write(f"Bollinger Middle: ${bb.get('middle', 0):.2f}")
                st.write(f"Bollinger Lower: ${bb.get('lower', 0):.2f}")
                st.write(f"BB Width: {bb.get('width', 0):.2f}%")
                st.write(f"BB %B: {bb.get('percent_b', 0):.2f}% ({bb.get('signal', 'N/A')})")

        with col2:
            st.markdown("**Support/Resistance**")
            if support_resistance:
                st.write(f"Pivot Point: ${support_resistance.get('pivot', 0):.2f}")
                st.write(f"Resistance 1: ${support_resistance.get('resistance_1', 0):.2f}")
                st.write(f"Resistance 2: ${support_resistance.get('resistance_2', 0):.2f}")
                st.write(f"Support 1: ${support_resistance.get('support_1', 0):.2f}")
                st.write(f"Support 2: ${support_resistance.get('support_2', 0):.2f}")


def display_risks(risks: Dict[str, Any]):
    """Display Section 10: Risks and Red Flags"""
    st.markdown("### 10. Risks and Red Flags")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**A) Company Red Flags**")
        company_specific = risks.get('company_specific', [])
        for i, risk in enumerate(company_specific[:8], 1):
            st.write(f"{i}. {risk}")

    with col2:
        st.markdown("**B) General Risks**")
        general = risks.get('general', [])
        for i, risk in enumerate(general[:8], 1):
            st.write(f"{i}. {risk}")


def display_management(management: Dict[str, Any]):
    """Display Section 11: Management"""
    st.markdown("### 11. Management")

    # If management is a list (old format), convert
    if isinstance(management, list):
        key_executives = management
        insider_trading = {}
    else:
        key_executives = management.get('key_executives', management) if isinstance(management, dict) else management
        insider_trading = management.get('insider_trading', {}) if isinstance(management, dict) else {}

    # Key Executives
    if key_executives:
        st.markdown("**Key Executives**")
        exec_data = []
        for exec in key_executives[:10]:
            if isinstance(exec, dict):
                pay = exec.get('pay')
                pay_str = f"${pay:,.0f}" if pay else 'N/A'
                exec_data.append({
                    "Name": exec.get('name', 'N/A'),
                    "Title": exec.get('title', 'N/A'),
                    "Pay": pay_str,
                    "Stock Held": exec.get('stock_held', 'N/A'),
                    "Prior Employers": exec.get('prior_employers', 'N/A')
                })

        if exec_data:
            st.dataframe(pd.DataFrame(exec_data), use_container_width=True, hide_index=True)

    # Insider Trading
    if insider_trading:
        st.markdown("**Insider Trading (Last 3 Months)**")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Buys", f"{insider_trading.get('buys_count', 0)} (${insider_trading.get('buys_value', 0):,.0f})")
        with col2:
            st.metric("Sells", f"{insider_trading.get('sells_count', 0)} (${insider_trading.get('sells_value', 0):,.0f})")


def display_competitive_analysis(comp_analysis: Dict[str, Any]):
    """Display Competitive Analysis (AI-powered)"""
    st.markdown("### Competitive Analysis")

    if comp_analysis.get('moat_analysis'):
        with st.expander("Moat Analysis", expanded=True):
            st.markdown(comp_analysis['moat_analysis'])

    if comp_analysis.get('competitive_position'):
        with st.expander("Competitive Position"):
            st.markdown(comp_analysis['competitive_position'])

    if comp_analysis.get('market_dynamics'):
        with st.expander("Market Dynamics"):
            st.markdown(comp_analysis['market_dynamics'])

    if comp_analysis.get('competitive_advantages'):
        with st.expander("Key Competitive Advantages"):
            for adv in comp_analysis['competitive_advantages']:
                st.write(f"- {adv}")

    if comp_analysis.get('industry_analysis'):
        with st.expander("Industry-Specific Analysis"):
            st.markdown(comp_analysis['industry_analysis'])


def display_investment_thesis(thesis: Dict[str, Any]):
    """Display Investment Thesis"""
    st.markdown("### Investment Thesis")

    if thesis.get('summary'):
        st.markdown("**Executive Summary**")
        st.markdown(thesis['summary'])

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Bull Case**")
        for point in thesis.get('bull_case', []):
            st.write(f"- {point}")

    with col2:
        st.markdown("**Bear Case**")
        for point in thesis.get('bear_case', []):
            st.write(f"- {point}")

    if thesis.get('key_metrics_to_watch'):
        with st.expander("Key Metrics to Watch"):
            for metric in thesis['key_metrics_to_watch']:
                st.write(f"- {metric}")

    if thesis.get('catalysts'):
        with st.expander("Upcoming Catalysts"):
            for catalyst in thesis['catalysts']:
                st.write(f"- {catalyst}")


def main():
    st.sidebar.title("Company Report Generator")

    # Check API keys
    if not FMP_API_KEY:
        st.sidebar.error("FMP API key not configured")
        st.error("Please configure your FMP_API_KEY in .env file or Streamlit secrets")
        return

    st.sidebar.success("FMP API: Configured")
    st.sidebar.info(f"Anthropic API: {'Configured' if ANTHROPIC_API_KEY else 'Not configured'}")
    st.sidebar.info(f"OpenAI API: {'Configured' if OPENAI_API_KEY else 'Not configured'}")

    # Ticker input
    symbol = st.sidebar.text_input("Enter Ticker Symbol", value="AAPL", max_chars=10).upper().strip()

    generate_button = st.sidebar.button("Generate Report", type="primary", use_container_width=True)

    if generate_button and symbol:
        with st.spinner(f"Generating comprehensive report for {symbol}..."):
            progress_bar = st.progress(0)
            status_text = st.empty()

            try:
                # Fetch all data using original backend functions
                status_text.text("Fetching business overview...")
                progress_bar.progress(10)
                business_overview = get_business_overview(symbol)

                status_text.text("Fetching revenue segments...")
                progress_bar.progress(20)
                revenue_data = get_revenue_segments(symbol)

                status_text.text("Analyzing competitive advantages...")
                progress_bar.progress(25)
                competitive_advantages = get_competitive_advantages(symbol)

                status_text.text("Fetching recent highlights...")
                progress_bar.progress(30)
                recent_highlights = get_recent_highlights(symbol)

                status_text.text("Fetching key metrics...")
                progress_bar.progress(40)
                key_metrics = get_key_metrics_data(symbol)

                status_text.text("Fetching valuations...")
                progress_bar.progress(50)
                valuations = get_valuations(symbol)

                status_text.text("Fetching balance sheet metrics...")
                progress_bar.progress(55)
                balance_sheet = get_balance_sheet_metrics(symbol)

                status_text.text("Fetching technical analysis...")
                progress_bar.progress(60)
                technical = get_technical_analysis(symbol)

                status_text.text("Analyzing risks...")
                progress_bar.progress(70)
                risks = get_risks(symbol)

                status_text.text("Fetching management info...")
                progress_bar.progress(75)
                management = get_management(symbol)

                status_text.text("Analyzing competitive position...")
                progress_bar.progress(85)
                competitive_analysis = get_competitive_analysis_ai(symbol)

                # Build report data
                report_data = {
                    "symbol": symbol,
                    "generated_at": datetime.now().isoformat(),
                    "business_overview": business_overview,
                    "revenue_data": revenue_data,
                    "competitive_advantages": competitive_advantages,
                    "recent_highlights": recent_highlights,
                    "key_metrics": key_metrics,
                    "valuations": valuations,
                    "balance_sheet_metrics": balance_sheet,
                    "technical_analysis": technical,
                    "risks": risks,
                    "management": management,
                    "competitive_analysis": competitive_analysis,
                }

                status_text.text("Generating investment thesis...")
                progress_bar.progress(95)
                report_data["investment_thesis"] = get_investment_thesis(symbol, report_data)

                progress_bar.progress(100)
                status_text.empty()
                progress_bar.empty()

                # Store in session state
                st.session_state.report_data = report_data

            except Exception as e:
                st.error(f"Error generating report: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
                return

    # Display report if available
    if "report_data" in st.session_state:
        report_data = st.session_state.report_data
        overview = report_data.get("business_overview", {})

        if "error" in overview:
            st.error(overview["error"])
            return

        # Header
        col1, col2 = st.columns([3, 1])
        with col1:
            st.title(f"{overview.get('company_name', 'N/A')} ({report_data['symbol']})")
            st.caption(f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")

        with col2:
            # PDF Download button
            try:
                pdf_buffer = generate_pdf_report(report_data)
                st.download_button(
                    label="Download PDF Report",
                    data=pdf_buffer,
                    file_name=f"{report_data['symbol']}_Company_Report_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    type="primary"
                )
            except Exception as e:
                st.error(f"Error generating PDF: {e}")

        st.divider()

        # Display all sections
        display_company_details(overview)
        st.divider()

        display_business_overview(overview)
        st.divider()

        display_revenue_segments(report_data.get("revenue_data", {}))
        st.divider()

        display_recent_highlights(report_data.get("recent_highlights", []))
        st.divider()

        display_competitive_advantages(report_data.get("competitive_advantages", []))
        st.divider()

        display_key_metrics(report_data.get("key_metrics", {}))
        st.divider()

        display_valuations(report_data.get("valuations", {}))
        st.divider()

        display_balance_sheet(report_data.get("balance_sheet_metrics", {}))
        st.divider()

        display_technical_analysis(report_data.get("technical_analysis", {}))
        st.divider()

        display_risks(report_data.get("risks", {}))
        st.divider()

        display_management(report_data.get("management", {}))
        st.divider()

        display_competitive_analysis(report_data.get("competitive_analysis", {}))
        st.divider()

        display_investment_thesis(report_data.get("investment_thesis", {}))

    else:
        st.info("Enter a ticker symbol and click 'Generate Report' to get started.")

        st.markdown("""
        ### Features (Same as Original)
        - **Section 1**: Company Details (price, market cap, 52W range)
        - **Section 2**: Business Overview (AI-enhanced from annual reports)
        - **Section 3**: Revenue by Segment with margins
        - **Section 4**: Highlights from Recent Quarters (AI analysis)
        - **Section 5**: Competitive Advantages
        - **Section 6**: Key Metrics (5yr, 3yr, TTM, estimates)
        - **Section 7**: Valuations with 10-year history
        - **Section 8**: Balance Sheet / Credit Metrics (10-year history)
        - **Section 9**: Technical Analysis (SMAs, RSI, MACD, Bollinger, etc.)
        - **Section 10**: Risks and Red Flags (AI-powered)
        - **Section 11**: Management with insider trading
        - **Competitive Analysis**: AI-powered moat and market analysis
        - **Investment Thesis**: AI-generated bull/bear cases
        - **PDF Export**: Same format as original
        """)


if __name__ == "__main__":
    main()
