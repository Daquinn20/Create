"""
Targeted Equity Consulting Group - Main Dashboard
Central portal for all financial analysis tools
"""
import streamlit as st
from pathlib import Path

# Page config
st.set_page_config(
    page_title="Targeted Equity Consulting Group",
    page_icon="🎯",
    layout="wide"
)

# Custom CSS - Black background, white text, turquoise buttons
st.markdown("""
    <style>
    /* Main app background */
    .stApp {
        background-color: #000000;
    }

    /* All text white */
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div {
        color: #ffffff !important;
    }

    .main-title {
        text-align: center;
        font-size: 2.5rem;
        font-weight: bold;
        color: #ffffff;
        margin-bottom: 0.5rem;
    }
    .subtitle {
        text-align: center;
        font-size: 1.2rem;
        color: #cccccc;
        margin-bottom: 2rem;
    }
    .tool-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        text-align: center;
        color: white;
        margin: 1rem 0;
        transition: transform 0.3s ease;
    }
    .tool-card:hover {
        transform: translateY(-5px);
    }
    .tool-title {
        font-size: 1.5rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .tool-desc {
        font-size: 0.9rem;
        opacity: 0.9;
    }

    /* Custom colored buttons */
    .custom-button {
        display: block;
        width: 100%;
        padding: 1rem 2rem;
        font-size: 1.1rem;
        font-weight: bold;
        border-radius: 10px;
        border: none;
        margin-top: 0.5rem;
        text-align: center;
        text-decoration: none;
        color: #ffffff !important;
        transition: opacity 0.3s ease;
    }

    .custom-button:hover {
        opacity: 0.85;
        color: #ffffff !important;
    }

    /* Divider color */
    hr {
        border-color: #333333 !important;
    }

    /* Expander styling */
    .streamlit-expanderHeader {
        color: #ffffff !important;
        background-color: #1a1a1a !important;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================
# CONFIGURATION - Add your app URLs here
# ============================================
APPS = {
    "daily_brief": {
        "name": "📋 Daily Brief",
        "description": "Market overview with AI summaries, newsletters, and Disruption Index news",
        "url": "https://create-yxbdgymdk3ilmurfejegca.streamlit.app/",
        "color": "#9467bd"
    },
    "innovation_stack": {
        "name": "💡 Innovation Stack",
        "description": "AI-powered article summaries from Stratechery, Not Boring, Paul Graham & more",
        "url": "https://create-hci2wwa73a3mmhumnldkwr.streamlit.app/",
        "color": "#ff7f0e"
    },
    "company_report": {
        "name": "📊 Company Report Generator",
        "description": "Generate comprehensive PDF reports with financial data, AI analysis, and professional formatting",
        "url": "https://create-production-68ca.up.railway.app",
        "color": "#1f77b4"
    },
    "earnings_analyzer": {
        "name": "📈 Earnings Transcript Analyzer",
        "description": "Analyze earnings call transcripts with Claude & ChatGPT AI for investment insights",
        "url": "https://create-49l4zpzbe6ytmphetcmkuu.streamlit.app/",
        "color": "#2ca02c"
    },
    "technical_screen": {
        "name": "📉 Technical Analysis Screen",
        "description": "Screen stocks using technical indicators across S&P 500, NASDAQ 100, and Disruption Index",
        "url": "https://create-technicalanalysis.streamlit.app/",
        "color": "#d62728"
    },
    "tecg_equity_model": {
        "name": "🎯 TECG Equity Model",
        "description": "Fundamental factor rankings with valuation, earnings quality, and growth scores",
        "url": "https://tecgmodel.streamlit.app/",
        "color": "#17becf"
    }
}
# ============================================

# Company Logo - Centered and larger
logo_path = Path(__file__).parent / "company_logo.png"

# Remove top padding to move logo up
st.markdown("<style>div.block-container{padding-top:1rem;}</style>", unsafe_allow_html=True)

if logo_path.exists():
    col1, col2, col3 = st.columns([1.5, 1, 1.5])
    with col2:
        st.image(str(logo_path), use_container_width=True)
else:
    st.markdown('<p class="main-title">🎯 Targeted Equity Consulting Group</p>', unsafe_allow_html=True)

st.markdown('<p class="subtitle">Financial Analysis & Investment Research Tools</p>', unsafe_allow_html=True)

st.divider()

# Display tool cards
cols = st.columns(len(APPS))

for i, (key, app) in enumerate(APPS.items()):
    with cols[i]:
        st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, {app['color']} 0%, {app['color']}99 100%);
                padding: 1.5rem;
                border-radius: 15px;
                text-align: center;
                color: white;
                min-height: 200px;
            ">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">{app['name'].split()[0]}</div>
                <div style="font-size: 1.2rem; font-weight: bold; margin-bottom: 0.5rem;">{' '.join(app['name'].split()[1:])}</div>
                <div style="font-size: 0.85rem; opacity: 0.9;">{app['description']}</div>
            </div>
            <a href="{app['url']}" target="_blank" class="custom-button" style="background-color: {app['color']};">
                Open {' '.join(app['name'].split()[1:])}
            </a>
        """, unsafe_allow_html=True)

st.divider()

# Footer
st.markdown("""
<div style="text-align: center; color: #666; padding: 2rem;">
    <p><strong>Targeted Equity Consulting Group</strong></p>
    <p>daquinn@targetedequityconsulting.com | 617-905-7415</p>
</div>
""", unsafe_allow_html=True)

# Instructions to add more apps
with st.expander("ℹ️ How to add more tools"):
    st.markdown("""
    To add a new tool to this dashboard, edit the `APPS` dictionary in `main_dashboard.py`:

    ```python
    "new_tool": {
        "name": "🔧 New Tool Name",
        "description": "Description of what the tool does",
        "url": "https://your-app-url.streamlit.app",
        "color": "#ff7f0e"
    }
    ```

    Then push to GitHub and redeploy.
    """)
