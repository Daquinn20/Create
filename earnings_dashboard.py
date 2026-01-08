"""
Earnings Revision Dashboard
Interactive Streamlit dashboard for viewing S&P 500 earnings revision rankings
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from earnings_revision_ranker import EarningsRevisionRanker

# Page config
st.set_page_config(
    page_title="Earnings Revision Ranker",
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
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
    """, unsafe_allow_html=True)


@st.cache_data(ttl=3600)
def load_data(num_stocks, max_workers=10, sectors=None, sp500_file='SP500_list.xlsx'):
    """Load and cache earnings revision data for S&P 500"""
    ranker = EarningsRevisionRanker(max_workers=max_workers)
    df = ranker.scan_sp500(
        sp500_file=sp500_file,
        max_stocks=num_stocks,
        parallel=True,
        sectors=sectors
    )
    return df


@st.cache_data(ttl=3600)
def load_disruption_data(max_workers=10):
    """Load and cache earnings revision data for Disruption Index"""
    ranker = EarningsRevisionRanker(max_workers=max_workers)
    df = ranker.scan_disruption_index(parallel=True)
    return df


@st.cache_data(ttl=3600)
def load_broad_us_data(num_stocks=None, max_workers=10, sectors=None):
    """Load and cache earnings revision data for Broad US Index"""
    ranker = EarningsRevisionRanker(max_workers=max_workers)
    df = ranker.scan_broad_us_index(
        index_file='Index_Broad_US.xlsx',
        max_stocks=num_stocks,
        parallel=True,
        sectors=sectors
    )
    return df


def get_broad_us_sectors(index_file='Index_Broad_US.xlsx'):
    """Get list of available sectors from Broad US Index file"""
    try:
        df = pd.read_excel(index_file)
        if 'Sector' in df.columns:
            sectors = sorted(df['Sector'].dropna().unique().tolist())
            return sectors
    except:
        pass
    return []


def get_estimates_tracker_status():
    """Get status of the estimates tracking database"""
    import sqlite3
    # Use absolute path based on script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "estimates_history.db")

    if not os.path.exists(db_path):
        return None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get snapshot dates
        cursor.execute("SELECT DISTINCT snapshot_date FROM estimate_snapshots ORDER BY snapshot_date DESC")
        dates = [row[0] for row in cursor.fetchall()]

        # Get ticker count
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM estimate_snapshots")
        ticker_count = cursor.fetchone()[0]

        conn.close()

        return {
            'dates': dates,
            'ticker_count': ticker_count,
            'days_of_data': len(dates),
            'latest_date': dates[0] if dates else None,
            'oldest_date': dates[-1] if dates else None
        }
    except Exception as e:
        return {'error': str(e)}


def get_revision_data(ticker: str, days: int = 30):
    """Get revision data for a specific ticker"""
    import sqlite3
    from datetime import datetime, timedelta

    # Use absolute path based on script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "estimates_history.db")
    if not os.path.exists(db_path):
        return None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get all snapshots for this ticker
        cursor.execute("""
            SELECT snapshot_date, fiscal_period, eps_avg, revenue_avg
            FROM estimate_snapshots
            WHERE ticker = ?
            ORDER BY snapshot_date DESC, fiscal_period ASC
        """, (ticker.upper(),))

        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return None

        df = pd.DataFrame(rows, columns=['Date', 'Fiscal Period', 'EPS Estimate', 'Revenue Estimate'])
        return df
    except Exception as e:
        return None


def get_fy_estimates(ticker: str):
    """Fetch FY1, FY2, FY3 earnings estimates from FMP API"""
    import requests
    from dotenv import load_dotenv
    load_dotenv()

    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        return {'error': 'FMP_API_KEY not found'}

    try:
        url = f"https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker.upper()}?apikey={api_key}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data:
            return {'error': f'No estimates found for {ticker}'}

        # Sort by date to get chronological order (nearest fiscal year first)
        from datetime import datetime
        current_year = datetime.now().year

        # Filter for future fiscal years and sort
        future_estimates = []
        for est in data:
            try:
                est_date = datetime.strptime(est.get('date', ''), '%Y-%m-%d')
                if est_date.year >= current_year:
                    future_estimates.append(est)
            except:
                continue

        # Sort by date ascending (FY1 = nearest)
        future_estimates.sort(key=lambda x: x.get('date', ''))

        # Take first 3 as FY1, FY2, FY3
        result = {
            'ticker': ticker.upper(),
            'estimates': []
        }

        for i, est in enumerate(future_estimates[:3]):
            fy_label = f"FY{i+1}"
            result['estimates'].append({
                'fiscal_year': fy_label,
                'fiscal_end': est.get('date'),
                'eps_avg': est.get('estimatedEpsAvg'),
                'eps_high': est.get('estimatedEpsHigh'),
                'eps_low': est.get('estimatedEpsLow'),
                'revenue_avg': est.get('estimatedRevenueAvg'),
                'revenue_high': est.get('estimatedRevenueHigh'),
                'revenue_low': est.get('estimatedRevenueLow'),
                'num_analysts_eps': est.get('numberAnalystsEstimatedEps'),
                'num_analysts_rev': est.get('numberAnalystsEstimatedRevenue')
            })

        return result
    except requests.exceptions.RequestException as e:
        return {'error': f'API error: {str(e)}'}
    except Exception as e:
        return {'error': f'Error: {str(e)}'}


def get_available_sectors(sp500_file='SP500_list.xlsx'):
    """Get list of available sectors from SP500 file"""
    try:
        df = pd.read_excel(sp500_file)
        if 'Sector' in df.columns:
            sectors = sorted(df['Sector'].dropna().unique().tolist())
            return sectors
    except:
        pass
    return []


def create_score_distribution(df):
    """Create histogram of revision scores"""
    fig = px.histogram(
        df,
        x='revision_strength_score',
        nbins=30,
        title='Distribution of Revision Strength Scores',
        labels={'revision_strength_score': 'Revision Strength Score'},
        color_discrete_sequence=['#1f77b4']
    )
    fig.update_layout(
        xaxis_title="Revision Strength Score",
        yaxis_title="Number of Stocks",
        showlegend=False
    )
    return fig


def create_top_stocks_chart(df, n=20):
    """Create bar chart of top stocks"""
    top_n = df.head(n).copy()
    top_n = top_n.sort_values('revision_strength_score', ascending=True)

    fig = go.Figure(go.Bar(
        x=top_n['revision_strength_score'],
        y=top_n['ticker'],
        orientation='h',
        marker=dict(
            color=top_n['revision_strength_score'],
            colorscale='RdYlGn',
            showscale=True,
            colorbar=dict(title="Score")
        ),
        text=top_n['revision_strength_score'].round(2),
        textposition='outside'
    ))

    fig.update_layout(
        title=f'Top {n} Stocks by Revision Strength',
        xaxis_title='Revision Strength Score',
        yaxis_title='Ticker',
        height=600,
        showlegend=False
    )

    return fig


def create_scatter_plot(df):
    """Create scatter plot of EPS revision vs Rating change"""
    fig = px.scatter(
        df,
        x='eps_revision_pct',
        y='net_rating_change',
        size='revision_strength_score',
        color='revision_strength_score',
        hover_name='ticker',
        hover_data=['eps_revision_pct', 'revenue_revision_pct', 'analyst_count_change'],
        title='EPS Revisions vs Analyst Rating Changes',
        labels={
            'eps_revision_pct': 'EPS Revision %',
            'net_rating_change': 'Net Rating Change (Upgrades - Downgrades)',
            'revision_strength_score': 'Revision Score'
        },
        color_continuous_scale='RdYlGn'
    )

    fig.update_layout(
        xaxis_title='EPS Revision %',
        yaxis_title='Net Rating Change',
        height=500
    )

    return fig


def main():
    # Header with company logo
    import base64

    # Load and encode logo
    try:
        with open("company_logo.png", "rb") as f:
            logo_data = base64.b64encode(f.read()).decode()

        st.markdown(f"""
            <style>
            .company-header {{
                display: flex;
                align-items: center;
                justify-content: center;
                margin-bottom: 10px;
            }}
            .company-logo {{
                height: 280px;
                width: auto;
                max-width: 90%;
                object-fit: contain;
            }}
            </style>
            <div style='text-align: center; margin-top: -50px; margin-bottom: 5px;'>
                <div class='company-header'>
                    <img src='data:image/png;base64,{logo_data}' class='company-logo' alt='Company Logo'>
                </div>
                <p style='font-size: 18px; color: #666; margin-top: 0px; margin-bottom: 0px; font-style: italic;'>Precision Analysis for Informed Investment Decisions</p>
            </div>
        """, unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning("⚠️ Logo file 'company_logo.png' not found")
        st.markdown("""
            <div style='text-align: center; margin-bottom: 20px;'>
                <p style='font-size: 18px; color: #666; margin-top: 5px; font-style: italic;'>Precision Analysis for Informed Investment Decisions</p>
            </div>
        """, unsafe_allow_html=True)
    st.markdown('<p class="big-font">📈 Earnings Revision Ranker</p>', unsafe_allow_html=True)
    st.markdown("---")

    # Sidebar
    st.sidebar.header("⚙️ Settings")

    # Check which SP500 file to use
    sp500_file = 'SP500_list_with_sectors.xlsx' if os.path.exists('SP500_list_with_sectors.xlsx') else 'SP500_list.xlsx'
    has_sectors = 'with_sectors' in sp500_file

    # Scan mode selection
    scan_mode = st.sidebar.radio(
        "Scan Mode:",
        ["S&P 500", "S&P 500 by Sector", "Disruption Index", "Broad US Index", "Broad US by Sector"],
        help="Choose S&P 500, Disruption Index, or Broad US Index (~3000 stocks)"
    )

    num_stocks = None
    selected_sectors = None

    if scan_mode == "S&P 500":
        # Number of stocks to scan
        scan_options = {
            "Quick Test (20 stocks)": 20,
            "Medium Scan (50 stocks)": 50,
            "Large Scan (100 stocks)": 100,
            "Full S&P 500 (~500 stocks)": None
        }

        scan_choice = st.sidebar.selectbox(
            "Select scan size:",
            list(scan_options.keys())
        )

        num_stocks = scan_options[scan_choice]

    elif scan_mode == "S&P 500 by Sector":
        if not has_sectors:
            st.sidebar.warning("⚠️ Sector data not available. Run get_sp500_sectors.py first or use 'S&P 500' mode.")
            num_stocks = 20  # Default fallback
        else:
            available_sectors = get_available_sectors(sp500_file)

            if available_sectors:
                selected_sectors = st.sidebar.multiselect(
                    "Select Sectors:",
                    available_sectors,
                    default=available_sectors[:3] if len(available_sectors) >= 3 else available_sectors,
                    help="Select one or more sectors to analyze"
                )

                if not selected_sectors:
                    st.sidebar.warning("Please select at least one sector")

                # Show stock count estimate
                try:
                    df_temp = pd.read_excel(sp500_file)
                    if selected_sectors:
                        stock_count = len(df_temp[df_temp['Sector'].isin(selected_sectors)])
                        st.sidebar.info(f"📊 ~{stock_count} stocks in selected sector(s)")
                except:
                    pass

    elif scan_mode == "Disruption Index":
        from earnings_revision_ranker import EarningsRevisionRanker
        disruption_tickers = EarningsRevisionRanker.get_disruption_tickers()
        st.sidebar.info(f"📊 {len(disruption_tickers)} Disruption Index stocks")
        st.sidebar.markdown("""
        **Disruption Index includes:**
        - High-growth tech (NVDA, AMD, PLTR)
        - Fintech (COIN, SQ, SOFI, HOOD)
        - Cloud/SaaS (SNOW, DDOG, CRWD)
        - EV/Mobility (TSLA, RIVN, UBER)
        - And 300+ more innovation leaders
        """)

    elif scan_mode == "Broad US Index":
        # Number of stocks to scan
        broad_scan_options = {
            "Quick Test (50 stocks)": 50,
            "Medium Scan (200 stocks)": 200,
            "Large Scan (500 stocks)": 500,
            "Full Index (~3000 stocks)": None
        }

        scan_choice = st.sidebar.selectbox(
            "Select scan size:",
            list(broad_scan_options.keys())
        )

        num_stocks = broad_scan_options[scan_choice]
        st.sidebar.info("📊 Broad US Index covers ~3000 stocks across all sectors")

    else:  # Broad US by Sector
        broad_us_sectors = get_broad_us_sectors()

        if broad_us_sectors:
            selected_sectors = st.sidebar.multiselect(
                "Select Sectors:",
                broad_us_sectors,
                default=broad_us_sectors[:3] if len(broad_us_sectors) >= 3 else broad_us_sectors,
                help="Select one or more sectors to analyze"
            )

            if not selected_sectors:
                st.sidebar.warning("Please select at least one sector")

            # Show stock count estimate
            try:
                df_temp = pd.read_excel('Index_Broad_US.xlsx')
                if selected_sectors:
                    stock_count = len(df_temp[df_temp['Sector'].isin(selected_sectors)])
                    st.sidebar.info(f"📊 ~{stock_count} stocks in selected sector(s)")
            except:
                pass
        else:
            st.sidebar.warning("⚠️ Could not load Broad US Index file")

    # Advanced options
    with st.sidebar.expander("⚙️ Advanced Settings"):
        max_workers = st.slider("Parallel Workers", 1, 20, 10, help="More workers = faster scanning, but may hit API limits")

    # Scan button
    scan_disabled = (scan_mode in ["S&P 500 by Sector", "Broad US by Sector"] and not selected_sectors)

    if st.sidebar.button("🚀 Run Scan", type="primary", disabled=scan_disabled):
        if scan_mode == "Disruption Index":
            scan_description = "Disruption Index stocks"
        elif scan_mode == "S&P 500":
            scan_description = f"S&P 500 - {scan_choice}"
        elif scan_mode == "Broad US Index":
            scan_description = f"Broad US Index - {scan_choice}"
        elif scan_mode == "Broad US by Sector":
            scan_description = f"Broad US - {len(selected_sectors)} sector(s)"
        else:
            scan_description = f"S&P 500 - {len(selected_sectors)} sector(s)"

        with st.spinner(f'Scanning {scan_description}... Using {max_workers} parallel workers for faster processing.'):
            if scan_mode == "Disruption Index":
                st.session_state['df'] = load_disruption_data(max_workers)
            elif scan_mode == "Broad US Index":
                st.session_state['df'] = load_broad_us_data(num_stocks, max_workers)
            elif scan_mode == "Broad US by Sector":
                st.session_state['df'] = load_broad_us_data(None, max_workers, selected_sectors)
            else:
                st.session_state['df'] = load_data(
                    num_stocks,
                    max_workers,
                    selected_sectors if scan_mode == "S&P 500 by Sector" else None,
                    sp500_file
                )
            st.session_state['scan_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            st.session_state['scan_mode'] = scan_mode
            st.session_state['scan_sectors'] = selected_sectors if scan_mode in ["S&P 500 by Sector", "Broad US by Sector"] else None

        st.sidebar.success("✅ Scan complete!")

    # Display controls
    st.sidebar.markdown("---")
    st.sidebar.header("📊 Display Options")

    show_top_n = st.sidebar.slider("Show top N stocks:", 10, 200, 50)
    min_score = st.sidebar.slider("Minimum revision score:", -50, 50, 0)
    show_all_columns = st.sidebar.checkbox("Show all columns", value=False, help="Display all available data columns")

    # Beats/Misses filters
    st.sidebar.markdown("---")
    st.sidebar.header("🎯 Earnings Beats/Misses")
    min_beats = st.sidebar.slider("Minimum beats (last 4Q):", 0, 4, 0, help="Filter for stocks with at least N earnings beats")
    show_streaks_only = st.sidebar.checkbox("Show beat streaks only", value=False, help="Only show stocks on a beat streak")

    # Main content
    if 'df' not in st.session_state:
        st.info("👈 Click 'Run Scan' in the sidebar to start analyzing earnings revisions")

        # Show example/instructions
        st.markdown("""
        ### How It Works

        This dashboard ranks companies based on **earnings execution** - which stocks are
        consistently beating estimates and attracting analyst upgrades.

        #### Score Factors:
        - **Earnings Beats** (40 pts max): 10 pts per beat in last 4 quarters, -8 pts per miss
        - **Earnings Surprise %** (30 pts max): Average surprise magnitude vs estimates
        - **Analyst Upgrades/Downgrades** (30 pts max): Net upgrades vs downgrades in last 90 days

        #### Example Scores:
        - **NVDA** (4 beats, +5% avg surprise): ~55+ pts
        - **TSLA** (1 beat, 3 misses): ~-14 pts

        #### Why This Matters:
        Stocks that consistently beat estimates demonstrate strong execution and often outperform
        as positive momentum attracts more buyers.

        Get started by clicking **"Run Scan"** in the sidebar!
        """)

    else:
        df = st.session_state['df']

        # Apply filters
        df_filtered = df[df['revision_strength_score'] >= min_score].copy()

        # Apply beats/misses filters if columns exist
        if 'beats_4q' in df_filtered.columns:
            df_filtered = df_filtered[df_filtered['beats_4q'] >= min_beats]

        if show_streaks_only and 'streak' in df_filtered.columns:
            # Filter for positive beat streaks (streak > 0 means consecutive beats)
            df_filtered = df_filtered[df_filtered['streak'] > 0]

        # Summary metrics
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                "Stocks Analyzed",
                len(df),
                delta=None
            )

        with col2:
            positive_count = len(df[df['revision_strength_score'] > 0])
            st.metric(
                "Positive Revisions",
                positive_count,
                delta=f"{(positive_count/len(df)*100):.1f}%"
            )

        with col3:
            avg_score = df['revision_strength_score'].mean()
            st.metric(
                "Avg Score",
                f"{avg_score:.2f}",
                delta=None
            )

        with col4:
            top_score = df['revision_strength_score'].max()
            st.metric(
                "Highest Score",
                f"{top_score:.2f}",
                delta=None
            )

        with col5:
            scan_info = st.session_state.get('scan_mode', 'N/A')
            if scan_info == "By Sector" and st.session_state.get('scan_sectors'):
                scan_info = f"{len(st.session_state['scan_sectors'])} Sectors"
            st.metric(
                "Scan Mode",
                scan_info,
                delta=None
            )

        st.markdown("---")

        # Tabs for different views
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["🏆 Rankings", "📊 Charts", "📈 Analysis", "📋 Raw Data", "📅 Revision Tracker"])

        with tab1:
            st.subheader(f"Top {show_top_n} Stocks by Revision Strength")

            # Top stocks table with custom formatting
            top_stocks = df_filtered.head(show_top_n).copy()

            # Format the display - include sector and beats/misses if available
            if show_all_columns:
                # All columns
                display_cols = [
                    'ticker',
                    'revision_strength_score',
                    'eps_revision_pct',
                    'revenue_revision_pct',
                    'strong_buy',
                    'buy',
                    'hold',
                    'sell',
                    'upgrades_count',
                    'downgrades_count',
                    'price_target_avg'
                ]
                col_names_base = [
                    'Score',
                    'EPS Rev %',
                    'Rev Rev %',
                    'Strong Buy',
                    'Buy',
                    'Hold',
                    'Sell',
                    'Upgrades',
                    'Downgrades',
                    'Price Target'
                ]
            else:
                # Simplified view
                display_cols = [
                    'ticker',
                    'revision_strength_score',
                    'strong_buy',
                    'buy',
                    'hold',
                    'sell',
                    'price_target_avg'
                ]
                col_names_base = [
                    'Score',
                    'Strong Buy',
                    'Buy',
                    'Hold',
                    'Sell',
                    'Price Target'
                ]

            if 'sector' in top_stocks.columns:
                display_cols.insert(1, 'sector')

            # Add beats/misses columns if available
            beats_cols_available = []
            for col in ['beats_4q', 'misses_4q', 'streak', 'avg_surprise_pct']:
                if col in top_stocks.columns:
                    display_cols.append(col)
                    beats_cols_available.append(col)

            display_df = top_stocks[display_cols].copy()

            # Set column names based on what's included
            col_names = ['Ticker']
            if 'sector' in top_stocks.columns:
                col_names.append('Sector')
            col_names.extend(col_names_base)

            # Add beats/misses column names
            for col in beats_cols_available:
                if col == 'beats_4q':
                    col_names.append('Beats (4Q)')
                elif col == 'misses_4q':
                    col_names.append('Misses (4Q)')
                elif col == 'streak':
                    col_names.append('Streak')
                elif col == 'avg_surprise_pct':
                    col_names.append('Avg Surprise %')

            display_df.columns = col_names

            # Color code the dataframe
            def highlight_score(val):
                if pd.isna(val):
                    return ''
                try:
                    val = float(val)
                    if val > 20:
                        return 'background-color: #90EE90'  # Light green
                    elif val > 10:
                        return 'background-color: #FFFFE0'  # Light yellow
                    elif val < -10:
                        return 'background-color: #FFB6C6'  # Light red
                except:
                    return ''
                return ''

            # Build subset for highlighting
            highlight_subset = ['Score']
            if 'Avg Surprise %' in col_names:
                highlight_subset.append('Avg Surprise %')

            styled_df = display_df.style.applymap(
                highlight_score,
                subset=highlight_subset
            )

            # Build format dict
            format_dict = {
                'Score': '{:.2f}',
                'Price Target': '{:.2f}'
            }
            if 'EPS Rev %' in col_names:
                format_dict['EPS Rev %'] = '{:.2f}'
            if 'Rev Rev %' in col_names:
                format_dict['Rev Rev %'] = '{:.2f}'
            if 'Avg Surprise %' in col_names:
                format_dict['Avg Surprise %'] = '{:.2f}'

            styled_df = styled_df.format(format_dict, na_rep='N/A')

            st.dataframe(
                styled_df,
                use_container_width=True,
                height=600
            )

            # Download button
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Top Stocks CSV",
                data=csv,
                file_name=f"top_earnings_revisions_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

        with tab2:
            st.subheader("Visual Analysis")

            # Top stocks bar chart
            st.plotly_chart(
                create_top_stocks_chart(df_filtered, show_top_n),
                use_container_width=True
            )

            col1, col2 = st.columns(2)

            with col1:
                # Score distribution
                st.plotly_chart(
                    create_score_distribution(df_filtered),
                    use_container_width=True
                )

            with col2:
                # Scatter plot
                st.plotly_chart(
                    create_scatter_plot(df_filtered),
                    use_container_width=True
                )

        with tab3:
            st.subheader("Detailed Analysis")

            # Sector analysis would go here if we had sector data
            st.markdown("### Revision Leaders")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 🚀 Strongest EPS Revisions")
                eps_leaders = df_filtered.nlargest(10, 'eps_revision_pct')[['ticker', 'eps_revision_pct', 'current_eps_q1']]
                eps_leaders.columns = ['Ticker', 'EPS Rev %', 'EPS Q1 Est']
                st.dataframe(eps_leaders, use_container_width=True)

            with col2:
                st.markdown("#### 📊 Most Analyst Upgrades")
                upgrade_leaders = df_filtered.nlargest(10, 'net_rating_change')[['ticker', 'net_rating_change', 'upgrades_count', 'downgrades_count']]
                upgrade_leaders.columns = ['Ticker', 'Net Change', 'Upgrades', 'Downgrades']
                st.dataframe(upgrade_leaders, use_container_width=True)

            st.markdown("---")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 📈 Growing Analyst Coverage")
                analyst_leaders = df_filtered.nlargest(10, 'analyst_count_change')[['ticker', 'analyst_count_change', 'analyst_count_eps']]
                analyst_leaders.columns = ['Ticker', 'Analyst Δ', 'Total Analysts']
                st.dataframe(analyst_leaders, use_container_width=True)

            with col2:
                st.markdown("#### 💰 Revenue Revision Leaders")
                rev_leaders = df_filtered.nlargest(10, 'revenue_revision_pct')[['ticker', 'revenue_revision_pct', 'current_revenue_q1']]
                rev_leaders.columns = ['Ticker', 'Rev Rev %', 'Rev Q1 Est']
                st.dataframe(rev_leaders, use_container_width=True)

            # FY Estimates Lookup Section
            st.markdown("---")
            st.markdown("### 📊 FY Earnings Estimates Lookup")
            st.caption("Look up FY1, FY2, FY3 consensus earnings estimates from FMP")

            fy_ticker = st.text_input("Enter ticker symbol:", "", key="fy_estimates_lookup").upper()

            if fy_ticker:
                with st.spinner(f"Fetching estimates for {fy_ticker}..."):
                    fy_data = get_fy_estimates(fy_ticker)

                if 'error' in fy_data:
                    st.error(fy_data['error'])
                elif fy_data.get('estimates'):
                    st.success(f"Found {len(fy_data['estimates'])} fiscal year estimates for {fy_ticker}")

                    # Display as columns
                    cols = st.columns(len(fy_data['estimates']))

                    for i, est in enumerate(fy_data['estimates']):
                        with cols[i]:
                            st.markdown(f"**{est['fiscal_year']}** ({est['fiscal_end']})")

                            # EPS
                            if est['eps_avg']:
                                st.metric(
                                    "EPS Estimate",
                                    f"${est['eps_avg']:.2f}",
                                    help=f"Range: ${est['eps_low']:.2f} - ${est['eps_high']:.2f}"
                                )
                                st.caption(f"Range: ${est['eps_low']:.2f} - ${est['eps_high']:.2f}")
                                st.caption(f"Analysts: {est['num_analysts_eps']}")

                            # Revenue
                            if est['revenue_avg']:
                                rev_billions = est['revenue_avg'] / 1e9
                                st.metric(
                                    "Revenue Estimate",
                                    f"${rev_billions:.2f}B",
                                    help=f"Range: ${est['revenue_low']/1e9:.2f}B - ${est['revenue_high']/1e9:.2f}B"
                                )
                                st.caption(f"Analysts: {est['num_analysts_rev']}")

                    # Also show as table
                    st.markdown("#### Detailed View")
                    table_data = []
                    for est in fy_data['estimates']:
                        table_data.append({
                            'Fiscal Year': est['fiscal_year'],
                            'Period End': est['fiscal_end'],
                            'EPS Avg': f"${est['eps_avg']:.2f}" if est['eps_avg'] else 'N/A',
                            'EPS Low': f"${est['eps_low']:.2f}" if est['eps_low'] else 'N/A',
                            'EPS High': f"${est['eps_high']:.2f}" if est['eps_high'] else 'N/A',
                            'Revenue Avg': f"${est['revenue_avg']/1e9:.2f}B" if est['revenue_avg'] else 'N/A',
                            '# EPS Analysts': est['num_analysts_eps'],
                            '# Rev Analysts': est['num_analysts_rev']
                        })
                    st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True)
                else:
                    st.warning(f"No FY estimates found for {fy_ticker}")

            # Earnings Beats/Misses section (if data available)
            if 'beats_4q' in df_filtered.columns:
                st.markdown("---")
                st.markdown("### Earnings Beats/Misses (Last 4 Quarters)")

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("#### 🏆 Consistent Beaters")
                    beat_cols = ['ticker', 'beats_4q', 'misses_4q', 'streak', 'avg_surprise_pct']
                    beat_cols = [c for c in beat_cols if c in df_filtered.columns]
                    beat_leaders = df_filtered.nlargest(10, 'beats_4q')[beat_cols]
                    beat_leaders.columns = ['Ticker', 'Beats', 'Misses', 'Streak', 'Avg Surprise %'][:len(beat_cols)]
                    st.dataframe(beat_leaders, use_container_width=True)

                with col2:
                    st.markdown("#### 📈 Biggest Upside Surprises")
                    if 'avg_surprise_pct' in df_filtered.columns:
                        surprise_cols = ['ticker', 'avg_surprise_pct', 'beats_4q', 'streak']
                        surprise_cols = [c for c in surprise_cols if c in df_filtered.columns]
                        surprise_leaders = df_filtered.nlargest(10, 'avg_surprise_pct')[surprise_cols]
                        surprise_leaders.columns = ['Ticker', 'Avg Surprise %', 'Beats', 'Streak'][:len(surprise_cols)]
                        st.dataframe(surprise_leaders, use_container_width=True)
                    else:
                        st.info("Average surprise data not available")

        with tab4:
            st.subheader("Complete Dataset")

            # Search/filter
            search_ticker = st.text_input("Search by ticker:", "").upper()

            if search_ticker:
                filtered_data = df_filtered[df_filtered['ticker'].str.contains(search_ticker)]
            else:
                filtered_data = df_filtered

            st.dataframe(
                filtered_data,
                use_container_width=True,
                height=600
            )

            # Download full dataset
            full_csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Full Dataset CSV",
                data=full_csv,
                file_name=f"earnings_revisions_full_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

        with tab5:
            st.subheader("Estimates Revision Tracker")

            # Get tracker status
            tracker_status = get_estimates_tracker_status()

            if tracker_status is None:
                st.warning("No estimates tracking data available yet. Data collection starts automatically via GitHub Actions.")
                st.info("Estimates are captured daily at 6 AM ET. After 30 days, you'll see real revision trends.")
            elif 'error' in tracker_status:
                st.error(f"Error reading tracker: {tracker_status['error']}")
            else:
                # Show status metrics
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Days of Data", tracker_status['days_of_data'])
                with col2:
                    st.metric("Tickers Tracked", tracker_status['ticker_count'])
                with col3:
                    st.metric("Latest Snapshot", tracker_status['latest_date'] or "N/A")
                with col4:
                    days_until_revisions = max(0, 30 - tracker_status['days_of_data'])
                    if days_until_revisions > 0:
                        st.metric("Days Until Revisions", days_until_revisions)
                    else:
                        st.metric("Revision Data", "Available!")

                st.markdown("---")

                # Ticker lookup
                st.markdown("### Look Up Ticker Estimates History")
                lookup_ticker = st.text_input("Enter ticker symbol:", "", key="revision_lookup").upper()

                if lookup_ticker:
                    revision_df = get_revision_data(lookup_ticker)

                    if revision_df is not None and len(revision_df) > 0:
                        st.success(f"Found {len(revision_df)} estimate records for {lookup_ticker}")

                        # Show data table
                        st.dataframe(revision_df, use_container_width=True)

                        # If we have multiple dates, show revision calculation
                        unique_dates = revision_df['Date'].unique()
                        if len(unique_dates) >= 2:
                            st.markdown("### Estimate Changes Over Time")

                            # Get first fiscal period
                            first_period = revision_df['Fiscal Period'].iloc[0]
                            period_data = revision_df[revision_df['Fiscal Period'] == first_period]

                            if len(period_data) >= 2:
                                latest = period_data.iloc[0]
                                oldest = period_data.iloc[-1]

                                col1, col2 = st.columns(2)
                                with col1:
                                    if oldest['EPS Estimate'] and oldest['EPS Estimate'] != 0:
                                        eps_change = ((latest['EPS Estimate'] - oldest['EPS Estimate']) / abs(oldest['EPS Estimate'])) * 100
                                        st.metric(
                                            f"EPS Revision ({oldest['Date']} to {latest['Date']})",
                                            f"{latest['EPS Estimate']:.2f}",
                                            f"{eps_change:+.2f}%"
                                        )
                                with col2:
                                    if oldest['Revenue Estimate'] and oldest['Revenue Estimate'] != 0:
                                        rev_change = ((latest['Revenue Estimate'] - oldest['Revenue Estimate']) / abs(oldest['Revenue Estimate'])) * 100
                                        st.metric(
                                            f"Revenue Revision",
                                            f"${latest['Revenue Estimate']/1e9:.2f}B",
                                            f"{rev_change:+.2f}%"
                                        )
                    else:
                        st.warning(f"No estimate data found for {lookup_ticker}. It may not be in the tracked universe.")

                # Show progress info
                if tracker_status['days_of_data'] < 30:
                    st.markdown("---")
                    st.info(f"""
                    **Building Revision History**

                    Currently tracking {tracker_status['ticker_count']} stocks daily.
                    After 30 days of data, you'll be able to see real EPS and revenue revision trends.

                    Progress: {tracker_status['days_of_data']}/30 days ({(tracker_status['days_of_data']/30*100):.0f}%)
                    """)

        # Sector Revision Summary (if sector data available)
        if 'sector' in df_filtered.columns:
            st.markdown("---")
            st.markdown("### 📊 Sector Revision Summary")

            # Calculate sector averages
            sector_summary = df_filtered.groupby('sector').agg({
                'revision_strength_score': 'mean',
                'eps_revision_pct': 'mean',
                'revenue_revision_pct': 'mean',
                'ticker': 'count'
            }).reset_index()

            sector_summary.columns = ['Sector', 'Avg Revision Score', 'Avg EPS Rev %', 'Avg Rev Rev %', 'Stock Count']
            sector_summary = sector_summary.sort_values('Avg Revision Score', ascending=False)

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 🚀 Sectors with Largest Revision Increases")
                top_sectors = sector_summary.head(5).copy()

                def highlight_positive(val):
                    try:
                        if float(val) > 0:
                            return 'background-color: #90EE90'
                    except:
                        pass
                    return ''

                styled_top = top_sectors.style.applymap(
                    highlight_positive,
                    subset=['Avg Revision Score', 'Avg EPS Rev %', 'Avg Rev Rev %']
                ).format({
                    'Avg Revision Score': '{:.2f}',
                    'Avg EPS Rev %': '{:.2f}',
                    'Avg Rev Rev %': '{:.2f}'
                })
                st.dataframe(styled_top, use_container_width=True, hide_index=True)

            with col2:
                st.markdown("#### 📉 Sectors with Largest Revision Decreases")
                bottom_sectors = sector_summary.tail(5).sort_values('Avg Revision Score', ascending=True).copy()

                def highlight_negative(val):
                    try:
                        if float(val) < 0:
                            return 'background-color: #FFB6C6'
                    except:
                        pass
                    return ''

                styled_bottom = bottom_sectors.style.applymap(
                    highlight_negative,
                    subset=['Avg Revision Score', 'Avg EPS Rev %', 'Avg Rev Rev %']
                ).format({
                    'Avg Revision Score': '{:.2f}',
                    'Avg EPS Rev %': '{:.2f}',
                    'Avg Rev Rev %': '{:.2f}'
                })
                st.dataframe(styled_bottom, use_container_width=True, hide_index=True)

        # Footer
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center; color: gray;'>
        <small>Data source: Financial Modeling Prep API |
        Revision scores are calculated based on EPS/revenue estimate changes, analyst coverage changes, and rating upgrades/downgrades</small>
        </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
