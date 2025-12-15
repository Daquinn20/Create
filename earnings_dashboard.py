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
    """Load and cache earnings revision data"""
    ranker = EarningsRevisionRanker(max_workers=max_workers)
    df = ranker.scan_sp500(
        sp500_file=sp500_file,
        max_stocks=num_stocks,
        parallel=True,
        sectors=sectors
    )
    return df


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
    st.markdown('<p class="big-font">📈 S&P 500 Earnings Revision Ranker</p>', unsafe_allow_html=True)
    st.markdown("---")

    # Sidebar
    st.sidebar.header("⚙️ Settings")

    # Check which SP500 file to use
    sp500_file = 'SP500_list_with_sectors.xlsx' if os.path.exists('SP500_list_with_sectors.xlsx') else 'SP500_list.xlsx'
    has_sectors = 'with_sectors' in sp500_file

    # Scan mode selection
    scan_mode = st.sidebar.radio(
        "Scan Mode:",
        ["By Number of Stocks", "By Sector"],
        help="Choose to scan a specific number of stocks or filter by sector"
    )

    num_stocks = None
    selected_sectors = None

    if scan_mode == "By Number of Stocks":
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

    else:  # By Sector
        if not has_sectors:
            st.sidebar.warning("⚠️ Sector data not available. Run get_sp500_sectors.py first or use 'By Number of Stocks' mode.")
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

    # Advanced options
    with st.sidebar.expander("⚙️ Advanced Settings"):
        max_workers = st.slider("Parallel Workers", 1, 20, 10, help="More workers = faster scanning, but may hit API limits")

    # Scan button
    scan_disabled = (scan_mode == "By Sector" and not selected_sectors)

    if st.sidebar.button("🚀 Run Scan", type="primary", disabled=scan_disabled):
        scan_description = f"{scan_choice}" if scan_mode == "By Number of Stocks" else f"{len(selected_sectors)} sector(s)"

        with st.spinner(f'Scanning {scan_description}... Using {max_workers} parallel workers for faster processing.'):
            st.session_state['df'] = load_data(
                num_stocks,
                max_workers,
                selected_sectors if scan_mode == "By Sector" else None,
                sp500_file
            )
            st.session_state['scan_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            st.session_state['scan_mode'] = scan_mode
            st.session_state['scan_sectors'] = selected_sectors if scan_mode == "By Sector" else None

        st.sidebar.success("✅ Scan complete!")

    # Display controls
    st.sidebar.markdown("---")
    st.sidebar.header("📊 Display Options")

    show_top_n = st.sidebar.slider("Show top N stocks:", 10, 200, 50)
    min_score = st.sidebar.slider("Minimum revision score:", -50, 50, 0)

    # Main content
    if 'df' not in st.session_state:
        st.info("👈 Click 'Run Scan' in the sidebar to start analyzing earnings revisions")

        # Show example/instructions
        st.markdown("""
        ### How It Works

        This dashboard ranks S&P 500 companies based on **earnings revision strength** to identify which stocks
        are experiencing the most positive estimate changes from analysts.

        #### Revision Strength Score Factors:
        - **EPS Revisions** (40 pts max): Magnitude of earnings estimate increases
        - **Revenue Revisions** (20 pts max): Magnitude of revenue estimate increases
        - **Analyst Coverage** (15 pts max): Growth in number of analysts covering the stock
        - **Rating Changes** (25 pts max): Net upgrades vs downgrades in last 90 days

        #### Why This Matters:
        Stocks with strong upward earnings revisions often outperform as positive momentum attracts more buyers
        and validates the company's fundamental improvement.

        Get started by clicking **"Run Scan"** in the sidebar!
        """)

    else:
        df = st.session_state['df']

        # Apply filters
        df_filtered = df[df['revision_strength_score'] >= min_score].copy()

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
        tab1, tab2, tab3, tab4 = st.tabs(["🏆 Rankings", "📊 Charts", "📈 Analysis", "📋 Raw Data"])

        with tab1:
            st.subheader(f"Top {show_top_n} Stocks by Revision Strength")

            # Top stocks table with custom formatting
            top_stocks = df_filtered.head(show_top_n).copy()

            # Format the display - include sector if available
            display_cols = [
                'ticker',
                'revision_strength_score',
                'eps_revision_pct',
                'revenue_revision_pct',
                'net_rating_change',
                'analyst_count_change',
                'current_eps_q1',
                'price_target_avg'
            ]

            if 'sector' in top_stocks.columns:
                display_cols.insert(1, 'sector')

            display_df = top_stocks[display_cols].copy()

            # Set column names based on what's included
            col_names = ['Ticker']
            if 'sector' in top_stocks.columns:
                col_names.append('Sector')
            col_names.extend([
                'Score',
                'EPS Rev %',
                'Rev Rev %',
                'Net Ratings',
                'Analyst Δ',
                'EPS Q1',
                'Price Target'
            ])

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

            styled_df = display_df.style.applymap(
                highlight_score,
                subset=['Score', 'EPS Rev %', 'Rev Rev %']
            ).format({
                'Score': '{:.2f}',
                'EPS Rev %': '{:.2f}',
                'Rev Rev %': '{:.2f}',
                'EPS Q1': '{:.2f}',
                'Price Target': '{:.2f}'
            }, na_rep='N/A')

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
