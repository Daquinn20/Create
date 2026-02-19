"""
SMA Volatility Screener Dashboard
Interactive Streamlit dashboard for viewing technical analysis screening results
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
from datetime import datetime
import os
import glob
import subprocess
import sys

# Page config
st.set_page_config(
    page_title="Targeted Equity Consulting Group - SMA Screener",
    page_icon="🎯",
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


@st.cache_data(ttl=300)
def load_latest_results():
    """Load the most recent screening results"""
    files = glob.glob('sma_volatility_screen_*.xlsx')
    if not files:
        return None
    latest_file = max(files, key=os.path.getctime)

    # Load all sheets
    excel_file = pd.ExcelFile(latest_file)
    sheets = {}
    for sheet_name in excel_file.sheet_names:
        sheets[sheet_name] = pd.read_excel(latest_file, sheet_name=sheet_name)

    return sheets, latest_file


@st.cache_data(ttl=300)
def get_stock_chart_data(ticker, period='6mo'):
    """Fetch stock data for charting"""
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=period)

        # Calculate SMAs
        df['SMA_10'] = df['Close'].rolling(window=10).mean()
        df['SMA_20'] = df['Close'].rolling(window=20).mean()
        df['SMA_50'] = df['Close'].rolling(window=50).mean()
        df['SMA_100'] = df['Close'].rolling(window=100).mean()
        df['SMA_150'] = df['Close'].rolling(window=150).mean()
        df['SMA_200'] = df['Close'].rolling(window=200).mean()

        return df
    except Exception as e:
        st.error(f"Error fetching data for {ticker}: {str(e)}")
        return None


def create_price_sma_chart(df, ticker):
    """Create price chart with SMAs"""
    fig = go.Figure()

    # Price
    fig.add_trace(go.Scatter(
        x=df.index, y=df['Close'],
        name='Price',
        line=dict(color='black', width=2)
    ))

    # SMAs
    colors = {
        'SMA_10': 'purple',
        'SMA_20': 'blue',
        'SMA_50': 'green',
        'SMA_100': 'orange',
        'SMA_150': 'red',
        'SMA_200': 'brown'
    }

    for sma, color in colors.items():
        fig.add_trace(go.Scatter(
            x=df.index, y=df[sma],
            name=sma,
            line=dict(color=color, width=1.5, dash='dash')
        ))

    fig.update_layout(
        title=f'{ticker} - Price & Moving Averages',
        xaxis_title='Date',
        yaxis_title='Price ($)',
        hovermode='x unified',
        height=500
    )

    return fig


def create_volume_chart(df, ticker):
    """Create volume chart"""
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df.index, y=df['Volume'],
        name='Volume',
        marker_color='lightblue'
    ))

    # Add volume moving average
    df['Volume_MA'] = df['Volume'].rolling(window=20).mean()
    fig.add_trace(go.Scatter(
        x=df.index, y=df['Volume_MA'],
        name='20-Day Avg Volume',
        line=dict(color='red', width=2)
    ))

    fig.update_layout(
        title=f'{ticker} - Volume Analysis',
        xaxis_title='Date',
        yaxis_title='Volume',
        hovermode='x unified',
        height=300
    )

    return fig


def create_volatility_chart(df, ticker):
    """Create volatility chart"""
    # Calculate rolling volatility
    df['Volatility_20'] = df['Close'].rolling(window=20).std()
    df['Volatility_60'] = df['Close'].rolling(window=60).std()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df.index, y=df['Volatility_20'],
        name='20-Day Volatility',
        line=dict(color='blue', width=2)
    ))

    fig.add_trace(go.Scatter(
        x=df.index, y=df['Volatility_60'],
        name='60-Day Volatility',
        line=dict(color='red', width=2)
    ))

    fig.update_layout(
        title=f'{ticker} - Volatility Analysis',
        xaxis_title='Date',
        yaxis_title='Volatility (Std Dev)',
        hovermode='x unified',
        height=300
    )

    return fig


def main():
    # Company branding with logo
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
                <p style='font-size: 18px; color: #666; margin-top: 0px; margin-bottom: 10px; font-style: italic;'>Precision Analysis for Informed Investment Decisions</p>
            </div>
            <h2 style='text-align: center; color: #666; margin-top: 0px;'>
                SMA Volatility Screener Dashboard
            </h2>
        """, unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning("⚠️ Logo file 'company_logo.png' not found")
        st.markdown("""
            <div style='text-align: center; margin-bottom: 20px;'>
                <p style='font-size: 18px; color: #666; margin-top: 5px; font-style: italic;'>Precision Analysis for Informed Investment Decisions</p>
                <h2 style='color: #666;'>SMA Volatility Screener Dashboard</h2>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # Index Selection and Screener Runner
    st.markdown("### 📊 Run New Screen")

    # Centralized master universe path
    MASTER_UNIVERSE_PATH = r"C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\AI dashboard Data\master_universe.csv"

    # Define available indexes
    INDEX_OPTIONS = {
        "Master Universe": MASTER_UNIVERSE_PATH,
        "SP 500": "SP500_list.xlsx",
        "Nasdaq 100": "Nasdaq100_list.xlsx",
        "Disruption Index": "disruption index.xlsx"
    }

    # Check which files exist
    available_indexes = {name: file for name, file in INDEX_OPTIONS.items() if os.path.exists(file)}

    if available_indexes:
        col1, col2, col3 = st.columns([2, 1, 2])

        with col1:
            # Default to SP 500 if it exists, otherwise first available
            default_idx = 0
            if "SP 500" in available_indexes:
                default_idx = list(available_indexes.keys()).index("SP 500")

            selected_index = st.selectbox(
                "Select Index to Screen:",
                options=list(available_indexes.keys()),
                index=default_idx,
                help="Choose which stock index to screen"
            )

        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            run_button = st.button("🚀 Run Screener", type="primary")

        with col3:
            # Show file info
            if selected_index:
                try:
                    file_path = available_indexes[selected_index]
                    if file_path.endswith('.csv'):
                        df_temp = pd.read_csv(file_path, header=None)
                    else:
                        df_temp = pd.read_excel(file_path)
                    intl_note = " (US + International)" if selected_index == "Master Universe" else ""
                    st.info(f"📄 {len(df_temp)} tickers in {selected_index}{intl_note}")
                except:
                    st.warning(f"📄 File: {available_indexes[selected_index]}")

        # Run screener if button clicked
        if run_button:
            with st.spinner(f"🔄 Running screener on {selected_index}... This may take a few minutes."):
                try:
                    # Run the screener using the same Python interpreter
                    result = subprocess.run(
                        [sys.executable, 'sma_volatility_screener.py', available_indexes[selected_index]],
                        capture_output=True,
                        text=True,
                        timeout=600
                    )

                    if result.returncode == 0:
                        st.success(f"✅ Screener completed for {selected_index}!")
                        # Clear cache to load new results
                        st.cache_data.clear()
                        # Show output in expander
                        with st.expander("View Screener Output"):
                            st.code(result.stdout, language="text")
                        st.info("🔄 Refresh the page to see the new results below")
                        st.rerun()
                    else:
                        st.error("❌ Screener encountered an error")
                        st.code(result.stderr, language="text")

                except subprocess.TimeoutExpired:
                    st.error("⏱️ Screener timed out after 10 minutes")
                except Exception as e:
                    st.error(f"❌ Error running screener: {str(e)}")
    else:
        st.warning("⚠️ No index files found. Please add index files to the directory.")
        st.caption("Expected files: master_universe.csv, SP500_list.xlsx, Nasdaq100_list.xlsx, disruption index.xlsx")

    st.markdown("---")

    # Load data
    result = load_latest_results()

    if result is None:
        st.error("No screening results found. Please run the screener first: `python sma_volatility_screener.py`")
        return

    sheets, filename = result

    # Header info
    st.markdown(f"**Latest Results:** {filename}")
    st.markdown(f"**Scan Date:** {datetime.fromtimestamp(os.path.getctime(filename)).strftime('%Y-%m-%d %H:%M:%S')}")

    # Grading System Explanation
    with st.expander("📊 **Understanding the Grading System**", expanded=False):
        st.markdown("""
        ### **Grade Criteria:**

        **Grade A (Excellent)** - All 3 metrics meet strong criteria:
        - ✅ Volatility Decline ≥ 50%
        - ✅ Volume Decline ≥ 20%
        - ✅ Price within 5% of 10-day SMA

        **Grade B (Good)** - 2 out of 3 metrics meet strong criteria

        **Grade C (Fair)** - 1 out of 3 metrics meet strong criteria

        **Note:** All stocks shown have already passed the following requirements:
        - Price above 20, 50, 100, and 150 SMA
        - Declining volatility trend
        - Declining volume trend
        """)

    # List selector
    st.markdown("### 🔍 Select Stock List:")

    available_sheets = list(sheets.keys())
    col1, col2 = st.columns([1, 3])

    with col1:
        selected_list = st.selectbox(
            "View:",
            available_sheets,
            format_func=lambda x: f"{'✅ ' if x == 'PASS' else '👀 ' if x == 'WATCHLIST' else ''}{ x}",
            help="PASS: Stocks within 5% of 10-day SMA | WATCHLIST: Stocks >5% from 10-day SMA"
        )

    df = sheets[selected_list]

    # Description of selected list
    if selected_list == "PASS":
        st.success(f"**✅ PASS STOCKS** - {len(df)} stocks within 5% of 10-day SMA (Ready for immediate consideration)")
    elif selected_list == "WATCHLIST":
        st.info(f"**👀 WATCHLIST STOCKS** - {len(df)} stocks >5% from 10-day SMA (Monitor these for potential entry)")
    elif selected_list == "FAIL":
        st.error(f"**❌ FAILED STOCKS** - {len(df)} stocks did not meet screening criteria")

    # Summary metrics - different for FAIL list
    if selected_list == "FAIL":
        # FAIL list has different columns
        col1, col2 = st.columns(2)

        with col1:
            st.metric("Total Failed Stocks", len(df))

        with col2:
            if 'Fail_Reason' in df.columns:
                most_common_reason = df['Fail_Reason'].value_counts().index[0]
                count = df['Fail_Reason'].value_counts().iloc[0]
                st.metric("Most Common Failure", most_common_reason, f"{count} stocks")

        # Show failure breakdown
        if 'Fail_Reason' in df.columns:
            st.markdown("### Failure Reasons Breakdown:")
            fail_counts = df['Fail_Reason'].value_counts()
            for reason, count in fail_counts.items():
                st.write(f"- **{reason}**: {count} stocks ({count/len(df)*100:.1f}%)")

    else:
        # PASS and WATCHLIST have full metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            grade_counts = df['Grade'].value_counts()
            st.metric("Total Stocks", len(df))
            st.caption(f"A: {grade_counts.get('A', 0)} | B: {grade_counts.get('B', 0)} | C: {grade_counts.get('C', 0)}")

        with col2:
            avg_vol_decline = df['Volume_Decline_%'].mean()
            st.metric("Avg Volume Decline", f"{avg_vol_decline:.1f}%")

        with col3:
            avg_volatility_decline = df['Vol_Decline_%'].mean()
            st.metric("Avg Volatility Decline", f"{avg_volatility_decline:.1f}%")

        with col4:
            avg_price_vs_sma20 = df['Price_vs_SMA20_%'].mean()
            st.metric("Avg Price vs SMA20", f"{avg_price_vs_sma20:.1f}%")

    st.markdown("---")

    # Filters in sidebar
    st.sidebar.header("Filters")

    if selected_list == "FAIL":
        # FAIL list has simpler filtering
        if 'Fail_Reason' in df.columns:
            fail_reasons = df['Fail_Reason'].unique().tolist()
            selected_reasons = st.sidebar.multiselect(
                "Filter by Failure Reason",
                options=fail_reasons,
                default=fail_reasons
            )
            filtered_df = df[df['Fail_Reason'].isin(selected_reasons)]
        else:
            filtered_df = df

        # Simple sort for FAIL list
        sort_by = st.sidebar.selectbox(
            "Sort By",
            ['Ticker', 'Fail_Reason', 'Price']
        )
        sort_order = st.sidebar.radio("Sort Order", ['Ascending', 'Descending'])
        filtered_df = filtered_df.sort_values(
            by=sort_by,
            ascending=(sort_order == 'Ascending')
        )

    else:
        # PASS and WATCHLIST have full filtering
        # Grade filter
        grade_filter = st.sidebar.multiselect(
            "Filter by Grade",
            options=['A', 'B', 'C'],
            default=['A', 'B', 'C'],
            help="A: Excellent | B: Good | C: Fair"
        )

        # Price range filter
        price_values = df['Price'].dropna()
        if len(price_values) > 0:
            min_price, max_price = st.sidebar.slider(
                "Price Range ($)",
                float(price_values.min()),
                float(price_values.max()),
                (float(price_values.min()), float(price_values.max()))
            )
        else:
            min_price, max_price = 0.0, 1000.0

        # Volume decline filter
        min_vol_decline = st.sidebar.slider(
            "Min Volume Decline (%)",
            0.0,
            float(df['Volume_Decline_%'].max()),
            0.0
        )

        # Volatility decline filter
        min_volatility_decline = st.sidebar.slider(
            "Min Volatility Decline (%)",
            0.0,
            float(df['Vol_Decline_%'].max()),
            0.0
        )

        # Sort options
        sort_by = st.sidebar.selectbox(
            "Sort By",
            ['Volume_Decline_%', 'Vol_Decline_%', 'Price_vs_SMA20_%', 'Price_vs_SMA50_%', 'Price']
        )

        sort_order = st.sidebar.radio("Sort Order", ['Descending', 'Ascending'])

        # Apply filters
        filtered_df = df[
            (df['Grade'].isin(grade_filter)) &
            (df['Price'] >= min_price) &
            (df['Price'] <= max_price) &
            (df['Volume_Decline_%'] >= min_vol_decline) &
            (df['Vol_Decline_%'] >= min_volatility_decline)
        ]

        # Sort
        filtered_df = filtered_df.sort_values(
            by=sort_by,
            ascending=(sort_order == 'Ascending')
        )

    st.subheader(f"Filtered Results: {len(filtered_df)} stocks")

    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📋 Results Table", "📈 Charts & Analysis", "📊 Statistics"])

    with tab1:
        # Display table
        st.dataframe(
            filtered_df,
            use_container_width=True,
            height=600
        )

        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Download Results as CSV",
            data=csv,
            file_name=f"sma_screener_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

    with tab2:
        st.subheader("Stock Analysis")

        # Only show charts for PASS and WATCHLIST (not FAIL)
        if selected_list == "FAIL":
            st.info("📊 Detailed charts are only available for PASS and WATCHLIST stocks.")
            st.markdown("**Failed stocks did not meet the screening criteria.**")
            if 'Fail_Reason' in filtered_df.columns:
                st.markdown("### Failure Reasons:")
                for ticker in filtered_df['Ticker'].head(10):
                    stock_data = filtered_df[filtered_df['Ticker'] == ticker].iloc[0]
                    st.write(f"- **{ticker}**: {stock_data.get('Fail_Reason', 'Unknown')}")
        else:
            # Stock selector
            selected_ticker = st.selectbox(
                "Select a stock to analyze:",
                filtered_df['Ticker'].tolist()
            )

            if selected_ticker:
                # Get stock data
                stock_data = filtered_df[filtered_df['Ticker'] == selected_ticker].iloc[0]

                # Display stock metrics
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Price", f"${stock_data['Price']:.2f}")

                with col2:
                    st.metric("Vol Decline", f"{stock_data['Vol_Decline_%']:.2f}%")

                with col3:
                    st.metric("Volume Decline", f"{stock_data['Volume_Decline_%']:.2f}%")

                with col4:
                    st.metric("vs SMA20", f"{stock_data['Price_vs_SMA20_%']:.2f}%")

                # Display SMA values
                st.markdown("**Simple Moving Averages:**")
                sma_col1, sma_col2, sma_col3, sma_col4, sma_col5, sma_col6 = st.columns(6)

                with sma_col1:
                    st.metric("SMA 10", f"${stock_data['SMA_10']:.2f}")
                with sma_col2:
                    st.metric("SMA 20", f"${stock_data['SMA_20']:.2f}")
                with sma_col3:
                    st.metric("SMA 50", f"${stock_data['SMA_50']:.2f}")
                with sma_col4:
                    st.metric("SMA 100", f"${stock_data['SMA_100']:.2f}")
                with sma_col5:
                    st.metric("SMA 150", f"${stock_data['SMA_150']:.2f}")
                with sma_col6:
                    st.metric("SMA 200", f"${stock_data['SMA_200']:.2f}")

            st.markdown("---")

            # Fetch and display charts
            with st.spinner(f"Loading charts for {selected_ticker}..."):
                chart_data = get_stock_chart_data(selected_ticker)

                if chart_data is not None:
                    # Price & SMA chart
                    st.plotly_chart(
                        create_price_sma_chart(chart_data, selected_ticker),
                        use_container_width=True
                    )

                    # Volume and volatility charts
                    col1, col2 = st.columns(2)

                    with col1:
                        st.plotly_chart(
                            create_volume_chart(chart_data, selected_ticker),
                            use_container_width=True
                        )

                    with col2:
                        st.plotly_chart(
                            create_volatility_chart(chart_data, selected_ticker),
                            use_container_width=True
                        )

    with tab3:
        st.subheader("Statistical Analysis")

        # Only show statistics for PASS and WATCHLIST (not FAIL)
        if selected_list == "FAIL":
            st.info("📊 Statistical analysis is only available for PASS and WATCHLIST stocks.")
            st.markdown("**FAIL list only contains basic information about stocks that didn't meet criteria.**")
        else:
            # Distribution charts
            col1, col2 = st.columns(2)

            with col1:
                # Volume decline distribution
                fig = px.histogram(
                    filtered_df,
                    x='Volume_Decline_%',
                    nbins=30,
                    title='Volume Decline Distribution',
                    labels={'Volume_Decline_%': 'Volume Decline (%)'}
                )
                st.plotly_chart(fig, use_container_width=True)

                # Price vs SMA20 distribution
                fig = px.histogram(
                    filtered_df,
                    x='Price_vs_SMA20_%',
                    nbins=30,
                    title='Price vs SMA20 Distribution',
                    labels={'Price_vs_SMA20_%': 'Price vs SMA20 (%)'}
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Volatility decline distribution
                fig = px.histogram(
                    filtered_df,
                    x='Vol_Decline_%',
                    nbins=30,
                    title='Volatility Decline Distribution',
                    labels={'Vol_Decline_%': 'Volatility Decline (%)'}
                )
                st.plotly_chart(fig, use_container_width=True)

                # Price distribution
                fig = px.histogram(
                    filtered_df,
                    x='Price',
                    nbins=30,
                    title='Price Distribution',
                    labels={'Price': 'Stock Price ($)'}
                )
                st.plotly_chart(fig, use_container_width=True)

            # Scatter plots
            st.markdown("### Correlation Analysis")

            col1, col2 = st.columns(2)

            with col1:
                # Volume decline vs Volatility decline
                fig = px.scatter(
                    filtered_df,
                    x='Volume_Decline_%',
                    y='Vol_Decline_%',
                    hover_data=['Ticker', 'Price'],
                    title='Volume Decline vs Volatility Decline',
                    labels={
                        'Volume_Decline_%': 'Volume Decline (%)',
                        'Vol_Decline_%': 'Volatility Decline (%)'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Price vs Volume decline
                fig = px.scatter(
                    filtered_df,
                    x='Price',
                    y='Volume_Decline_%',
                    hover_data=['Ticker'],
                    title='Price vs Volume Decline',
                    labels={
                        'Price': 'Stock Price ($)',
                        'Volume_Decline_%': 'Volume Decline (%)'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)

            # Summary statistics
            st.markdown("### Summary Statistics")

            summary_stats = filtered_df[['Price', 'Vol_Decline_%', 'Volume_Decline_%',
                                          'Price_vs_SMA20_%', 'Price_vs_SMA50_%']].describe()
            st.dataframe(summary_stats)


if __name__ == "__main__":
    main()
