"""
Earnings Revision Dashboard
Interactive Streamlit dashboard for viewing S&P 500 earnings revision rankings
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from typing import Optional
import os
import sqlite3

# Database configuration - check for Neon PostgreSQL
DATABASE_URL = None
USE_POSTGRES = False

# Try Streamlit secrets first, then environment variable
try:
    if hasattr(st, 'secrets') and 'DATABASE_URL' in st.secrets:
        DATABASE_URL = st.secrets['DATABASE_URL']
        USE_POSTGRES = True
except Exception:
    pass

if not DATABASE_URL:
    DATABASE_URL = os.getenv('DATABASE_URL')
    USE_POSTGRES = DATABASE_URL is not None

if USE_POSTGRES:
    try:
        import psycopg2
    except ImportError:
        USE_POSTGRES = False
        DATABASE_URL = None


def get_db_connection():
    """Get database connection - PostgreSQL (Neon) or SQLite"""
    if USE_POSTGRES and DATABASE_URL:
        return psycopg2.connect(DATABASE_URL)
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, "estimates_history.db")
        return sqlite3.connect(db_path)


def db_exists():
    """Check if database exists and has data"""
    if USE_POSTGRES and DATABASE_URL:
        return True  # Neon always exists if configured
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, "estimates_history.db")
        return os.path.exists(db_path)


# Import with fallback for Streamlit Cloud compatibility
try:
    from earnings_revision_ranker import EarningsRevisionRanker, MASTER_UNIVERSE_PATH, convert_to_fmp_ticker
except ImportError as e:
    st.error(f"Import error: {e}")
    # Fallback: define MASTER_UNIVERSE_PATH locally
    MASTER_UNIVERSE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_universe.csv")

    # Minimal convert_to_fmp_ticker fallback
    def convert_to_fmp_ticker(ticker: str) -> str:
        return ticker.strip()

    # Import just the class
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


@st.cache_data(ttl=3600)
def load_master_universe_data(num_stocks=None, max_workers=10):
    """Load and cache earnings revision data for Master Universe (centralized ticker source)"""
    ranker = EarningsRevisionRanker(max_workers=max_workers)
    df = ranker.scan_master_universe(
        parallel=True,
        max_stocks=num_stocks
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
    if not db_exists():
        return None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get snapshot dates
        cursor.execute("SELECT DISTINCT snapshot_date FROM estimate_snapshots ORDER BY snapshot_date DESC")
        dates = [row[0] for row in cursor.fetchall()]

        # Get ticker count
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM estimate_snapshots")
        ticker_count = cursor.fetchone()[0]

        conn.close()

        # Convert dates to strings (PostgreSQL returns date objects)
        dates_str = [str(d) for d in dates]

        return {
            'dates': dates_str,
            'ticker_count': ticker_count,
            'days_of_data': len(dates_str),
            'latest_date': dates_str[0] if dates_str else None,
            'oldest_date': dates_str[-1] if dates_str else None,
            'database': 'Neon PostgreSQL' if USE_POSTGRES else 'SQLite'
        }
    except Exception as e:
        return {'error': str(e)}


def get_revision_data(ticker: str, days: int = 30):
    """Get revision data for a specific ticker"""
    if not db_exists():
        return None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all snapshots for this ticker
        placeholder = '%s' if USE_POSTGRES else '?'
        cursor.execute(f"""
            SELECT snapshot_date, fiscal_period, eps_avg, revenue_avg
            FROM estimate_snapshots
            WHERE ticker = {placeholder}
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


def get_eps_revision_history(ticker: str) -> Optional[pd.DataFrame]:
    """
    Get EPS estimate revision history for FY1, FY2, FY3.
    Returns DataFrame with snapshot_date, FY1_EPS, FY2_EPS, FY3_EPS columns.
    """
    if not db_exists():
        return None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all snapshots for this ticker, ordered by date
        placeholder = '%s' if USE_POSTGRES else '?'
        cursor.execute(f"""
            SELECT snapshot_date, fiscal_period, eps_avg
            FROM estimate_snapshots
            WHERE ticker = {placeholder} AND eps_avg IS NOT NULL
            ORDER BY snapshot_date ASC, fiscal_period ASC
        """, (ticker.upper(),))

        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return None

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=['snapshot_date', 'fiscal_period', 'eps_avg'])

        # Get unique snapshot dates
        snapshot_dates = df['snapshot_date'].unique()

        # For each snapshot date, label the fiscal periods as FY1, FY2, FY3
        result_data = []
        for snap_date in snapshot_dates:
            day_data = df[df['snapshot_date'] == snap_date].sort_values('fiscal_period')
            fiscal_periods = day_data['fiscal_period'].tolist()
            eps_values = day_data['eps_avg'].tolist()

            row = {'snapshot_date': snap_date}
            for i, (fp, eps) in enumerate(zip(fiscal_periods[:3], eps_values[:3])):
                fy_label = f'FY{i+1}_EPS'
                row[fy_label] = eps
                row[f'FY{i+1}_period'] = fp

            result_data.append(row)

        result_df = pd.DataFrame(result_data)
        result_df['snapshot_date'] = pd.to_datetime(result_df['snapshot_date'])
        return result_df

    except Exception as e:
        return None


def create_eps_revision_chart(ticker: str, df: pd.DataFrame) -> go.Figure:
    """
    Create EPS revision trend chart showing FY1, FY2, FY3 estimates over time.
    Similar to the BANC chart in the PDF.
    """
    fig = go.Figure()

    # Colors matching the BANC PDF style
    colors = {
        'FY1_EPS': '#1f77b4',  # Dark blue
        'FY2_EPS': '#17becf',  # Teal/Cyan
        'FY3_EPS': '#2ca02c'   # Green
    }

    # Add a line for each fiscal year
    for col in ['FY1_EPS', 'FY2_EPS', 'FY3_EPS']:
        if col in df.columns:
            period_col = col.replace('_EPS', '_period')
            fy_label = col.replace('_EPS', ' EPS')

            # Get the fiscal period label if available
            if period_col in df.columns:
                period = df[period_col].iloc[-1] if len(df) > 0 else ''
                hover_template = f"{fy_label} ({period}): $%{{y:.2f}}<extra></extra>"
            else:
                hover_template = f"{fy_label}: $%{{y:.2f}}<extra></extra>"

            fig.add_trace(go.Scatter(
                x=df['snapshot_date'],
                y=df[col],
                mode='lines+markers',
                name=fy_label,
                line=dict(color=colors.get(col, '#333'), width=2),
                marker=dict(size=6),
                hovertemplate=hover_template
            ))

    fig.update_layout(
        title=f"EPS Estimate Chart: {ticker}",
        xaxis_title="Date",
        yaxis_title="EPS Estimate ($)",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        height=400,
        hovermode='x unified'
    )

    return fig


def _extract_tickers_from_excel(file_path: str) -> list:
    """
    Extract ticker symbols from any Excel file.
    Tries common column names: Ticker, Symbol, then falls back to first column.
    Special handling for known formats (e.g., Disruption Index).
    """
    try:
        filename = os.path.basename(file_path)

        # Special case: Disruption Index has header rows to skip
        if 'disruption' in filename.lower():
            df = pd.read_excel(file_path)
            symbols = df.iloc[2:, 1].dropna().tolist()
            return [str(s).upper().strip() for s in symbols if str(s).strip()]

        df = pd.read_excel(file_path)

        # Try common ticker column names
        for col in ['Ticker', 'Symbol', 'ticker', 'symbol', 'TICKER', 'SYMBOL']:
            if col in df.columns:
                tickers = df[col].dropna().astype(str).str.upper().str.strip().tolist()
                return [t for t in tickers if t and t != 'NAN']

        # Fallback: use first column
        tickers = df.iloc[:, 0].dropna().astype(str).str.upper().str.strip().tolist()
        return [t for t in tickers if t and t != 'NAN']

    except Exception:
        return []


def _filename_to_display_name(filename: str) -> str:
    """Convert a filename to a clean display name for the dropdown."""
    name = filename.replace('.xlsx', '').replace('.xls', '')

    # Known mappings
    known = {
        'SP500_list': 'S&P 500',
        'SP500_list_with_sectors': 'S&P 500 (Sectors)',
        'NASDAQ100_LIST': 'NASDAQ 100',
        'Russell_2000_index': 'Russell 2000',
        'Disruption Index': 'Disruption Index',
        'Index_Broad_US': 'Broad US',
        'International_Index': 'International (All)',
        'Index_UK_FTSE': 'UK / FTSE',
        'Index_Europe': 'Europe',
        'Index_Japan': 'Japan',
        'Index_Asia_Pacific': 'Asia Pacific',
        'Index_Canada': 'Canada / TSX',
        'MyIndex_list': 'My Index',
    }

    if name in known:
        return known[name]

    # Auto-format: replace underscores, strip 'Index_' prefix
    name = name.replace('Index_', '').replace('_index', '').replace('_list', '')
    name = name.replace('_', ' ').replace('-', ' ')
    return name.title()


# Files to exclude from auto-detection (not index files)
_EXCLUDED_FILES = {
    'VCP_Results.xlsx', 'VCP_Results_Enhanced.xlsx', 'revenue_analysis.xlsx',
    'financial_analysis_20251128_121945.xlsx', 'PREMARKET_MOVERS.xlsx',
    'Dividend Index_Broad_US.xlsx',
}


@st.cache_data(ttl=3600)
def get_available_indexes() -> list:
    """
    Auto-detect available index files.
    Any .xlsx file with a Ticker or Symbol column is treated as an index.
    Returns list of (display_name, file_path) tuples, plus "All Stocks" at the top.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    indexes = [("All Stocks", None)]

    # Prioritized files shown first
    priority_files = [
        'SP500_list.xlsx',
        'NASDAQ100_LIST.xlsx',
        'Index_Broad_US.xlsx',
        'Disruption Index.xlsx',
        'Russell_2000_index.xlsx',
    ]

    seen_files = set()

    # Add priority files first
    for filename in priority_files:
        filepath = os.path.join(script_dir, filename)
        if os.path.exists(filepath):
            tickers = _extract_tickers_from_excel(filepath)
            if len(tickers) >= 5:  # Must have at least 5 tickers to count
                display_name = _filename_to_display_name(filename)
                indexes.append((f"{display_name} ({len(tickers)})", filepath))
                seen_files.add(filename)

    # Auto-detect other Excel files with index-like names or Ticker/Symbol columns
    import glob
    for filepath in sorted(glob.glob(os.path.join(script_dir, '*.xlsx'))):
        filename = os.path.basename(filepath)

        # Skip already added, excluded, temp files, and result files
        if filename in seen_files or filename in _EXCLUDED_FILES:
            continue
        if filename.startswith('~$') or filename.startswith('.'):
            continue
        # Skip files that look like timestamped results
        if any(x in filename.lower() for x in ['_2025', '_2026', 'results', 'screen_', 'analysis_']):
            continue

        tickers = _extract_tickers_from_excel(filepath)
        if len(tickers) >= 5:
            display_name = _filename_to_display_name(filename)
            indexes.append((f"{display_name} ({len(tickers)})", filepath))
            seen_files.add(filename)

    return indexes


@st.cache_data(ttl=3600)
def get_index_tickers(file_path: str) -> list:
    """Get tickers from an index file path."""
    if file_path is None:
        return []
    return _extract_tickers_from_excel(file_path)


@st.cache_data(ttl=3600)
def compare_estimates_between_dates_filtered(date1: str, date2: str, index_tickers: list = None) -> pd.DataFrame:
    """
    Compare EPS estimates between two snapshot dates, optionally filtered by index.
    """
    if not db_exists():
        return pd.DataFrame()

    try:
        conn = get_db_connection()
        placeholder = '%s' if USE_POSTGRES else '?'
        # PostgreSQL needs CAST to numeric for ROUND with decimal places
        round_cast = 'CAST(' if USE_POSTGRES else ''
        round_cast_end = ' AS numeric)' if USE_POSTGRES else ''

        # Build ticker filter clause
        ticker_filter = ""
        if index_tickers and len(index_tickers) > 0:
            placeholders = ','.join([placeholder for _ in index_tickers])
            ticker_filter = f"AND n.ticker IN ({placeholders})"

        query = f'''
        WITH old_estimates AS (
            SELECT ticker, fiscal_period, eps_avg as old_eps, revenue_avg as old_rev
            FROM estimate_snapshots
            WHERE snapshot_date = {placeholder}
        ),
        new_estimates AS (
            SELECT ticker, fiscal_period, eps_avg as new_eps, revenue_avg as new_rev
            FROM estimate_snapshots
            WHERE snapshot_date = {placeholder}
        )
        SELECT
            n.ticker,
            n.fiscal_period,
            o.old_eps,
            n.new_eps,
            CASE WHEN o.old_eps != 0 AND o.old_eps IS NOT NULL
                 THEN ROUND({round_cast}((n.new_eps - o.old_eps) / ABS(o.old_eps)) * 100{round_cast_end}, 2)
                 ELSE NULL END as eps_revision_pct,
            o.old_rev / 1000000 as old_rev_M,
            n.new_rev / 1000000 as new_rev_M,
            CASE WHEN o.old_rev != 0 AND o.old_rev IS NOT NULL
                 THEN ROUND({round_cast}((n.new_rev - o.old_rev) / ABS(o.old_rev)) * 100{round_cast_end}, 2)
                 ELSE NULL END as rev_revision_pct
        FROM new_estimates n
        JOIN old_estimates o ON n.ticker = o.ticker AND n.fiscal_period = o.fiscal_period
        WHERE n.new_eps IS NOT NULL AND o.old_eps IS NOT NULL
        {ticker_filter}
        ORDER BY n.fiscal_period, eps_revision_pct DESC
        '''

        params = [date1, date2]
        if index_tickers and len(index_tickers) > 0:
            params.extend(index_tickers)

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        if len(df) == 0:
            return pd.DataFrame()

        # Get FY1 only (first fiscal period for each ticker)
        fy1 = df.groupby('ticker').first().reset_index()
        fy1 = fy1.sort_values('eps_revision_pct', ascending=False)

        return fy1

    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def get_ticker_sector_map() -> dict:
    """Get mapping of ticker to sector from Broad US Index file."""
    try:
        df = pd.read_excel('Index_Broad_US.xlsx')
        if 'Ticker' in df.columns and 'Sector' in df.columns:
            return dict(zip(df['Ticker'].str.upper(), df['Sector']))
    except:
        pass
    return {}


@st.cache_data(ttl=3600)
def get_sector_revision_summary(date1: str, date2: str, index_tickers: tuple = None) -> pd.DataFrame:
    """
    Get average EPS revision by sector between two dates.
    Returns DataFrame with sector, avg_revision, positive_count, negative_count, total_count.
    index_tickers should be a tuple (for caching) or None for all stocks.
    """
    # Convert tuple back to list for filtering
    tickers_list = list(index_tickers) if index_tickers else None
    comparison_df = compare_estimates_between_dates_filtered(date1, date2, tickers_list)
    if len(comparison_df) == 0:
        return pd.DataFrame()

    sector_map = get_ticker_sector_map()
    if not sector_map:
        return pd.DataFrame()

    # Add sector to comparison data
    comparison_df['sector'] = comparison_df['ticker'].map(sector_map)
    comparison_df = comparison_df[comparison_df['sector'].notna()]

    if len(comparison_df) == 0:
        return pd.DataFrame()

    # Aggregate by sector
    sector_summary = comparison_df.groupby('sector').agg({
        'eps_revision_pct': ['mean', 'median', 'count'],
        'ticker': lambda x: (comparison_df.loc[x.index, 'eps_revision_pct'] > 0).sum()
    }).reset_index()

    sector_summary.columns = ['Sector', 'Avg EPS Rev %', 'Median EPS Rev %', 'Total Stocks', 'Positive Count']
    sector_summary['Negative Count'] = sector_summary['Total Stocks'] - sector_summary['Positive Count']
    sector_summary['% Positive'] = (sector_summary['Positive Count'] / sector_summary['Total Stocks'] * 100).round(1)
    sector_summary = sector_summary.sort_values('Avg EPS Rev %', ascending=False)

    return sector_summary


@st.cache_data(ttl=3600)
def compare_estimates_between_dates(date1: str, date2: str) -> pd.DataFrame:
    """
    Compare EPS estimates between two snapshot dates.
    Returns DataFrame with ticker, old EPS, new EPS, revision %, sorted by revision.
    """
    if not db_exists():
        return pd.DataFrame()

    try:
        conn = get_db_connection()
        placeholder = '%s' if USE_POSTGRES else '?'
        # PostgreSQL needs CAST to numeric for ROUND with decimal places
        round_cast = 'CAST(' if USE_POSTGRES else ''
        round_cast_end = ' AS numeric)' if USE_POSTGRES else ''

        query = f'''
        WITH old_estimates AS (
            SELECT ticker, fiscal_period, eps_avg as old_eps, revenue_avg as old_rev
            FROM estimate_snapshots
            WHERE snapshot_date = {placeholder}
        ),
        new_estimates AS (
            SELECT ticker, fiscal_period, eps_avg as new_eps, revenue_avg as new_rev
            FROM estimate_snapshots
            WHERE snapshot_date = {placeholder}
        )
        SELECT
            n.ticker,
            n.fiscal_period,
            o.old_eps,
            n.new_eps,
            CASE WHEN o.old_eps != 0 AND o.old_eps IS NOT NULL
                 THEN ROUND({round_cast}((n.new_eps - o.old_eps) / ABS(o.old_eps)) * 100{round_cast_end}, 2)
                 ELSE NULL END as eps_revision_pct,
            o.old_rev / 1000000 as old_rev_M,
            n.new_rev / 1000000 as new_rev_M,
            CASE WHEN o.old_rev != 0 AND o.old_rev IS NOT NULL
                 THEN ROUND({round_cast}((n.new_rev - o.old_rev) / ABS(o.old_rev)) * 100{round_cast_end}, 2)
                 ELSE NULL END as rev_revision_pct
        FROM new_estimates n
        JOIN old_estimates o ON n.ticker = o.ticker AND n.fiscal_period = o.fiscal_period
        WHERE n.new_eps IS NOT NULL AND o.old_eps IS NOT NULL
        ORDER BY n.fiscal_period, eps_revision_pct DESC
        '''

        df = pd.read_sql_query(query, conn, params=[date1, date2])
        conn.close()

        if len(df) == 0:
            return pd.DataFrame()

        # Get FY1 only (first fiscal period for each ticker)
        fy1 = df.groupby('ticker').first().reset_index()
        fy1 = fy1.sort_values('eps_revision_pct', ascending=False)

        return fy1

    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def screen_positive_revision_trends(min_days: int = 7) -> pd.DataFrame:
    """
    Screen all tracked tickers for positive EPS revision trends.
    Returns DataFrame with tickers where FY1, FY2, FY3 estimates are all trending up.
    """
    if not db_exists():
        return pd.DataFrame()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all unique tickers
        cursor.execute("SELECT DISTINCT ticker FROM estimate_snapshots")
        tickers = [row[0] for row in cursor.fetchall()]

        # Get date range
        cursor.execute("SELECT MIN(snapshot_date), MAX(snapshot_date) FROM estimate_snapshots")
        date_range = cursor.fetchone()
        conn.close()

        if not date_range or not date_range[0] or not date_range[1]:
            return pd.DataFrame()

        min_date, max_date = date_range
        from datetime import datetime, date

        # Handle both date objects (PostgreSQL) and strings (SQLite)
        if isinstance(min_date, date):
            min_dt = datetime.combine(min_date, datetime.min.time())
        else:
            min_dt = datetime.strptime(str(min_date), '%Y-%m-%d')

        if isinstance(max_date, date):
            max_dt = datetime.combine(max_date, datetime.min.time())
        else:
            max_dt = datetime.strptime(str(max_date), '%Y-%m-%d')

        days_of_data = (max_dt - min_dt).days

        if days_of_data < min_days:
            return pd.DataFrame()

        results = []

        for ticker in tickers:
            hist = get_eps_revision_history(ticker)
            if hist is None or len(hist) < 2:
                continue

            # Calculate revision trend for each FY
            row = {'ticker': ticker}
            all_positive = True

            for col in ['FY1_EPS', 'FY2_EPS', 'FY3_EPS']:
                if col not in hist.columns:
                    continue

                # Get first and last non-null values
                series = hist[col].dropna()
                if len(series) < 2:
                    continue

                first_val = series.iloc[0]
                last_val = series.iloc[-1]

                if first_val and first_val != 0:
                    revision_pct = ((last_val - first_val) / abs(first_val)) * 100
                    row[f'{col}_first'] = first_val
                    row[f'{col}_last'] = last_val
                    row[f'{col}_rev_pct'] = revision_pct

                    if revision_pct <= 0:
                        all_positive = False
                else:
                    all_positive = False

            row['all_fy_positive'] = all_positive
            row['days_tracked'] = days_of_data

            # Only include if we have at least FY1 data
            if 'FY1_EPS_rev_pct' in row:
                results.append(row)

        df = pd.DataFrame(results)
        if len(df) > 0:
            df = df.sort_values('FY1_EPS_rev_pct', ascending=False)

        return df

    except Exception as e:
        return pd.DataFrame()


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
        ["Master Universe", "S&P 500", "S&P 500 by Sector", "Disruption Index", "Broad US Index", "Broad US by Sector"],
        help="Master Universe = centralized ticker source (US + International)"
    )

    num_stocks = None
    selected_sectors = None

    if scan_mode == "Master Universe":
        # Master Universe scan options
        master_options = {
            "Quick Test (50 stocks)": 50,
            "Medium Scan (200 stocks)": 200,
            "Large Scan (500 stocks)": 500,
            "Full Master Universe": None
        }

        scan_choice = st.sidebar.selectbox(
            "Select scan size:",
            list(master_options.keys())
        )

        num_stocks = master_options[scan_choice]

        # Show info about Master Universe
        try:
            master_tickers = EarningsRevisionRanker.get_master_universe_tickers()
            intl_count = sum(1 for t in master_tickers if '.' in t)
            st.sidebar.info(f"📊 {len(master_tickers)} total tickers ({intl_count} international)")
        except:
            st.sidebar.info("📊 Master Universe (US + International stocks)")

    elif scan_mode == "S&P 500":
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
        if scan_mode == "Master Universe":
            scan_description = f"Master Universe - {scan_choice}"
        elif scan_mode == "Disruption Index":
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
            if scan_mode == "Master Universe":
                st.session_state['df'] = load_master_universe_data(num_stocks, max_workers)
            elif scan_mode == "Disruption Index":
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

    # Main content - Always show EPS Revision Trends tab, other tabs need scan data
    if 'df' not in st.session_state:
        # Show two tabs: Instructions and EPS Revision Trends (always available)
        intro_tab, revision_trends_tab = st.tabs(["📖 Getting Started", "📉 EPS Revision Trends"])

        with intro_tab:
            st.info("👈 Click 'Run Scan' in the sidebar to start analyzing earnings revisions")

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

        with revision_trends_tab:
            # EPS Revision Trends - Available without running a scan
            st.subheader("📉 EPS Revision Trend Charts")

            tracker_status_standalone = get_estimates_tracker_status()

            if tracker_status_standalone is None or tracker_status_standalone.get('days_of_data', 0) < 2:
                st.warning("Not enough historical data yet. Need at least 2 days of estimate snapshots.")
                st.info("Estimates are captured daily via GitHub Actions. Check back after data collection.")
            else:
                st.success(f"**{tracker_status_standalone['days_of_data']} days** of estimate data available ({tracker_status_standalone['ticker_count']} tickers)")

                available_dates_standalone = tracker_status_standalone.get('dates', [])

                # Index filter at the top
                st.markdown("---")
                available_indexes = get_available_indexes()
                index_display_names = [name for name, _ in available_indexes]

                selected_idx = st.selectbox(
                    "🎯 Filter by Index:",
                    range(len(index_display_names)),
                    format_func=lambda i: index_display_names[i],
                    index=0,
                    key="index_filter_standalone",
                    help="Filter stocks by index membership. Any Excel file with a Ticker/Symbol column is auto-detected."
                )

                selected_name, selected_file = available_indexes[selected_idx]

                # Get tickers for selected index
                if selected_file is None:
                    index_tickers_list = None
                    st.caption("Showing all tracked stocks")
                else:
                    index_tickers_list = get_index_tickers(selected_file)
                    if index_tickers_list:
                        st.caption(f"Filtering to **{len(index_tickers_list)}** stocks in {selected_name}")
                    else:
                        st.warning(f"Could not load tickers from {selected_name}. Showing all stocks.")
                        index_tickers_list = None

                st.markdown("---")

                # Create sub-tabs for different views
                stock_tab, sector_tab, compare_tab = st.tabs(["📈 Individual Stock", "🏢 By Sector", "📊 Date Comparison"])

                # ==================== INDIVIDUAL STOCK TAB ====================
                with stock_tab:
                    st.markdown("### Individual Stock EPS Revision Chart")
                    st.caption("View how FY1, FY2, FY3 estimates have changed over time for any stock")

                    chart_ticker_input = st.text_input(
                        "Enter ticker symbol:",
                        "",
                        key="individual_stock_chart_input",
                        placeholder="e.g., NVDA, AAPL, MSFT"
                    ).upper()

                    if chart_ticker_input:
                        with st.spinner(f"Loading revision history for {chart_ticker_input}..."):
                            hist_df = get_eps_revision_history(chart_ticker_input)

                        if hist_df is not None and len(hist_df) >= 2:
                            st.success(f"Found {len(hist_df)} data points for {chart_ticker_input}")

                            # Create and display the chart
                            fig = create_eps_revision_chart(chart_ticker_input, hist_df)
                            st.plotly_chart(fig, use_container_width=True)

                            # Show revision summary metrics
                            st.markdown("#### Revision Summary")
                            col1, col2, col3 = st.columns(3)

                            for i, (col, fy_col) in enumerate(zip([col1, col2, col3], ['FY1_EPS', 'FY2_EPS', 'FY3_EPS'])):
                                if fy_col in hist_df.columns:
                                    series = hist_df[fy_col].dropna()
                                    if len(series) >= 2:
                                        first_val = series.iloc[0]
                                        last_val = series.iloc[-1]
                                        if first_val and first_val != 0:
                                            rev_pct = ((last_val - first_val) / abs(first_val)) * 100
                                            with col:
                                                st.metric(
                                                    f"FY{i+1} EPS",
                                                    f"${last_val:.2f}",
                                                    f"{rev_pct:+.2f}%"
                                                )

                            # Show data table
                            with st.expander("View Raw Data"):
                                display_cols = ['snapshot_date']
                                for col in ['FY1_EPS', 'FY2_EPS', 'FY3_EPS']:
                                    if col in hist_df.columns:
                                        display_cols.append(col)
                                st.dataframe(hist_df[display_cols], use_container_width=True)

                        elif hist_df is not None and len(hist_df) == 1:
                            st.warning(f"Only 1 data point for {chart_ticker_input}. Need at least 2 days of data to show trends.")
                        else:
                            st.warning(f"No revision history found for {chart_ticker_input}. It may not be in the tracked universe.")

                # ==================== SECTOR TAB ====================
                with sector_tab:
                    st.markdown("### EPS Revisions by Sector")
                    st.caption(f"See which sectors have the strongest/weakest estimate revisions {f'in {selected_name}' if selected_file else ''}")

                    if len(available_dates_standalone) >= 2:
                        col1, col2 = st.columns(2)

                        with col1:
                            sector_old_date = st.selectbox(
                                "From Date:",
                                available_dates_standalone[::-1],
                                index=0,
                                key="sector_old_date"
                            )

                        with col2:
                            sector_new_date = st.selectbox(
                                "To Date:",
                                available_dates_standalone,
                                index=0,
                                key="sector_new_date"
                            )

                        if st.button("📊 Analyze Sectors", type="primary", key="sector_analysis_btn"):
                            with st.spinner("Analyzing sector revisions..."):
                                # Convert to tuple for caching
                                tickers_tuple = tuple(index_tickers_list) if index_tickers_list else None
                                sector_df = get_sector_revision_summary(sector_old_date, sector_new_date, tickers_tuple)

                            if len(sector_df) > 0:
                                # Create horizontal bar chart
                                fig_sector = go.Figure()

                                # Color bars based on positive/negative
                                colors = ['#2ca02c' if x > 0 else '#d62728' for x in sector_df['Avg EPS Rev %']]

                                fig_sector.add_trace(go.Bar(
                                    y=sector_df['Sector'],
                                    x=sector_df['Avg EPS Rev %'],
                                    orientation='h',
                                    marker_color=colors,
                                    text=[f"{x:+.2f}%" for x in sector_df['Avg EPS Rev %']],
                                    textposition='outside'
                                ))

                                fig_sector.update_layout(
                                    title=f"Average EPS Revision by Sector ({sector_old_date} to {sector_new_date})",
                                    xaxis_title="Avg EPS Revision %",
                                    yaxis_title="",
                                    height=500,
                                    yaxis={'categoryorder': 'total ascending'},
                                    showlegend=False
                                )

                                st.plotly_chart(fig_sector, use_container_width=True)

                                # Show sector table
                                st.markdown("#### Sector Details")
                                display_sector_df = sector_df[['Sector', 'Avg EPS Rev %', 'Median EPS Rev %', 'Positive Count', 'Negative Count', 'Total Stocks', '% Positive']].copy()

                                def highlight_sector_rev(val):
                                    try:
                                        if float(val) > 0:
                                            return 'background-color: #90EE90; color: black'
                                        elif float(val) < 0:
                                            return 'background-color: #FFB6C6; color: black'
                                    except:
                                        pass
                                    return ''

                                styled_sector = display_sector_df.style.applymap(
                                    highlight_sector_rev,
                                    subset=['Avg EPS Rev %', 'Median EPS Rev %']
                                ).format({
                                    'Avg EPS Rev %': '{:+.2f}%',
                                    'Median EPS Rev %': '{:+.2f}%',
                                    '% Positive': '{:.1f}%'
                                }, na_rep='N/A')

                                st.dataframe(styled_sector, use_container_width=True, hide_index=True)

                                # Download sector data
                                csv_sector = sector_df.to_csv(index=False)
                                st.download_button(
                                    "📥 Download Sector Data CSV",
                                    csv_sector,
                                    file_name=f"sector_revisions_{sector_old_date}_to_{sector_new_date}.csv",
                                    mime="text/csv",
                                    key="download_sector"
                                )
                            else:
                                st.warning("Could not generate sector analysis. Sector data may not be available.")

                # ==================== DATE COMPARISON TAB ====================
                with compare_tab:
                    st.markdown("### Compare Estimates Between Dates")
                    st.caption(f"See which stocks have the biggest estimate revisions {f'in {selected_name}' if selected_file else ''}")

                    if len(available_dates_standalone) >= 2:
                        col1, col2 = st.columns(2)

                        with col1:
                            old_date_s = st.selectbox(
                                "From Date (older):",
                                available_dates_standalone[::-1],
                                index=0,
                                key="compare_old_date_standalone"
                            )

                        with col2:
                            new_date_s = st.selectbox(
                                "To Date (newer):",
                                available_dates_standalone,
                                index=0,
                                key="compare_new_date_standalone"
                            )

                        if st.button("🔍 Compare Estimates", type="primary", key="compare_dates_btn_standalone"):
                            with st.spinner(f"Comparing estimates from {old_date_s} to {new_date_s}..."):
                                comparison_df_s = compare_estimates_between_dates_filtered(old_date_s, new_date_s, index_tickers_list)

                            if len(comparison_df_s) > 0:
                                # Store in session state for charts
                                st.session_state['comparison_data'] = comparison_df_s
                                st.session_state['comparison_dates'] = (old_date_s, new_date_s)

                                positive_revs_s = len(comparison_df_s[comparison_df_s['eps_revision_pct'] > 0])
                                negative_revs_s = len(comparison_df_s[comparison_df_s['eps_revision_pct'] < 0])

                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("Tickers Compared", len(comparison_df_s))
                                with col2:
                                    st.metric("Positive Revisions", positive_revs_s, f"{positive_revs_s/len(comparison_df_s)*100:.1f}%")
                                with col3:
                                    st.metric("Negative Revisions", negative_revs_s, f"{negative_revs_s/len(comparison_df_s)*100:.1f}%")

                                # Distribution chart
                                st.markdown("#### Revision Distribution")
                                fig_dist = px.histogram(
                                    comparison_df_s,
                                    x='eps_revision_pct',
                                    nbins=50,
                                    title='Distribution of EPS Revisions',
                                    labels={'eps_revision_pct': 'EPS Revision %'},
                                    color_discrete_sequence=['#1f77b4']
                                )
                                fig_dist.add_vline(x=0, line_dash="dash", line_color="red")
                                fig_dist.update_layout(height=300)
                                st.plotly_chart(fig_dist, use_container_width=True)

                                # Top gainers and decliners side by side
                                col1, col2 = st.columns(2)

                                with col1:
                                    st.markdown("#### 🚀 Top 25 Positive Revisions")
                                    top_gainers_s = comparison_df_s[comparison_df_s['eps_revision_pct'] > 0].head(25).copy()

                                    if len(top_gainers_s) > 0:
                                        display_gainers_s = top_gainers_s[['ticker', 'old_eps', 'new_eps', 'eps_revision_pct']].copy()
                                        display_gainers_s.columns = ['Ticker', 'Old EPS', 'New EPS', 'Rev %']
                                        st.dataframe(display_gainers_s.style.format({
                                            'Old EPS': '${:.2f}',
                                            'New EPS': '${:.2f}',
                                            'Rev %': '{:+.2f}%'
                                        }, na_rep='N/A'), use_container_width=True, hide_index=True, height=400)
                                    else:
                                        st.info("No positive revisions found.")

                                with col2:
                                    st.markdown("#### 📉 Top 25 Negative Revisions")
                                    top_decliners_s = comparison_df_s[comparison_df_s['eps_revision_pct'] < 0].sort_values('eps_revision_pct').head(25).copy()

                                    if len(top_decliners_s) > 0:
                                        display_decliners_s = top_decliners_s[['ticker', 'old_eps', 'new_eps', 'eps_revision_pct']].copy()
                                        display_decliners_s.columns = ['Ticker', 'Old EPS', 'New EPS', 'Rev %']
                                        st.dataframe(display_decliners_s.style.format({
                                            'Old EPS': '${:.2f}',
                                            'New EPS': '${:.2f}',
                                            'Rev %': '{:+.2f}%'
                                        }, na_rep='N/A'), use_container_width=True, hide_index=True, height=400)
                                    else:
                                        st.info("No negative revisions found.")

                                csv_compare_s = comparison_df_s.to_csv(index=False)
                                st.download_button(
                                    "📥 Download Full Comparison CSV",
                                    csv_compare_s,
                                    file_name=f"eps_comparison_{old_date_s}_to_{new_date_s}.csv",
                                    mime="text/csv",
                                    key="download_standalone"
                                )
                            else:
                                st.warning("No matching data found between these dates.")

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
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🏆 Rankings", "📊 Charts", "📈 Analysis", "📋 Raw Data", "📅 Revision Tracker", "📉 EPS Revision Trends"])

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

                    # Charts for FY estimates
                    st.markdown("#### Estimates Trend")

                    chart_col1, chart_col2 = st.columns(2)

                    with chart_col1:
                        # EPS Chart
                        eps_data = []
                        for est in fy_data['estimates']:
                            if est['eps_avg']:
                                eps_data.append({
                                    'Fiscal Year': est['fiscal_year'],
                                    'EPS Low': est['eps_low'],
                                    'EPS Avg': est['eps_avg'],
                                    'EPS High': est['eps_high']
                                })

                        if eps_data:
                            eps_df = pd.DataFrame(eps_data)

                            # Create bar chart with error bars
                            fig_eps = go.Figure()

                            fig_eps.add_trace(go.Bar(
                                x=eps_df['Fiscal Year'],
                                y=eps_df['EPS Avg'],
                                name='EPS Estimate',
                                marker_color='#1f77b4',
                                error_y=dict(
                                    type='data',
                                    symmetric=False,
                                    array=eps_df['EPS High'] - eps_df['EPS Avg'],
                                    arrayminus=eps_df['EPS Avg'] - eps_df['EPS Low'],
                                    color='#666'
                                ),
                                text=[f"${v:.2f}" for v in eps_df['EPS Avg']],
                                textposition='outside'
                            ))

                            fig_eps.update_layout(
                                title=f"{fy_ticker} EPS Estimates",
                                xaxis_title="Fiscal Year",
                                yaxis_title="EPS ($)",
                                showlegend=False,
                                height=350
                            )

                            st.plotly_chart(fig_eps, use_container_width=True)

                    with chart_col2:
                        # Revenue Chart
                        rev_data = []
                        for est in fy_data['estimates']:
                            if est['revenue_avg']:
                                rev_data.append({
                                    'Fiscal Year': est['fiscal_year'],
                                    'Rev Low': est['revenue_low'] / 1e9,
                                    'Rev Avg': est['revenue_avg'] / 1e9,
                                    'Rev High': est['revenue_high'] / 1e9
                                })

                        if rev_data:
                            rev_df = pd.DataFrame(rev_data)

                            fig_rev = go.Figure()

                            fig_rev.add_trace(go.Bar(
                                x=rev_df['Fiscal Year'],
                                y=rev_df['Rev Avg'],
                                name='Revenue Estimate',
                                marker_color='#2ca02c',
                                error_y=dict(
                                    type='data',
                                    symmetric=False,
                                    array=rev_df['Rev High'] - rev_df['Rev Avg'],
                                    arrayminus=rev_df['Rev Avg'] - rev_df['Rev Low'],
                                    color='#666'
                                ),
                                text=[f"${v:.1f}B" for v in rev_df['Rev Avg']],
                                textposition='outside'
                            ))

                            fig_rev.update_layout(
                                title=f"{fy_ticker} Revenue Estimates",
                                xaxis_title="Fiscal Year",
                                yaxis_title="Revenue ($B)",
                                showlegend=False,
                                height=350
                            )

                            st.plotly_chart(fig_rev, use_container_width=True)

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

        with tab6:
            st.subheader("📉 EPS Revision Trend Charts")
            st.markdown("View how FY1, FY2, FY3 earnings estimates have changed over time - like the chart shown for BANC.")

            # Get tracker status first
            tracker_status_t6 = get_estimates_tracker_status()

            if tracker_status_t6 is None or tracker_status_t6.get('days_of_data', 0) < 2:
                st.warning("Not enough historical data yet. Need at least 2 days of estimate snapshots to show trends.")
                st.info("Estimates are captured daily via GitHub Actions. Check back after a few days of data collection.")
            else:
                st.success(f"**{tracker_status_t6['days_of_data']} days** of estimate data available ({tracker_status_t6['ticker_count']} tickers)")

                # Date Comparison Section
                st.markdown("---")
                st.markdown("### 📊 Compare Estimates Between Dates")
                st.caption("See which stocks have the biggest estimate revisions between any two snapshot dates")

                available_dates = tracker_status_t6.get('dates', [])

                if len(available_dates) >= 2:
                    col1, col2 = st.columns(2)

                    with col1:
                        old_date = st.selectbox(
                            "From Date (older):",
                            available_dates[::-1],  # Reverse to show oldest first
                            index=0,
                            key="compare_old_date"
                        )

                    with col2:
                        new_date = st.selectbox(
                            "To Date (newer):",
                            available_dates,  # Newest first
                            index=0,
                            key="compare_new_date"
                        )

                    if st.button("🔍 Compare Estimates", type="primary", key="compare_dates_btn"):
                        with st.spinner(f"Comparing estimates from {old_date} to {new_date}..."):
                            comparison_df = compare_estimates_between_dates(old_date, new_date)

                        if len(comparison_df) > 0:
                            # Summary stats
                            positive_revs = len(comparison_df[comparison_df['eps_revision_pct'] > 0])
                            negative_revs = len(comparison_df[comparison_df['eps_revision_pct'] < 0])

                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Tickers Compared", len(comparison_df))
                            with col2:
                                st.metric("Positive Revisions", positive_revs, f"{positive_revs/len(comparison_df)*100:.1f}%")
                            with col3:
                                st.metric("Negative Revisions", negative_revs, f"{negative_revs/len(comparison_df)*100:.1f}%")

                            # Top gainers
                            st.markdown("#### 🚀 Top 25 Positive EPS Revisions")
                            top_gainers = comparison_df[comparison_df['eps_revision_pct'] > 0].head(25).copy()

                            if len(top_gainers) > 0:
                                display_gainers = top_gainers[['ticker', 'old_eps', 'new_eps', 'eps_revision_pct', 'rev_revision_pct']].copy()
                                display_gainers.columns = ['Ticker', f'EPS ({old_date})', f'EPS ({new_date})', 'EPS Rev %', 'Rev Rev %']

                                def highlight_positive_val(val):
                                    try:
                                        if float(val) > 0:
                                            return 'background-color: #90EE90; color: black'
                                    except:
                                        pass
                                    return ''

                                styled_gainers = display_gainers.style.applymap(
                                    highlight_positive_val,
                                    subset=['EPS Rev %']
                                ).format({
                                    f'EPS ({old_date})': '${:.2f}',
                                    f'EPS ({new_date})': '${:.2f}',
                                    'EPS Rev %': '{:+.2f}%',
                                    'Rev Rev %': '{:+.2f}%'
                                }, na_rep='N/A')

                                st.dataframe(styled_gainers, use_container_width=True, hide_index=True)
                            else:
                                st.info("No positive revisions found in this period.")

                            # Top decliners
                            st.markdown("#### 📉 Top 25 Negative EPS Revisions")
                            top_decliners = comparison_df[comparison_df['eps_revision_pct'] < 0].sort_values('eps_revision_pct').head(25).copy()

                            if len(top_decliners) > 0:
                                display_decliners = top_decliners[['ticker', 'old_eps', 'new_eps', 'eps_revision_pct', 'rev_revision_pct']].copy()
                                display_decliners.columns = ['Ticker', f'EPS ({old_date})', f'EPS ({new_date})', 'EPS Rev %', 'Rev Rev %']

                                def highlight_negative_val(val):
                                    try:
                                        if float(val) < 0:
                                            return 'background-color: #FFB6C6; color: black'
                                    except:
                                        pass
                                    return ''

                                styled_decliners = display_decliners.style.applymap(
                                    highlight_negative_val,
                                    subset=['EPS Rev %']
                                ).format({
                                    f'EPS ({old_date})': '${:.2f}',
                                    f'EPS ({new_date})': '${:.2f}',
                                    'EPS Rev %': '{:+.2f}%',
                                    'Rev Rev %': '{:+.2f}%'
                                }, na_rep='N/A')

                                st.dataframe(styled_decliners, use_container_width=True, hide_index=True)
                            else:
                                st.info("No negative revisions found in this period.")

                            # Download button for full comparison
                            csv_compare = comparison_df.to_csv(index=False)
                            st.download_button(
                                "📥 Download Full Comparison CSV",
                                csv_compare,
                                file_name=f"eps_comparison_{old_date}_to_{new_date}.csv",
                                mime="text/csv"
                            )
                        else:
                            st.warning("No matching data found between these dates.")

                st.markdown("---")

                # Ticker lookup for revision chart
                st.markdown("### 📊 Individual Stock EPS Revision Chart")
                chart_ticker = st.text_input("Enter ticker to view EPS revision history:", "", key="eps_chart_ticker").upper()

                if chart_ticker:
                    with st.spinner(f"Loading revision history for {chart_ticker}..."):
                        hist_df = get_eps_revision_history(chart_ticker)

                    if hist_df is not None and len(hist_df) >= 2:
                        st.success(f"Found {len(hist_df)} data points for {chart_ticker}")

                        # Create and display the chart
                        fig = create_eps_revision_chart(chart_ticker, hist_df)
                        st.plotly_chart(fig, use_container_width=True)

                        # Show revision summary
                        st.markdown("#### Revision Summary")
                        col1, col2, col3 = st.columns(3)

                        for i, (col, fy_col) in enumerate(zip([col1, col2, col3], ['FY1_EPS', 'FY2_EPS', 'FY3_EPS'])):
                            if fy_col in hist_df.columns:
                                series = hist_df[fy_col].dropna()
                                if len(series) >= 2:
                                    first_val = series.iloc[0]
                                    last_val = series.iloc[-1]
                                    if first_val and first_val != 0:
                                        rev_pct = ((last_val - first_val) / abs(first_val)) * 100
                                        with col:
                                            st.metric(
                                                f"FY{i+1} EPS",
                                                f"${last_val:.2f}",
                                                f"{rev_pct:+.2f}%"
                                            )

                        # Show data table
                        with st.expander("View Raw Data"):
                            display_cols = ['snapshot_date']
                            for col in ['FY1_EPS', 'FY2_EPS', 'FY3_EPS']:
                                if col in hist_df.columns:
                                    display_cols.append(col)
                            st.dataframe(hist_df[display_cols], use_container_width=True)

                    elif hist_df is not None and len(hist_df) == 1:
                        st.warning(f"Only 1 data point for {chart_ticker}. Need at least 2 days of data to show trends.")
                    else:
                        st.warning(f"No revision history found for {chart_ticker}. It may not be in the tracked universe.")

                st.markdown("---")

                # Positive Revision Screener
                st.markdown("### 🔍 Positive Revision Trend Screener")
                st.caption("Find stocks where FY1, FY2, FY3 estimates are ALL trending upward")

                if st.button("🚀 Screen for Positive Trends", type="primary", key="screen_positive"):
                    with st.spinner("Screening all tracked tickers for positive revision trends..."):
                        screen_df = screen_positive_revision_trends(min_days=2)

                    if len(screen_df) > 0:
                        # Filter for all positive
                        all_positive_df = screen_df[screen_df['all_fy_positive'] == True].copy()

                        st.success(f"Found **{len(all_positive_df)}** stocks with ALL FY estimates trending up (out of {len(screen_df)} with data)")

                        if len(all_positive_df) > 0:
                            st.markdown("#### 🌟 Stocks with All Positive Revisions")

                            # Format display
                            display_screen_df = all_positive_df[['ticker', 'FY1_EPS_rev_pct', 'FY2_EPS_rev_pct', 'FY3_EPS_rev_pct']].copy()
                            display_screen_df.columns = ['Ticker', 'FY1 Rev %', 'FY2 Rev %', 'FY3 Rev %']

                            # Style with green for positive
                            def highlight_positive_rev(val):
                                try:
                                    if float(val) > 0:
                                        return 'background-color: #90EE90; color: black'
                                except:
                                    pass
                                return ''

                            styled_screen = display_screen_df.style.applymap(
                                highlight_positive_rev,
                                subset=['FY1 Rev %', 'FY2 Rev %', 'FY3 Rev %']
                            ).format({
                                'FY1 Rev %': '{:.2f}%',
                                'FY2 Rev %': '{:.2f}%',
                                'FY3 Rev %': '{:.2f}%'
                            }, na_rep='N/A')

                            st.dataframe(styled_screen, use_container_width=True, hide_index=True)

                            # Download button
                            csv_data = all_positive_df.to_csv(index=False)
                            st.download_button(
                                "📥 Download Positive Revision Stocks",
                                csv_data,
                                file_name=f"positive_revision_stocks_{datetime.now().strftime('%Y%m%d')}.csv",
                                mime="text/csv"
                            )
                        else:
                            st.info("No stocks found with ALL FY estimates trending up. Try again after more data is collected.")

                        # Also show top movers by FY1
                        st.markdown("---")
                        st.markdown("#### 📈 Top FY1 EPS Revision Gainers")
                        top_fy1 = screen_df.nlargest(20, 'FY1_EPS_rev_pct')[['ticker', 'FY1_EPS_first', 'FY1_EPS_last', 'FY1_EPS_rev_pct']].copy()
                        top_fy1.columns = ['Ticker', 'FY1 EPS (Start)', 'FY1 EPS (Now)', 'Revision %']
                        st.dataframe(
                            top_fy1.style.format({
                                'FY1 EPS (Start)': '${:.2f}',
                                'FY1 EPS (Now)': '${:.2f}',
                                'Revision %': '{:.2f}%'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.warning("Not enough data to screen for trends. Need at least 2 days of snapshots.")

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
