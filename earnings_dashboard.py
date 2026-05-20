"""
Earnings Revision Dashboard
Interactive Streamlit dashboard for viewing S&P 500 earnings revision rankings
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
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


@st.cache_data(ttl=3600)
def get_fy_revisions_table(ticker: str, days_list: tuple = (30, 60, 90)) -> Optional[pd.DataFrame]:
    """EPS revision % per future FY (FY1/FY2/FY3) across multiple lookback windows.

    Compares the SAME fiscal_period across time so fiscal-year rollover is handled correctly.
    """
    if not db_exists():
        return None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        placeholder = '%s' if USE_POSTGRES else '?'
        today_str = datetime.now().strftime('%Y-%m-%d')

        cursor.execute(f"""
            SELECT DISTINCT fiscal_period FROM estimate_snapshots
            WHERE ticker = {placeholder}
              AND (period_type = 'annual' OR period_type IS NULL)
              AND fiscal_period >= {placeholder}
            ORDER BY fiscal_period ASC
            LIMIT 3
        """, (ticker.upper(), today_str))
        periods = [str(row[0]) for row in cursor.fetchall()]

        if not periods:
            conn.close()
            return None

        rows = []
        for i, period in enumerate(periods, 1):
            cursor.execute(f"""
                SELECT eps_avg FROM estimate_snapshots
                WHERE ticker = {placeholder} AND fiscal_period = {placeholder}
                  AND eps_avg IS NOT NULL
                ORDER BY snapshot_date DESC LIMIT 1
            """, (ticker.upper(), period))
            cur = cursor.fetchone()
            if not cur:
                continue
            current_eps = cur[0]

            row = {'FY': f'FY{i}', 'Fiscal Period': period, 'Current EPS': current_eps}

            for days in days_list:
                past_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
                cursor.execute(f"""
                    SELECT eps_avg FROM estimate_snapshots
                    WHERE ticker = {placeholder} AND fiscal_period = {placeholder}
                      AND snapshot_date <= {placeholder}
                      AND eps_avg IS NOT NULL
                    ORDER BY snapshot_date DESC LIMIT 1
                """, (ticker.upper(), period, past_date))
                past = cursor.fetchone()
                if past and past[0] not in (None, 0) and current_eps is not None:
                    row[f'{days}d Rev %'] = ((current_eps - past[0]) / abs(past[0])) * 100
                else:
                    row[f'{days}d Rev %'] = None
            rows.append(row)

        conn.close()
        return pd.DataFrame(rows) if rows else None
    except Exception:
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

    # Add Master Universe (includes international stocks)
    if os.path.exists(MASTER_UNIVERSE_PATH):
        try:
            # Master universe CSV has no header: Ticker, Name, Exchange
            master_df = pd.read_csv(MASTER_UNIVERSE_PATH, header=None, names=['Ticker', 'Name', 'Exchange'])
            master_tickers = master_df['Ticker'].dropna().tolist()
            # International stocks use space + 2-letter exchange code (e.g., "ASML NA", "NESN SE")
            import re
            intl_pattern = re.compile(r' [A-Z]{2}$')
            intl_count = sum(1 for t in master_tickers if intl_pattern.search(str(t)))
            indexes.append((f"Master Universe ({len(master_tickers)} tickers, {intl_count} intl)", MASTER_UNIVERSE_PATH))
        except Exception:
            pass

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
    """Get tickers from an index file path (supports .xlsx and .csv)."""
    if file_path is None:
        return []

    # Handle CSV files (like master_universe.csv)
    if file_path.endswith('.csv'):
        try:
            # First try reading with header
            df = pd.read_csv(file_path)
            if 'Ticker' in df.columns:
                return df['Ticker'].dropna().astype(str).str.upper().str.strip().tolist()
            elif 'Symbol' in df.columns:
                return df['Symbol'].dropna().astype(str).str.upper().str.strip().tolist()
            else:
                # No header - assume first column is ticker (master_universe.csv format)
                df = pd.read_csv(file_path, header=None)
                return df.iloc[:, 0].dropna().astype(str).str.upper().str.strip().tolist()
        except Exception:
            pass
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
def get_ticker_metadata_map() -> dict:
    """
    Build a ticker -> {name, market_cap, sector, industry} map from local index files.
    SP500_list_with_sectors.xlsx is the only local source with MarketCap; other files
    fill in name/sector/industry for the broader universe.
    """
    meta: dict = {}

    # Broadest coverage for name/sector/industry (no market cap)
    try:
        bu = pd.read_excel('Index_Broad_US.xlsx')
        if 'Ticker' in bu.columns:
            for _, row in bu.iterrows():
                t = str(row['Ticker']).upper().strip()
                if not t or t == 'NAN':
                    continue
                meta[t] = {
                    'name': row.get('Name'),
                    'market_cap': None,
                    'sector': row.get('Sector'),
                    'industry': row.get('Industry'),
                }
    except Exception:
        pass

    # SP500 file has MarketCap — overlay to fill that field and refresh others
    try:
        sp = pd.read_excel('SP500_list_with_sectors.xlsx')
        if 'Symbol' in sp.columns:
            for _, row in sp.iterrows():
                t = str(row['Symbol']).upper().strip()
                if not t or t == 'NAN':
                    continue
                existing = meta.get(t, {})
                meta[t] = {
                    'name': row.get('Name') or existing.get('name'),
                    'market_cap': row.get('MarketCap') if pd.notna(row.get('MarketCap')) else existing.get('market_cap'),
                    'sector': row.get('Sector') or existing.get('sector'),
                    'industry': row.get('Industry') or existing.get('industry'),
                }
    except Exception:
        pass

    # Master universe fills in names for international/extra tickers
    try:
        mu = pd.read_csv(MASTER_UNIVERSE_PATH, header=None, names=['Ticker', 'Name', 'Exchange'])
        for _, row in mu.iterrows():
            raw = str(row['Ticker']).strip()
            if not raw or raw == 'nan':
                continue
            # Also store under the FMP-converted key so it joins with the ranker output
            for key in {raw.upper(), convert_to_fmp_ticker(raw).upper()}:
                existing = meta.get(key, {})
                if not existing.get('name'):
                    existing['name'] = row.get('Name')
                meta[key] = {
                    'name': existing.get('name'),
                    'market_cap': existing.get('market_cap'),
                    'sector': existing.get('sector'),
                    'industry': existing.get('industry'),
                }
    except Exception:
        pass

    return meta


def _format_market_cap(val) -> str:
    """Format market cap as $XB / $XM."""
    if val is None or pd.isna(val):
        return 'N/A'
    try:
        v = float(val)
    except Exception:
        return 'N/A'
    if v >= 1e12:
        return f"${v/1e12:.2f}T"
    if v >= 1e9:
        return f"${v/1e9:.2f}B"
    if v >= 1e6:
        return f"${v/1e6:.2f}M"
    return f"${v:,.0f}"


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
def screen_positive_revision_trends(min_days: int = 7, filter_tickers: tuple = None) -> pd.DataFrame:
    """
    Screen tracked tickers for positive EPS revision trends.
    Returns DataFrame with tickers where FY1, FY2, FY3 estimates are all trending up.

    Args:
        min_days: Minimum days of data required
        filter_tickers: Optional tuple of tickers to filter to (e.g., Master Universe, S&P 500)
    """
    if not db_exists():
        return pd.DataFrame()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all unique tickers from database
        cursor.execute("SELECT DISTINCT ticker FROM estimate_snapshots")
        all_tickers = [row[0] for row in cursor.fetchall()]

        # Filter to specified universe if provided
        if filter_tickers:
            filter_set = set(t.upper() for t in filter_tickers)
            tickers = [t for t in all_tickers if t.upper() in filter_set]
        else:
            tickers = all_tickers

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
        hover_data=['eps_revision_pct', 'revenue_revision_pct'],
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


def render_ticker_lookup_tab():
    """Single-ticker EPS estimate history. Reads from estimates_history.db; no scan required."""
    tracker_status = get_estimates_tracker_status()

    if tracker_status is None:
        st.warning("No estimate snapshots collected yet. Data collection runs daily via GitHub Actions.")
        return
    if 'error' in tracker_status:
        st.error(f"Error reading tracker: {tracker_status['error']}")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Days of Data", tracker_status['days_of_data'])
    with c2:
        st.metric("Tickers Tracked", tracker_status['ticker_count'])
    with c3:
        st.metric("Latest Snapshot", tracker_status['latest_date'] or "N/A")

    if tracker_status.get('days_of_data', 0) < 2:
        st.info("Need at least 2 days of snapshots to show revision trends.")
        return

    st.markdown("---")

    chart_ticker = st.text_input(
        "Enter ticker symbol:",
        "",
        key="lookup_ticker_input",
        placeholder="e.g., NVDA, AAPL, MSFT"
    ).upper()

    if not chart_ticker:
        return

    with st.spinner(f"Loading revision history for {chart_ticker}..."):
        hist_df = get_eps_revision_history(chart_ticker)

    if hist_df is None or len(hist_df) == 0:
        st.warning(f"No revision history found for {chart_ticker}. It may not be in the tracked universe.")
        return
    if len(hist_df) < 2:
        st.warning(f"Only 1 data point for {chart_ticker}. Need at least 2 days to show trends.")
        return

    st.success(f"Found {len(hist_df)} data points for {chart_ticker}")

    st.markdown("#### 30 / 60 / 90 Day EPS Revisions by Fiscal Year")
    rev_table = get_fy_revisions_table(chart_ticker)
    if rev_table is None or rev_table.empty:
        st.info("Not enough snapshot history yet to compute 30/60/90 day revisions for this ticker.")
    else:
        def color_rev(val):
            if pd.isna(val):
                return ''
            try:
                v = float(val)
                if v > 0:
                    return 'background-color: #90EE90'
                if v < 0:
                    return 'background-color: #FFB6C6'
            except Exception:
                pass
            return ''

        rev_cols = [c for c in rev_table.columns if c.endswith('Rev %')]
        fmt = {c: '{:+.2f}%' for c in rev_cols}
        fmt['Current EPS'] = '${:.2f}'
        styled = rev_table.style.map(color_rev, subset=rev_cols).format(fmt, na_rep='N/A')
        st.dataframe(styled, use_container_width=True, hide_index=True)

    st.plotly_chart(create_eps_revision_chart(chart_ticker, hist_df), use_container_width=True)

    st.markdown("#### Revision Summary")
    cols = st.columns(3)
    for i, (col, fy_col) in enumerate(zip(cols, ['FY1_EPS', 'FY2_EPS', 'FY3_EPS'])):
        if fy_col in hist_df.columns:
            series = hist_df[fy_col].dropna()
            if len(series) >= 2:
                first_val = series.iloc[0]
                last_val = series.iloc[-1]
                if first_val and first_val != 0:
                    rev_pct = ((last_val - first_val) / abs(first_val)) * 100
                    with col:
                        st.metric(f"FY{i+1} EPS", f"${last_val:.2f}", f"{rev_pct:+.2f}%")

    with st.expander("View Raw Data"):
        display_cols = ['snapshot_date']
        for col in ['FY1_EPS', 'FY2_EPS', 'FY3_EPS']:
            if col in hist_df.columns:
                display_cols.append(col)
        st.dataframe(hist_df[display_cols], use_container_width=True)


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
            import re
            master_tickers = EarningsRevisionRanker.get_master_universe_tickers()
            # International stocks use space + 2-letter exchange code (e.g., "ASML NA", "NESN SE")
            intl_pattern = re.compile(r' [A-Z]{2}$')
            intl_count = sum(1 for t in master_tickers if intl_pattern.search(str(t)))
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
    show_all_stocks = st.sidebar.checkbox("Show ALL companies", value=False, help="Override the top-N limit and display every scanned company.")
    min_score = st.sidebar.slider("Minimum revision score:", -50, 50, 0)
    show_all_columns = st.sidebar.checkbox("Show all columns", value=False, help="Display all available data columns")

    # Beats/Misses filters
    st.sidebar.markdown("---")
    st.sidebar.header("🎯 Earnings Beats/Misses")
    min_beats = st.sidebar.slider("Minimum beats (last 4Q):", 0, 4, 0, help="Filter for stocks with at least N earnings beats")
    show_streaks_only = st.sidebar.checkbox("Show beat streaks only", value=False, help="Only show stocks on a beat streak")

    # Main content - Always show EPS Revision Trends tab, other tabs need scan data
    if 'df' not in st.session_state:
        ranking_tab, lookup_tab = st.tabs(["🏆 Rankings", "🔍 Ticker Lookup"])

        with ranking_tab:
            st.info("👈 Click 'Run Scan' in the sidebar to populate rankings.")
            st.caption("Ranks companies in the chosen universe by EPS or revenue revision % (30-day window).")

        with lookup_tab:
            render_ticker_lookup_tab()


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
            positive_count = int((df['eps_revision_pct'] > 0).sum())
            denom = int(df['eps_revision_pct'].notna().sum()) or len(df)
            st.metric(
                "Positive EPS Revisions",
                positive_count,
                delta=f"{(positive_count/denom*100):.1f}%"
            )

        with col3:
            negative_count = int((df['eps_revision_pct'] < 0).sum())
            st.metric(
                "Negative EPS Revisions",
                negative_count,
                delta=f"{(negative_count/denom*100):.1f}%"
            )

        with col4:
            avg_eps_rev = df['eps_revision_pct'].mean()
            st.metric(
                "Avg EPS Rev %",
                f"{avg_eps_rev:+.2f}%" if pd.notna(avg_eps_rev) else "N/A",
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

        # Two tabs: Rankings + Ticker Lookup
        ranking_tab, lookup_tab = st.tabs(["🏆 Rankings", "🔍 Ticker Lookup"])

        with ranking_tab:
            sort_metric = st.radio(
                "Rank by:",
                ["EPS Revision %", "Revenue Revision %"],
                horizontal=True,
                key="ranking_sort_metric",
            )
            sort_col = "eps_revision_pct" if sort_metric == "EPS Revision %" else "revenue_revision_pct"

            ranked = df_filtered.dropna(subset=[sort_col]).sort_values(sort_col, ascending=False)

            # Enrich with name / market cap / sector / industry from local index files
            meta_map = get_ticker_metadata_map()
            ranked = ranked.copy()
            ranked_tickers_upper = ranked["ticker"].astype(str).str.upper()
            ranked["_meta_name"] = ranked_tickers_upper.map(lambda t: (meta_map.get(t) or {}).get("name"))
            ranked["_meta_market_cap"] = ranked_tickers_upper.map(lambda t: (meta_map.get(t) or {}).get("market_cap"))
            ranked["_meta_sector"] = ranked_tickers_upper.map(lambda t: (meta_map.get(t) or {}).get("sector"))
            ranked["_meta_industry"] = ranked_tickers_upper.map(lambda t: (meta_map.get(t) or {}).get("industry"))
            # Prefer enrichment sector over any sector already on the scan
            if "sector" in ranked.columns:
                ranked["sector"] = ranked["sector"].where(ranked["sector"].notna(), ranked["_meta_sector"])
            else:
                ranked["sector"] = ranked["_meta_sector"]
            ranked["industry"] = ranked.get("industry", pd.Series([None] * len(ranked), index=ranked.index))
            ranked["industry"] = ranked["industry"].where(ranked["industry"].notna(), ranked["_meta_industry"])
            ranked["company_name"] = ranked["_meta_name"]
            ranked["market_cap"] = ranked["_meta_market_cap"]

            total_ranked = len(ranked)
            top_stocks = ranked.copy() if show_all_stocks else ranked.head(show_top_n).copy()

            display_cols = ["ticker", "company_name", "market_cap", "sector", "industry",
                            "eps_revision_pct", "revenue_revision_pct", "current_eps_q1", "price_target_avg"]
            col_names = ["Ticker", "Company Name", "Market Cap", "Sector", "Industry",
                         "EPS Rev %", "Rev Rev %", "EPS Q1 Est", "Price Target"]
            for col, name in [("beats_4q", "Beats (4Q)"), ("misses_4q", "Misses (4Q)"), ("avg_surprise_pct", "Avg Surprise %")]:
                if col in top_stocks.columns:
                    display_cols.append(col)
                    col_names.append(name)

            display_df = top_stocks[display_cols].copy()
            display_df.columns = col_names
            display_df["Market Cap"] = display_df["Market Cap"].apply(_format_market_cap)

            highlight_col = "EPS Rev %" if sort_metric == "EPS Revision %" else "Rev Rev %"

            def highlight_rev(val):
                if pd.isna(val):
                    return ""
                try:
                    v = float(val)
                    if v > 5:
                        return "background-color: #90EE90"
                    elif v > 0:
                        return "background-color: #FFFFE0"
                    elif v < -5:
                        return "background-color: #FFB6C6"
                except Exception:
                    return ""
                return ""

            format_dict = {
                "EPS Rev %": "{:+.2f}",
                "Rev Rev %": "{:+.2f}",
                "Price Target": "{:.2f}",
            }
            if "Avg Surprise %" in col_names:
                format_dict["Avg Surprise %"] = "{:+.2f}"

            styled = display_df.style.map(highlight_rev, subset=[highlight_col]).format(format_dict, na_rep="N/A")

            if show_all_stocks:
                st.markdown(f"### All {len(display_df)} Stocks by {sort_metric} (highest → lowest)")
            else:
                st.markdown(f"### Top {min(show_top_n, total_ranked)} Stocks by {sort_metric} (highest → lowest)")
            st.dataframe(styled, use_container_width=True, height=600)

            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"revisions_by_{sort_col}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

            st.markdown("---")
            st.markdown("### Leaders")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 🚀 Strongest EPS Revisions (↑)")
                eps_pos = df_filtered[df_filtered["eps_revision_pct"] > 0].nlargest(10, "eps_revision_pct")[["ticker", "eps_revision_pct", "current_eps_q1"]]
                eps_pos.columns = ["Ticker", "EPS Rev %", "EPS Q1 Est"]
                if not eps_pos.empty:
                    st.dataframe(eps_pos, use_container_width=True, hide_index=True)
                else:
                    st.info("No positive EPS revisions in current selection.")
            with col2:
                st.markdown("#### 📉 Weakest EPS Revisions (↓)")
                eps_neg = df_filtered[df_filtered["eps_revision_pct"] < 0].nsmallest(10, "eps_revision_pct")[["ticker", "eps_revision_pct", "current_eps_q1"]]
                eps_neg.columns = ["Ticker", "EPS Rev %", "EPS Q1 Est"]
                if not eps_neg.empty:
                    st.dataframe(eps_neg, use_container_width=True, hide_index=True)
                else:
                    st.info("No negative EPS revisions in current selection.")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 💰 Strongest Revenue Revisions (↑)")
                rev_pos = df_filtered[df_filtered["revenue_revision_pct"] > 0].nlargest(10, "revenue_revision_pct")[["ticker", "revenue_revision_pct", "current_revenue_q1"]]
                rev_pos.columns = ["Ticker", "Rev Rev %", "Rev Q1 Est"]
                if not rev_pos.empty:
                    st.dataframe(rev_pos, use_container_width=True, hide_index=True)
                else:
                    st.info("No positive revenue revisions in current selection.")
            with col2:
                st.markdown("#### 🔻 Weakest Revenue Revisions (↓)")
                rev_neg = df_filtered[df_filtered["revenue_revision_pct"] < 0].nsmallest(10, "revenue_revision_pct")[["ticker", "revenue_revision_pct", "current_revenue_q1"]]
                rev_neg.columns = ["Ticker", "Rev Rev %", "Rev Q1 Est"]
                if not rev_neg.empty:
                    st.dataframe(rev_neg, use_container_width=True, hide_index=True)
                else:
                    st.info("No negative revenue revisions in current selection.")

            if "sector" in df_filtered.columns:
                st.markdown("---")
                st.markdown("### 📊 Sectors with the Biggest Changes")

                sector_summary = df_filtered.groupby("sector").agg(
                    avg_eps_rev=("eps_revision_pct", "mean"),
                    avg_rev_rev=("revenue_revision_pct", "mean"),
                    stock_count=("ticker", "count"),
                ).reset_index()
                sector_summary.columns = ["Sector", "Avg EPS Rev %", "Avg Rev Rev %", "Stock Count"]
                sector_summary = sector_summary.dropna(subset=["Avg EPS Rev %"]).sort_values("Avg EPS Rev %", ascending=False)

                def sector_highlight(val):
                    try:
                        v = float(val)
                        if v > 0:
                            return "background-color: #90EE90"
                        elif v < 0:
                            return "background-color: #FFB6C6"
                    except Exception:
                        pass
                    return ""

                fmt = {"Avg EPS Rev %": "{:+.2f}", "Avg Rev Rev %": "{:+.2f}"}

                sc1, sc2 = st.columns(2)
                with sc1:
                    st.markdown("#### 🚀 Top 5 Sectors (Upward Revisions)")
                    top_sec = sector_summary.head(5).copy()
                    if not top_sec.empty:
                        st.dataframe(
                            top_sec.style.map(sector_highlight, subset=["Avg EPS Rev %", "Avg Rev Rev %"]).format(fmt, na_rep="N/A"),
                            use_container_width=True,
                            hide_index=True,
                        )
                    else:
                        st.info("No sector data available.")
                with sc2:
                    st.markdown("#### 📉 Bottom 5 Sectors (Downward Revisions)")
                    bot_sec = sector_summary.tail(5).sort_values("Avg EPS Rev %", ascending=True).copy()
                    if not bot_sec.empty:
                        st.dataframe(
                            bot_sec.style.map(sector_highlight, subset=["Avg EPS Rev %", "Avg Rev Rev %"]).format(fmt, na_rep="N/A"),
                            use_container_width=True,
                            hide_index=True,
                        )
                    else:
                        st.info("No sector data available.")

        with lookup_tab:
            render_ticker_lookup_tab()

        # Footer
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center; color: gray;'>
        <small>Data source: Financial Modeling Prep API |
        Rankings reflect EPS and revenue estimate revisions over a 30-day window from the snapshot database.</small>
        </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
