"""
Earnings Estimates Tracker
Saves daily snapshots of analyst estimates to track real revisions over time.
Supports both PostgreSQL (Neon) and SQLite backends.
"""
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# Database configuration - use Neon (PostgreSQL) if DATABASE_URL is set, otherwise SQLite
DATABASE_URL = os.getenv('DATABASE_URL')
USE_POSTGRES = DATABASE_URL is not None

if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor
else:
    import sqlite3

# Centralized master universe path (repo copy preferred, OneDrive fallback)
_REPO_MASTER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_universe.csv")
_ONEDRIVE_MASTER_PATH = r"C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\AI dashboard Data\master_universe.csv"
MASTER_UNIVERSE_PATH = _REPO_MASTER_PATH if os.path.exists(_REPO_MASTER_PATH) else _ONEDRIVE_MASTER_PATH

# Exchange code mapping: Your format -> FMP API format
EXCHANGE_CODE_MAP = {
    'LN': '.L',      # London
    'GY': '.DE',     # Germany (Xetra)
    'FP': '.PA',     # France (Paris)
    'NA': '.AS',     # Netherlands (Amsterdam)
    'DC': '.CO',     # Denmark (Copenhagen)
    'SE': '.SW',     # Switzerland
    'SQ': '.MC',     # Spain (Madrid)
    'IM': '.MI',     # Italy (Milan)
    'BB': '.BR',     # Belgium (Brussels)
    'SS': '.ST',     # Sweden (Stockholm)
    'FH': '.HE',     # Finland (Helsinki)
    'NO': '.OL',     # Norway (Oslo)
    'AT': '.VI',     # Austria (Vienna)
    'PL': '.WA',     # Poland (Warsaw)
    'AU': '.AX',     # Australia (ASX)
    'HK': '.HK',     # Hong Kong
    'JP': '.T',      # Japan (Tokyo)
    'CN': '.SS',     # China (Shanghai)
    'SZ': '.SZ',     # China (Shenzhen)
    'TO': '.TO',     # Canada (Toronto)
    'V': '.V',       # Canada (TSX Venture)
}


def convert_to_fmp_ticker(ticker: str) -> str:
    """
    Convert ticker from 'SYMBOL EXCHANGE' format to FMP API format.
    Examples:
        'AZN LN' -> 'AZN.L'
        'SIE GY' -> 'SIE.DE'
        'AAPL' -> 'AAPL' (unchanged if no space)
    """
    ticker = ticker.strip()

    # If ticker contains a space, it's international format
    if ' ' in ticker:
        parts = ticker.split(' ')
        if len(parts) == 2:
            symbol, exchange_code = parts
            if exchange_code in EXCHANGE_CODE_MAP:
                return f"{symbol}{EXCHANGE_CODE_MAP[exchange_code]}"
            else:
                # Unknown exchange, return as-is but log warning
                print(f"Warning: Unknown exchange code '{exchange_code}' for {symbol}")
                return symbol

    # Handle tickers with / in them (like BP/ LN, RR/ LN)
    if '/' in ticker and ' ' in ticker:
        parts = ticker.rsplit(' ', 1)
        if len(parts) == 2:
            symbol, exchange_code = parts
            symbol = symbol.replace('/', '')  # Remove the slash
            if exchange_code in EXCHANGE_CODE_MAP:
                return f"{symbol}{EXCHANGE_CODE_MAP[exchange_code]}"

    return ticker


class EstimatesTracker:
    """Track and store analyst estimates over time to calculate real revisions."""

    def __init__(self, db_path: str = "estimates_history.db"):
        self.db_path = db_path
        self.api_key = os.getenv('FMP_API_KEY')
        self.use_postgres = USE_POSTGRES

        if not self.api_key:
            raise ValueError("FMP_API_KEY not found")

        self.base_url = "https://financialmodelingprep.com/api/v3"
        self._init_database()

    def _get_connection(self):
        """Get database connection based on backend."""
        if self.use_postgres:
            return psycopg2.connect(DATABASE_URL)
        else:
            return sqlite3.connect(self.db_path)

    def _init_database(self):
        """Initialize database with required tables."""
        conn = self._get_connection()
        cursor = conn.cursor()

        if self.use_postgres:
            # PostgreSQL syntax
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS estimate_snapshots (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    snapshot_date DATE NOT NULL,
                    fiscal_period TEXT NOT NULL,
                    period_type TEXT DEFAULT 'annual',
                    eps_avg REAL,
                    eps_high REAL,
                    eps_low REAL,
                    revenue_avg REAL,
                    revenue_high REAL,
                    revenue_low REAL,
                    num_analysts_eps INTEGER,
                    num_analysts_revenue INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, snapshot_date, fiscal_period)
                )
            """)

            # Add period_type column if it doesn't exist (for existing databases)
            try:
                cursor.execute("""
                    ALTER TABLE estimate_snapshots ADD COLUMN IF NOT EXISTS period_type TEXT DEFAULT 'annual'
                """)
            except Exception:
                pass  # Column may already exist

            # Index for fast lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticker_date
                ON estimate_snapshots(ticker, snapshot_date)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshot_date
                ON estimate_snapshots(snapshot_date)
            """)

            print(f"Database initialized: Neon PostgreSQL")
        else:
            # SQLite syntax
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS estimate_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    snapshot_date DATE NOT NULL,
                    fiscal_period TEXT NOT NULL,
                    period_type TEXT DEFAULT 'annual',
                    eps_avg REAL,
                    eps_high REAL,
                    eps_low REAL,
                    revenue_avg REAL,
                    revenue_high REAL,
                    revenue_low REAL,
                    num_analysts_eps INTEGER,
                    num_analysts_revenue INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, snapshot_date, fiscal_period)
                )
            """)

            # Add period_type column if it doesn't exist (for existing databases)
            try:
                cursor.execute("ALTER TABLE estimate_snapshots ADD COLUMN period_type TEXT DEFAULT 'annual'")
            except Exception:
                pass  # Column may already exist

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticker_date
                ON estimate_snapshots(ticker, snapshot_date)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshot_date
                ON estimate_snapshots(snapshot_date)
            """)

            print(f"Database initialized: SQLite ({self.db_path})")

        conn.commit()
        conn.close()

    def _make_request(self, endpoint: str) -> Optional[List[Dict]]:
        """Make API request."""
        url = f"{self.base_url}/{endpoint}?apikey={self.api_key}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"API Error: {e}")
            return None

    def fetch_estimates(self, ticker: str) -> Optional[List[Dict]]:
        """Fetch current analyst estimates for a ticker."""
        return self._make_request(f"analyst-estimates/{ticker}")

    @staticmethod
    def _identify_period_type(fiscal_period: str) -> str:
        """
        Identify if a fiscal period is annual (FY) or quarterly (Q).

        Note: This is a simple heuristic. The more reliable method is
        _get_annual_periods() which looks at spacing between dates.
        """
        if not fiscal_period or fiscal_period == 'unknown':
            return 'unknown'

        # Common fiscal year end patterns (month-day)
        # Include both month-end and common non-standard dates
        annual_patterns = ['12-31', '06-30', '03-31', '09-30', '01-31', '05-31',
                          '01-25', '01-26', '01-27', '01-28', '01-29',  # Late Jan (NVDA, etc)
                          '02-28', '02-29',  # Feb year-ends
                          '04-30', '07-31', '08-31', '10-31', '11-30']  # Other month-ends

        for pattern in annual_patterns:
            if fiscal_period.endswith(pattern):
                return 'annual'

        return 'quarterly'

    def _get_annual_periods(self, estimates: List[Dict]) -> List[Dict]:
        """
        Extract annual (fiscal year) estimates from API response.

        Uses date spacing to identify annual periods: if consecutive periods
        are ~1 year apart, they're annual. If ~3 months apart, quarterly.

        Returns future annual estimates (FY1, FY2, FY3+) for rollover protection.
        """
        if not estimates:
            return []

        def parse_date(date_str):
            try:
                return datetime.strptime(date_str, '%Y-%m-%d')
            except:
                return None

        today = datetime.now()

        # Filter to only FUTURE periods (or very recent past - within 60 days)
        # This prevents picking up old historical data
        cutoff = today - timedelta(days=60)

        future_estimates = []
        for est in estimates:
            date_obj = parse_date(est.get('date', ''))
            if date_obj and date_obj >= cutoff:
                future_estimates.append(est)

        if not future_estimates:
            # Fallback - take whatever we have
            future_estimates = estimates

        # Sort by date (ascending - earliest first = FY1, FY2, etc.)
        sorted_estimates = sorted(future_estimates, key=lambda x: x.get('date', ''))

        if len(sorted_estimates) < 2:
            # Single estimate - assume annual
            if sorted_estimates:
                sorted_estimates[0]['_period_type'] = 'annual'
            return sorted_estimates

        # Check spacing between first two periods to determine if annual or quarterly
        date1 = parse_date(sorted_estimates[0].get('date', ''))
        date2 = parse_date(sorted_estimates[1].get('date', ''))

        if date1 and date2:
            days_apart = (date2 - date1).days
            # If periods are >180 days apart, assume annual; otherwise quarterly
            is_annual_series = days_apart > 180
        else:
            # Fallback to pattern matching
            is_annual_series = self._identify_period_type(sorted_estimates[0].get('date', '')) == 'annual'

        if is_annual_series:
            # Already annual - take first 5 future periods
            annual_periods = []
            for est in sorted_estimates[:5]:
                est['_period_type'] = 'annual'
                annual_periods.append(est)
            return annual_periods
        else:
            # Quarterly - find one period per fiscal year (the fiscal year end)
            annual_periods = []
            seen_fiscal_years = set()

            for est in sorted_estimates:
                fiscal_period = est.get('date', '')
                date_obj = parse_date(fiscal_period)
                if date_obj:
                    fiscal_year = date_obj.year
                    if fiscal_year not in seen_fiscal_years:
                        est['_period_type'] = 'annual'
                        annual_periods.append(est)
                        seen_fiscal_years.add(fiscal_year)

            return annual_periods[:5]  # Return up to 5 annual periods

    def save_snapshot(self, ticker: str, estimates: List[Dict], snapshot_date: str = None):
        """
        Save estimate snapshot to database.

        Saves annual (fiscal year) estimates for FY1, FY2, FY3+ to ensure
        we have historical data after fiscal year rollover.
        """
        if not estimates:
            return

        if snapshot_date is None:
            snapshot_date = datetime.now().strftime('%Y-%m-%d')

        conn = self._get_connection()
        cursor = conn.cursor()

        # Get annual periods (FY1, FY2, FY3, etc.)
        annual_periods = self._get_annual_periods(estimates)

        # If no annual periods found, fall back to first 4 periods
        periods_to_save = annual_periods if annual_periods else estimates[:4]

        for est in periods_to_save:
            fiscal_period = est.get('date', 'unknown')
            period_type = est.get('_period_type', self._identify_period_type(fiscal_period))

            try:
                if self.use_postgres:
                    # PostgreSQL upsert syntax
                    cursor.execute("""
                        INSERT INTO estimate_snapshots
                        (ticker, snapshot_date, fiscal_period, period_type, eps_avg, eps_high, eps_low,
                         revenue_avg, revenue_high, revenue_low, num_analysts_eps, num_analysts_revenue)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ticker, snapshot_date, fiscal_period)
                        DO UPDATE SET
                            period_type = EXCLUDED.period_type,
                            eps_avg = EXCLUDED.eps_avg,
                            eps_high = EXCLUDED.eps_high,
                            eps_low = EXCLUDED.eps_low,
                            revenue_avg = EXCLUDED.revenue_avg,
                            revenue_high = EXCLUDED.revenue_high,
                            revenue_low = EXCLUDED.revenue_low,
                            num_analysts_eps = EXCLUDED.num_analysts_eps,
                            num_analysts_revenue = EXCLUDED.num_analysts_revenue
                    """, (
                        ticker,
                        snapshot_date,
                        fiscal_period,
                        period_type,
                        est.get('estimatedEpsAvg'),
                        est.get('estimatedEpsHigh'),
                        est.get('estimatedEpsLow'),
                        est.get('estimatedRevenueAvg'),
                        est.get('estimatedRevenueHigh'),
                        est.get('estimatedRevenueLow'),
                        est.get('numberAnalystsEstimatedEps'),
                        est.get('numberAnalystsEstimatedRevenue')
                    ))
                else:
                    # SQLite syntax
                    cursor.execute("""
                        INSERT OR REPLACE INTO estimate_snapshots
                        (ticker, snapshot_date, fiscal_period, period_type, eps_avg, eps_high, eps_low,
                         revenue_avg, revenue_high, revenue_low, num_analysts_eps, num_analysts_revenue)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker,
                        snapshot_date,
                        fiscal_period,
                        period_type,
                        est.get('estimatedEpsAvg'),
                        est.get('estimatedEpsHigh'),
                        est.get('estimatedEpsLow'),
                        est.get('estimatedRevenueAvg'),
                        est.get('estimatedRevenueHigh'),
                        est.get('estimatedRevenueLow'),
                        est.get('numberAnalystsEstimatedEps'),
                        est.get('numberAnalystsEstimatedRevenue')
                    ))
            except Exception as e:
                print(f"Error saving {ticker} {fiscal_period}: {e}")

        conn.commit()
        conn.close()

    def capture_daily_snapshot(self, tickers: List[str], max_workers: int = 10):
        """Capture today's estimates for all tickers."""
        today = datetime.now().strftime('%Y-%m-%d')
        print(f"\n{'='*60}")
        print(f"CAPTURING ESTIMATES SNAPSHOT - {today}")
        print(f"Database: {'Neon PostgreSQL' if self.use_postgres else 'SQLite'}")
        print(f"{'='*60}\n")

        success_count = 0

        def process_ticker(ticker):
            estimates = self.fetch_estimates(ticker)
            if estimates:
                self.save_snapshot(ticker, estimates, today)
                return True
            return False

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_ticker, t): t for t in tickers}

            for i, future in enumerate(as_completed(futures), 1):
                ticker = futures[future]
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    print(f"Error processing {ticker}: {e}")

                print(f"Progress: {i}/{len(tickers)} - {ticker}", end='\r')

        print(f"\n\nSnapshot complete: {success_count}/{len(tickers)} tickers saved")
        return success_count

    def get_revision(self, ticker: str, fiscal_period: str, days_ago: int = 30) -> Optional[Dict]:
        """
        Calculate revision for a ticker by comparing current vs past estimate
        FOR THE SAME FISCAL PERIOD.

        This correctly handles fiscal year rollover:
        - We always compare the SAME fiscal_period (e.g., "2026-12-31")
        - If that period wasn't tracked N days ago (because it was FY2+ at the time),
          we return None for that comparison window.
        - Handles API date discrepancies (e.g., 2027-01-25 vs 2027-01-26) by
          fuzzy matching periods within the same year-month.

        Returns dict with eps_revision_pct and revenue_revision_pct
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        today = datetime.now().strftime('%Y-%m-%d')
        past_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

        placeholder = '%s' if self.use_postgres else '?'

        # Get current estimate for this specific fiscal period
        cursor.execute(f"""
            SELECT eps_avg, revenue_avg, snapshot_date FROM estimate_snapshots
            WHERE ticker = {placeholder} AND fiscal_period = {placeholder}
            ORDER BY snapshot_date DESC LIMIT 1
        """, (ticker, fiscal_period))
        current = cursor.fetchone()

        # Get past estimate for the SAME fiscal period (closest to days_ago)
        cursor.execute(f"""
            SELECT eps_avg, revenue_avg, snapshot_date FROM estimate_snapshots
            WHERE ticker = {placeholder} AND fiscal_period = {placeholder} AND snapshot_date <= {placeholder}
            ORDER BY snapshot_date DESC LIMIT 1
        """, (ticker, fiscal_period, past_date))
        past = cursor.fetchone()

        # If no exact match, try fuzzy match (same year-month, within ~5 days)
        # This handles API discrepancies like 2027-01-25 vs 2027-01-26
        if not past and len(fiscal_period) >= 7:
            year_month = fiscal_period[:7]  # "2027-01"
            cursor.execute(f"""
                SELECT eps_avg, revenue_avg, snapshot_date, fiscal_period FROM estimate_snapshots
                WHERE ticker = {placeholder}
                  AND fiscal_period LIKE {placeholder}
                  AND snapshot_date <= {placeholder}
                ORDER BY snapshot_date DESC LIMIT 1
            """, (ticker, f"{year_month}%", past_date))
            fuzzy_result = cursor.fetchone()
            if fuzzy_result:
                past = (fuzzy_result[0], fuzzy_result[1], fuzzy_result[2])

        conn.close()

        if not current:
            return None

        current_eps, current_rev, current_snapshot_date = current

        # If no historical data for this fiscal period, it means this period
        # didn't exist (or wasn't tracked) N days ago - likely a rollover case
        if not past:
            return {
                'ticker': ticker,
                'fiscal_period': fiscal_period,
                'days_compared': None,
                'current_eps': current_eps,
                'past_eps': None,
                'eps_revision_pct': None,
                'current_revenue': current_rev,
                'past_revenue': None,
                'revenue_revision_pct': None,
                'note': f'No historical data for {fiscal_period} from {days_ago}+ days ago (possible rollover)'
            }

        past_eps, past_rev, past_snapshot_date = past

        # Handle date parsing for both backends
        if isinstance(past_snapshot_date, str):
            past_date_obj = datetime.strptime(past_snapshot_date, '%Y-%m-%d').date()
        elif hasattr(past_snapshot_date, 'date'):
            past_date_obj = past_snapshot_date.date()
        else:
            past_date_obj = past_snapshot_date

        days_compared = (datetime.now().date() - past_date_obj).days

        result = {
            'ticker': ticker,
            'fiscal_period': fiscal_period,
            'days_compared': days_compared,
            'current_eps': current_eps,
            'past_eps': past_eps,
            'eps_revision_pct': None,
            'current_revenue': current_rev,
            'past_revenue': past_rev,
            'revenue_revision_pct': None
        }

        if past_eps and past_eps != 0 and current_eps is not None:
            result['eps_revision_pct'] = ((current_eps - past_eps) / abs(past_eps)) * 100

        if past_rev and past_rev != 0 and current_rev is not None:
            result['revenue_revision_pct'] = ((current_rev - past_rev) / abs(past_rev)) * 100

        return result

    def get_available_annual_periods(self, ticker: str) -> List[Dict]:
        """
        Get all available annual fiscal periods for a ticker with their date ranges.

        Returns list of dicts with:
        - fiscal_period: The fiscal year end date (e.g., "2026-12-31")
        - earliest_snapshot: First date we have data for this period
        - latest_snapshot: Most recent date we have data for this period
        - snapshot_count: Number of snapshots available
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        placeholder = '%s' if self.use_postgres else '?'

        cursor.execute(f"""
            SELECT fiscal_period,
                   MIN(snapshot_date) as earliest,
                   MAX(snapshot_date) as latest,
                   COUNT(*) as count
            FROM estimate_snapshots
            WHERE ticker = {placeholder}
              AND period_type = 'annual'
            GROUP BY fiscal_period
            ORDER BY fiscal_period ASC
        """, (ticker,))

        results = cursor.fetchall()
        conn.close()

        periods = []
        for row in results:
            periods.append({
                'fiscal_period': str(row[0]),
                'earliest_snapshot': str(row[1]),
                'latest_snapshot': str(row[2]),
                'snapshot_count': row[3]
            })

        return periods

    def get_revisions_summary(self, ticker: str, days_list: List[int] = [7, 30, 60, 90]) -> Dict:
        """
        Get revision summary across multiple time periods.

        IMPORTANT: Handles fiscal year rollover correctly.
        When the year rolls over, FY1 becomes a new fiscal period. We compare the
        SAME fiscal period across time, not just "whatever is currently FY1".

        For example: If current FY1 is "2026-12-31" but 30 days ago this was FY2,
        we compare today's estimate for "2026-12-31" to the historical estimate
        for "2026-12-31" (even though it was labeled FY2 back then).
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        placeholder = '%s' if self.use_postgres else '?'

        # Get the CURRENT FY1 (next annual fiscal period - must be in the future)
        # Use today's date as cutoff - we want the NEXT fiscal year end
        today = datetime.now()
        cutoff_date = today.strftime('%Y-%m-%d')

        cursor.execute(f"""
            SELECT DISTINCT fiscal_period FROM estimate_snapshots
            WHERE ticker = {placeholder}
              AND (period_type = 'annual' OR period_type IS NULL)
              AND fiscal_period >= {placeholder}
            ORDER BY fiscal_period ASC
            LIMIT 1
        """, (ticker, cutoff_date))

        result = cursor.fetchone()

        # Fallback: if no future annual period found, get the most recent one
        if not result:
            cursor.execute(f"""
                SELECT fiscal_period FROM estimate_snapshots
                WHERE ticker = {placeholder}
                ORDER BY fiscal_period DESC
                LIMIT 1
            """, (ticker,))
            result = cursor.fetchone()

        conn.close()

        if not result:
            return {'ticker': ticker, 'error': 'No data'}

        fiscal_period = result[0]

        summary = {'ticker': ticker, 'fiscal_period': fiscal_period}

        for days in days_list:
            revision = self.get_revision(ticker, fiscal_period, days)
            if revision:
                summary[f'eps_rev_{days}d'] = revision.get('eps_revision_pct')
                summary[f'rev_rev_{days}d'] = revision.get('revenue_revision_pct')
                summary[f'actual_days_{days}d'] = revision.get('days_compared')
            else:
                # No historical data for this fiscal period - could be a rollover
                # where this period didn't exist N days ago (was too far out)
                summary[f'eps_rev_{days}d'] = None
                summary[f'rev_rev_{days}d'] = None
                summary[f'actual_days_{days}d'] = None

        return summary

    def get_snapshot_dates(self) -> List[str]:
        """Get list of all snapshot dates in database."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT snapshot_date FROM estimate_snapshots ORDER BY snapshot_date DESC")
        dates = [str(row[0]) for row in cursor.fetchall()]
        conn.close()
        return dates

    def get_ticker_count(self) -> int:
        """Get count of unique tickers in database."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM estimate_snapshots")
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def print_status(self):
        """Print database status."""
        dates = self.get_snapshot_dates()
        ticker_count = self.get_ticker_count()

        print(f"\n{'='*60}")
        print("ESTIMATES TRACKER STATUS")
        print(f"{'='*60}")
        print(f"Database: {'Neon PostgreSQL' if self.use_postgres else f'SQLite ({self.db_path})'}")
        print(f"Total tickers tracked: {ticker_count}")
        print(f"Snapshot dates: {len(dates)}")
        if dates:
            print(f"  Latest: {dates[0]}")
            print(f"  Oldest: {dates[-1]}")
        print(f"{'='*60}\n")


def get_master_universe_tickers(file_path: str = MASTER_UNIVERSE_PATH) -> List[str]:
    """Load tickers from master universe CSV (centralized ticker source)."""
    try:
        # CSV has no header: Column 0 = Ticker, Column 1 = Name, Column 2 = Exchange
        df = pd.read_csv(file_path, header=None, names=['Ticker', 'Name', 'Exchange'])
        # Filter out empty rows and get unique tickers
        raw_tickers = df['Ticker'].dropna().astype(str).str.strip().tolist()
        # Remove any empty strings
        raw_tickers = [t for t in raw_tickers if t and len(t) > 0]

        # Convert international tickers to FMP format
        tickers = [convert_to_fmp_ticker(t) for t in raw_tickers]

        # Count conversions
        intl_count = sum(1 for t in tickers if '.' in t)
        print(f"Loaded {len(tickers)} tickers from master_universe.csv ({intl_count} international)")
        return tickers
    except Exception as e:
        print(f"Error loading master universe: {e}")
        return []


def get_sp500_tickers(file_path: str = 'SP500_list.xlsx') -> List[str]:
    """Load S&P 500 tickers from Excel file."""
    try:
        df = pd.read_excel(file_path)
        return df['Symbol'].tolist()
    except Exception as e:
        print(f"Error loading SP500 list: {e}")
        return []


def get_disruption_tickers(file_path: str = 'Disruption Index.xlsx') -> List[str]:
    """Get Disruption Index tickers from Excel file."""
    try:
        df = pd.read_excel(file_path)
        # Skip first 2 rows (header rows), get column 1 (Symbol column)
        symbols = df.iloc[2:, 1].dropna().tolist()
        # Convert to uppercase
        symbols = [str(s).upper() for s in symbols]
        return symbols
    except Exception as e:
        print(f"Error loading Disruption Index: {e}")
        # Fallback to small list
        return ["NVDA", "TSLA", "PLTR", "AMD", "COIN", "AAPL", "MSFT", "GOOGL", "AMZN", "META"]


def get_broad_us_tickers(file_path: str = 'Index_Broad_US.xlsx') -> List[str]:
    """Get Broad US Index tickers from Excel file."""
    try:
        df = pd.read_excel(file_path)
        symbols = df['Ticker'].dropna().tolist()
        symbols = [str(s).upper() for s in symbols]
        return symbols
    except Exception as e:
        print(f"Error loading Broad US Index: {e}")
        return []


def main():
    """Main function - run daily to capture estimates."""
    import argparse

    parser = argparse.ArgumentParser(description='Capture daily analyst estimates')
    parser.add_argument('--universe', choices=['master', 'sp500', 'disruption', 'both', 'broad'],
                        default='master', help='Which stocks to track (master = master_universe.csv, default)')
    parser.add_argument('--status', action='store_true', help='Show database status')
    parser.add_argument('--test', type=str, help='Test revision for a ticker (e.g., NVDA)')
    parser.add_argument('--diagnose', type=str, help='Diagnose fiscal periods for a ticker (shows all FY data)')

    args = parser.parse_args()

    tracker = EstimatesTracker()

    if args.status:
        tracker.print_status()
        return

    if args.diagnose:
        ticker = args.diagnose.upper()
        print(f"\n{'='*60}")
        print(f"FISCAL PERIOD DIAGNOSIS FOR {ticker}")
        print(f"{'='*60}")

        # Show all available annual periods
        periods = tracker.get_available_annual_periods(ticker)
        if periods:
            print(f"\nAvailable Annual Periods:")
            print(f"{'Fiscal Period':<15} {'Earliest Snapshot':<18} {'Latest Snapshot':<18} {'Count':<6}")
            print("-" * 60)
            for p in periods:
                print(f"{p['fiscal_period']:<15} {p['earliest_snapshot']:<18} {p['latest_snapshot']:<18} {p['snapshot_count']:<6}")
        else:
            print("\nNo annual periods found in database.")

        # Show revision summary
        print(f"\nRevision Summary (FY1):")
        summary = tracker.get_revisions_summary(ticker)
        for key, value in summary.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}%")
            else:
                print(f"  {key}: {value}")

        # Show what happens at different time windows
        print(f"\nDetailed Revision Check:")
        fiscal_period = summary.get('fiscal_period')
        if fiscal_period:
            for days in [7, 30, 60, 90]:
                rev = tracker.get_revision(ticker, fiscal_period, days)
                if rev:
                    note = rev.get('note', '')
                    if rev.get('past_eps') is None:
                        print(f"  {days}d: NO HISTORICAL DATA - {note}")
                    else:
                        eps_rev = rev.get('eps_revision_pct')
                        days_actual = rev.get('days_compared')
                        eps_str = f"{eps_rev:.2f}%" if eps_rev else "N/A"
                        print(f"  {days}d: EPS revision {eps_str} (actual: {days_actual} days)")

        print(f"{'='*60}\n")
        return

    if args.test:
        print(f"\nRevision summary for {args.test}:")
        summary = tracker.get_revisions_summary(args.test)
        for key, value in summary.items():
            if value is not None and 'rev' in key:
                print(f"  {key}: {value:.2f}%" if isinstance(value, float) else f"  {key}: {value}")
            else:
                print(f"  {key}: {value}")
        return

    # Build ticker list based on universe selection
    tickers = []

    if args.universe == 'master':
        tickers = get_master_universe_tickers()
    elif args.universe == 'broad':
        tickers = get_broad_us_tickers()
        print(f"Added {len(tickers)} Broad US Index tickers")
    else:
        if args.universe in ['sp500', 'both']:
            sp500 = get_sp500_tickers()
            tickers.extend(sp500)
            print(f"Added {len(sp500)} S&P 500 tickers")

        if args.universe in ['disruption', 'both']:
            disruption = get_disruption_tickers()
            # Add only unique tickers
            new_tickers = [t for t in disruption if t not in tickers]
            tickers.extend(new_tickers)
            print(f"Added {len(new_tickers)} Disruption Index tickers")

        # Remove duplicates while preserving order
        tickers = list(dict.fromkeys(tickers))

    print(f"\nTotal unique tickers: {len(tickers)}")

    # Capture snapshot
    tracker.capture_daily_snapshot(tickers)
    tracker.print_status()


if __name__ == "__main__":
    main()
