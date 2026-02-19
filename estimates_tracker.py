"""
Earnings Estimates Tracker
Saves daily snapshots of analyst estimates to track real revisions over time.
Run daily via Windows Task Scheduler to build revision history.
"""
import sqlite3
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

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
        if not self.api_key:
            raise ValueError("FMP_API_KEY not found")

        self.base_url = "https://financialmodelingprep.com/api/v3"
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database with required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Table for daily estimate snapshots
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS estimate_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                snapshot_date DATE NOT NULL,
                fiscal_period TEXT NOT NULL,
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

        # Index for fast lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ticker_date
            ON estimate_snapshots(ticker, snapshot_date)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshot_date
            ON estimate_snapshots(snapshot_date)
        """)

        conn.commit()
        conn.close()
        print(f"Database initialized: {self.db_path}")

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

    def save_snapshot(self, ticker: str, estimates: List[Dict], snapshot_date: str = None):
        """Save estimate snapshot to database."""
        if not estimates:
            return

        if snapshot_date is None:
            snapshot_date = datetime.now().strftime('%Y-%m-%d')

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for est in estimates[:4]:  # Save next 4 periods
            fiscal_period = est.get('date', 'unknown')

            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO estimate_snapshots
                    (ticker, snapshot_date, fiscal_period, eps_avg, eps_high, eps_low,
                     revenue_avg, revenue_high, revenue_low, num_analysts_eps, num_analysts_revenue)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ticker,
                    snapshot_date,
                    fiscal_period,
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
        Calculate revision for a ticker by comparing current vs past estimate.

        Returns dict with eps_revision_pct and revenue_revision_pct
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        today = datetime.now().strftime('%Y-%m-%d')
        past_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

        # Get current estimate
        cursor.execute("""
            SELECT eps_avg, revenue_avg FROM estimate_snapshots
            WHERE ticker = ? AND fiscal_period = ?
            ORDER BY snapshot_date DESC LIMIT 1
        """, (ticker, fiscal_period))
        current = cursor.fetchone()

        # Get past estimate (closest to days_ago)
        cursor.execute("""
            SELECT eps_avg, revenue_avg, snapshot_date FROM estimate_snapshots
            WHERE ticker = ? AND fiscal_period = ? AND snapshot_date <= ?
            ORDER BY snapshot_date DESC LIMIT 1
        """, (ticker, fiscal_period, past_date))
        past = cursor.fetchone()

        conn.close()

        if not current or not past:
            return None

        current_eps, current_rev = current
        past_eps, past_rev, past_snapshot_date = past

        result = {
            'ticker': ticker,
            'fiscal_period': fiscal_period,
            'days_compared': (datetime.now() - datetime.strptime(past_snapshot_date, '%Y-%m-%d')).days,
            'current_eps': current_eps,
            'past_eps': past_eps,
            'eps_revision_pct': None,
            'current_revenue': current_rev,
            'past_revenue': past_rev,
            'revenue_revision_pct': None
        }

        if past_eps and past_eps != 0:
            result['eps_revision_pct'] = ((current_eps - past_eps) / abs(past_eps)) * 100

        if past_rev and past_rev != 0:
            result['revenue_revision_pct'] = ((current_rev - past_rev) / abs(past_rev)) * 100

        return result

    def get_revisions_summary(self, ticker: str, days_list: List[int] = [7, 30, 60, 90]) -> Dict:
        """Get revision summary across multiple time periods."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get the next fiscal period for this ticker
        cursor.execute("""
            SELECT DISTINCT fiscal_period FROM estimate_snapshots
            WHERE ticker = ?
            ORDER BY fiscal_period ASC LIMIT 1
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
            else:
                summary[f'eps_rev_{days}d'] = None
                summary[f'rev_rev_{days}d'] = None

        return summary

    def get_snapshot_dates(self) -> List[str]:
        """Get list of all snapshot dates in database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT snapshot_date FROM estimate_snapshots ORDER BY snapshot_date DESC")
        dates = [row[0] for row in cursor.fetchall()]
        conn.close()
        return dates

    def get_ticker_count(self) -> int:
        """Get count of unique tickers in database."""
        conn = sqlite3.connect(self.db_path)
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
        print(f"Database: {self.db_path}")
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

    args = parser.parse_args()

    tracker = EstimatesTracker()

    if args.status:
        tracker.print_status()
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
