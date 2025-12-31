"""
Enhanced Earnings Revision Ranker for S&P 500
Identifies companies with strongest upward earnings revisions relative to consensus
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv
import numpy as np
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

load_dotenv()

# Try to import estimates tracker for real revision data
try:
    from estimates_tracker import EstimatesTracker
    ESTIMATES_TRACKER_AVAILABLE = True
except ImportError:
    ESTIMATES_TRACKER_AVAILABLE = False


class EarningsRevisionRanker:
    """
    Advanced earnings revision analysis and ranking system.
    Tracks estimate changes, analyst count shifts, and revision momentum.
    """

    def __init__(self, api_key: Optional[str] = None, max_workers: int = 10):
        """Initialize with FMP API key"""
        self.api_key = api_key or os.getenv('FMP_API_KEY')
        if not self.api_key:
            raise ValueError("FMP_API_KEY not found in environment")

        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.results = []
        self.max_workers = max_workers
        self.lock = Lock()
        self.progress_count = 0

        # Initialize estimates tracker if available
        self.estimates_tracker = None
        if ESTIMATES_TRACKER_AVAILABLE:
            try:
                self.estimates_tracker = EstimatesTracker()
            except Exception as e:
                print(f"Warning: Could not initialize estimates tracker: {e}")

    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make API request with error handling"""
        if params is None:
            params = {}
        params['apikey'] = self.api_key

        url = f"{self.base_url}/{endpoint}"

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API Error for {endpoint}: {e}")
            return None

    def get_analyst_estimates(self, ticker: str) -> Optional[List[Dict]]:
        """Get current analyst estimates for multiple quarters ahead"""
        return self._make_request(f"analyst-estimates/{ticker}")

    def get_price_target(self, ticker: str) -> Optional[List[Dict]]:
        """Get analyst price targets"""
        # Use v4 endpoint for price target consensus
        url = f"https://financialmodelingprep.com/api/v4/price-target-consensus?symbol={ticker}&apikey={self.api_key}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return None

    def get_upgrades_downgrades(self, ticker: str) -> Optional[List[Dict]]:
        """Get recent analyst rating changes"""
        return self._make_request(f"upgrades-downgrades", {"symbol": ticker})

    def get_analyst_ratings(self, ticker: str) -> Optional[List[Dict]]:
        """Get current analyst buy/hold/sell ratings"""
        return self._make_request(f"analyst-stock-recommendations/{ticker}")

    def get_analyst_estimates_history(self, ticker: str) -> Optional[List[Dict]]:
        """Get historical analyst estimates to track revisions"""
        return self._make_request(f"historical/analyst-estimates/{ticker}")

    def get_earnings_surprises(self, ticker: str) -> Optional[List[Dict]]:
        """Get earnings surprises (beats/misses) for last 4 quarters"""
        return self._make_request(f"earnings-surprises/{ticker}")

    def get_real_revisions(self, ticker: str, days: int = 30) -> Dict:
        """
        Get real EPS/Revenue revisions from historical tracker.
        Returns revision percentages comparing current vs N days ago.
        """
        result = {
            'eps_revision_pct': None,
            'revenue_revision_pct': None,
            'revision_days': days,
            'has_revision_data': False
        }

        if not self.estimates_tracker:
            return result

        try:
            summary = self.estimates_tracker.get_revisions_summary(ticker, [days])
            eps_key = f'eps_rev_{days}d'
            rev_key = f'rev_rev_{days}d'

            if eps_key in summary and summary[eps_key] is not None:
                result['eps_revision_pct'] = summary[eps_key]
                result['has_revision_data'] = True

            if rev_key in summary and summary[rev_key] is not None:
                result['revenue_revision_pct'] = summary[rev_key]
                result['has_revision_data'] = True

        except Exception as e:
            pass  # Silently fail, revision data is optional

        return result

    def analyze_beats_misses(self, ticker: str) -> Dict:
        """Analyze last 4 quarters of earnings beats/misses"""
        surprises = self.get_earnings_surprises(ticker)

        result = {
            'beats': 0,
            'misses': 0,
            'meets': 0,
            'streak': 'N/A',
            'avg_surprise_pct': 0
        }

        if not surprises:
            return result

        # Get last 4 quarters
        quarters = surprises[:4]
        streak_list = []
        surprise_pcts = []

        for q in quarters:
            actual = q.get('actualEarningResult', 0) or 0
            estimated = q.get('estimatedEarning', 0) or 0

            if estimated != 0:
                surprise_pct = ((actual - estimated) / abs(estimated)) * 100
                surprise_pcts.append(surprise_pct)

                if actual > estimated:
                    result['beats'] += 1
                    streak_list.append('B')
                elif actual < estimated:
                    result['misses'] += 1
                    streak_list.append('M')
                else:
                    result['meets'] += 1
                    streak_list.append('-')

        result['streak'] = ''.join(streak_list) if streak_list else 'N/A'
        result['avg_surprise_pct'] = sum(surprise_pcts) / len(surprise_pcts) if surprise_pcts else 0

        return result

    def calculate_revision_metrics(self, ticker: str) -> Optional[Dict]:
        """
        Calculate comprehensive revision metrics for a stock

        Returns dict with:
        - EPS revision trend (up/down/stable)
        - Magnitude of revisions
        - Analyst count changes
        - Price target changes
        - Composite revision strength score
        """
        print(f"Analyzing {ticker}...", end='\r')

        # Get data
        current_estimates = self.get_analyst_estimates(ticker)
        price_targets = self.get_price_target(ticker)
        upgrades = self.get_upgrades_downgrades(ticker)
        analyst_ratings = self.get_analyst_ratings(ticker)

        if not current_estimates or len(current_estimates) == 0:
            return None

        # Initialize metrics
        metrics = {
            'ticker': ticker,
            'timestamp': datetime.now().isoformat(),

            # Current estimates
            'current_eps_q1': None,
            'current_eps_fy1': None,
            'current_revenue_q1': None,
            'analyst_count_eps': None,
            'analyst_count_revenue': None,

            # Revision metrics
            'eps_revision_pct': None,
            'revenue_revision_pct': None,
            'analyst_count_change': None,

            # Price target metrics
            'price_target_avg': None,
            'price_target_high': None,
            'price_target_low': None,
            'price_target_count': None,

            # Rating changes (last 90 days)
            'upgrades_count': 0,
            'downgrades_count': 0,
            'net_rating_change': 0,

            # Current analyst ratings
            'strong_buy': 0,
            'buy': 0,
            'hold': 0,
            'sell': 0,
            'strong_sell': 0,

            # Earnings beats/misses (last 4 quarters)
            'beats_4q': 0,
            'misses_4q': 0,
            'streak': 'N/A',
            'avg_surprise_pct': 0,

            # Composite score
            'revision_strength_score': 0
        }

        # Get beats/misses data
        beats_misses = self.analyze_beats_misses(ticker)
        metrics['beats_4q'] = beats_misses['beats']
        metrics['misses_4q'] = beats_misses['misses']
        metrics['streak'] = beats_misses['streak']
        metrics['avg_surprise_pct'] = round(beats_misses['avg_surprise_pct'], 2)

        # Process current estimates
        try:
            # Sort by date to get forward quarters
            estimates_sorted = sorted(
                current_estimates,
                key=lambda x: x.get('date', ''),
                reverse=False
            )

            # Get next quarter (Q1) and full year estimates
            if len(estimates_sorted) > 0:
                q1 = estimates_sorted[0]
                metrics['current_eps_q1'] = q1.get('estimatedEpsAvg')
                metrics['current_revenue_q1'] = q1.get('estimatedRevenueAvg')
                metrics['analyst_count_eps'] = q1.get('numberAnalystsEstimatedEps', 0)
                metrics['analyst_count_revenue'] = q1.get('numberAnalystsEstimatedRevenue', 0)

            # Try to find full year estimate
            for est in estimates_sorted:
                if est.get('date', '').endswith('12-31') or 'FY' in str(est.get('date', '')):
                    metrics['current_eps_fy1'] = est.get('estimatedEpsAvg')
                    break

            if not metrics['current_eps_fy1'] and len(estimates_sorted) > 3:
                # Use 4th quarter estimate as proxy for FY
                metrics['current_eps_fy1'] = estimates_sorted[3].get('estimatedEpsAvg')

        except Exception as e:
            print(f"Error processing estimates for {ticker}: {e}")

        # Calculate EPS revisions by comparing to historical data
        # This is a simplified approach - in production you'd track actual revision dates
        try:
            # Get estimate history
            history = self._make_request(f"analyst-estimates/{ticker}")

            if history and len(history) >= 2:
                # Compare most recent to previous
                recent = history[0]
                previous = history[1] if len(history) > 1 else recent

                recent_eps = recent.get('estimatedEpsAvg', 0)
                prev_eps = previous.get('estimatedEpsAvg', 0)

                if prev_eps and prev_eps != 0:
                    metrics['eps_revision_pct'] = ((recent_eps - prev_eps) / abs(prev_eps)) * 100

                recent_rev = recent.get('estimatedRevenueAvg', 0)
                prev_rev = previous.get('estimatedRevenueAvg', 0)

                if prev_rev and prev_rev != 0:
                    metrics['revenue_revision_pct'] = ((recent_rev - prev_rev) / abs(prev_rev)) * 100

                # Analyst count change
                recent_count = recent.get('numberAnalystsEstimatedEps', 0)
                prev_count = previous.get('numberAnalystsEstimatedEps', 0)
                metrics['analyst_count_change'] = recent_count - prev_count

        except Exception as e:
            print(f"Error calculating revisions for {ticker}: {e}")

        # Process price targets (v4 endpoint format)
        if price_targets and len(price_targets) > 0:
            pt = price_targets[0]
            metrics['price_target_avg'] = pt.get('targetConsensus')
            metrics['price_target_high'] = pt.get('targetHigh')
            metrics['price_target_low'] = pt.get('targetLow')
            metrics['price_target_count'] = None  # Not available in consensus endpoint

        # Process analyst ratings (buy/hold/sell)
        if analyst_ratings and len(analyst_ratings) > 0:
            ar = analyst_ratings[0]
            metrics['strong_buy'] = ar.get('analystRatingsStrongBuy', 0)
            metrics['buy'] = ar.get('analystRatingsbuy', 0)
            metrics['hold'] = ar.get('analystRatingsHold', 0)
            metrics['sell'] = ar.get('analystRatingsSell', 0)
            metrics['strong_sell'] = ar.get('analystRatingsStrongSell', 0)

        # Process upgrades/downgrades (last 90 days)
        if upgrades:
            cutoff_date = datetime.now() - timedelta(days=90)

            for action in upgrades[:20]:  # Check recent actions
                try:
                    action_date_str = action.get('publishedDate', action.get('date', ''))
                    if action_date_str:
                        action_date = datetime.strptime(
                            action_date_str.split('T')[0],
                            '%Y-%m-%d'
                        )

                        if action_date >= cutoff_date:
                            action_type = action.get('action', '').lower()

                            if 'upgrade' in action_type or 'up' in action_type:
                                metrics['upgrades_count'] += 1
                            elif 'downgrade' in action_type or 'down' in action_type:
                                metrics['downgrades_count'] += 1

                except (ValueError, TypeError):
                    continue

            metrics['net_rating_change'] = metrics['upgrades_count'] - metrics['downgrades_count']

        # Calculate composite revision strength score
        # Factors: Earnings beats, Earnings surprises, Upgrades/Downgrades
        # Note: EPS/Revenue revision % removed - API data compares different fiscal years, not actual revisions
        score = 0

        # Factor 1: Earnings beats last 4 quarters (0-40 points)
        # 10 points per beat, -8 points per miss
        beats_score = metrics['beats_4q'] * 10
        score += beats_score
        score -= metrics['misses_4q'] * 8

        # Factor 2: Average earnings surprise % (0-30 points)
        # Rewards magnitude of beats, penalizes magnitude of misses
        if metrics['avg_surprise_pct'] is not None:
            surprise_score = min(metrics['avg_surprise_pct'] * 3, 30)
            score += max(surprise_score, -20)  # Allow negative for misses

        # Factor 3: Net rating changes - upgrades/downgrades (0-30 points)
        if metrics['net_rating_change'] > 0:
            rating_score = min(metrics['net_rating_change'] * 5, 30)
            score += rating_score
        elif metrics['net_rating_change'] < 0:
            # Penalize downgrades
            score += metrics['net_rating_change'] * 5

        metrics['revision_strength_score'] = round(score, 2)

        return metrics

    def _process_single_stock(self, ticker: str, total: int) -> Optional[Dict]:
        """Process a single stock (for parallel execution)"""
        metrics = self.calculate_revision_metrics(ticker)

        # Thread-safe progress update
        with self.lock:
            self.progress_count += 1
            print(f"Progress: {self.progress_count}/{total} - {ticker:<6} ({(self.progress_count/total)*100:.1f}%)", end='\r')

        return metrics

    @staticmethod
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
            return []

    def scan_disruption_index(self, parallel: bool = True, disruption_file: str = 'Disruption Index.xlsx') -> pd.DataFrame:
        """Scan Disruption Index stocks for earnings revisions"""
        tickers = self.get_disruption_tickers(disruption_file)
        return self.scan_tickers(tickers, parallel=parallel, source=f"Disruption Index ({len(tickers)} stocks)")

    def scan_broad_us_index(self, index_file: str = 'Index_Broad_US.xlsx', max_stocks: Optional[int] = None,
                           parallel: bool = True, sectors: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Scan Broad US Index stocks for earnings revisions

        Args:
            index_file: Path to Excel file with Broad US Index tickers
            max_stocks: Optional limit for testing (None = scan all)
            parallel: Use parallel processing (default True)
            sectors: Optional list of sectors to filter (None = scan all sectors)

        Returns:
            DataFrame with ranked results
        """
        # Load Broad US Index
        index_df = pd.read_excel(index_file)

        # Filter by sector if specified
        if sectors and 'Sector' in index_df.columns:
            index_df = index_df[index_df['Sector'].isin(sectors)]
            print(f"Filtering for sectors: {', '.join(sectors)}")

        tickers = index_df['Ticker'].tolist()

        if max_stocks:
            tickers = tickers[:max_stocks]

        print(f"\n{'='*80}")
        print(f"SCANNING {len(tickers)} BROAD US INDEX STOCKS FOR EARNINGS REVISIONS")
        if parallel:
            print(f"Using {self.max_workers} parallel workers for faster processing")
        print(f"{'='*80}\n")

        results = []
        total = len(tickers)
        self.progress_count = 0

        if parallel:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_ticker = {
                    executor.submit(self._process_single_stock, ticker, total): ticker
                    for ticker in tickers
                }
                for future in as_completed(future_to_ticker):
                    try:
                        metrics = future.result()
                        if metrics:
                            results.append(metrics)
                    except Exception as e:
                        ticker = future_to_ticker[future]
                        print(f"\nError processing {ticker}: {e}")
        else:
            for i, ticker in enumerate(tickers, 1):
                print(f"Progress: {i}/{total} - {ticker:<6} ({(i/total)*100:.1f}%)", end='\r')
                metrics = self.calculate_revision_metrics(ticker)
                if metrics:
                    results.append(metrics)
                time.sleep(0.2)

        print(f"\n\n{'='*80}")
        print(f"SCAN COMPLETE - Analyzed {len(results)} stocks")
        print(f"{'='*80}\n")

        df = pd.DataFrame(results)

        # Add sector/industry information if available
        if 'Sector' in index_df.columns:
            sector_map = dict(zip(index_df['Ticker'], index_df['Sector']))
            df['sector'] = df['ticker'].map(sector_map)
        if 'Industry' in index_df.columns:
            industry_map = dict(zip(index_df['Ticker'], index_df['Industry']))
            df['industry'] = df['ticker'].map(industry_map)

        df = df.sort_values('revision_strength_score', ascending=False)
        df = df.reset_index(drop=True)

        return df

    def scan_tickers(self, tickers: List[str], parallel: bool = True, source: str = "Custom") -> pd.DataFrame:
        """Scan a custom list of tickers"""
        print(f"\n{'='*80}")
        print(f"SCANNING {len(tickers)} {source.upper()} STOCKS FOR EARNINGS REVISIONS")
        if parallel:
            print(f"Using {self.max_workers} parallel workers for faster processing")
        print(f"{'='*80}\n")

        results = []
        total = len(tickers)
        self.progress_count = 0

        if parallel:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_ticker = {
                    executor.submit(self._process_single_stock, ticker, total): ticker
                    for ticker in tickers
                }
                for future in as_completed(future_to_ticker):
                    try:
                        metrics = future.result()
                        if metrics:
                            results.append(metrics)
                    except Exception as e:
                        ticker = future_to_ticker[future]
                        print(f"\nError processing {ticker}: {e}")
        else:
            for i, ticker in enumerate(tickers, 1):
                print(f"Progress: {i}/{total} - {ticker:<6} ({(i/total)*100:.1f}%)", end='\r')
                metrics = self.calculate_revision_metrics(ticker)
                if metrics:
                    results.append(metrics)
                time.sleep(0.2)

        print(f"\n\n{'='*80}")
        print(f"SCAN COMPLETE - Analyzed {len(results)} stocks")
        print(f"{'='*80}\n")

        df = pd.DataFrame(results)
        df = df.sort_values('revision_strength_score', ascending=False)
        df = df.reset_index(drop=True)

        return df

    def scan_sp500(self, sp500_file: str = 'SP500_list.xlsx', max_stocks: Optional[int] = None,
                   parallel: bool = True, sectors: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Scan all S&P 500 stocks and rank by revision strength

        Args:
            sp500_file: Path to Excel file with S&P 500 tickers
            max_stocks: Optional limit for testing (None = scan all)
            parallel: Use parallel processing (default True)
            sectors: Optional list of sectors to filter (None = scan all sectors)

        Returns:
            DataFrame with ranked results
        """
        # Load S&P 500 list
        sp500_df = pd.read_excel(sp500_file)

        # Filter by sector if specified
        if sectors and 'Sector' in sp500_df.columns:
            sp500_df = sp500_df[sp500_df['Sector'].isin(sectors)]
            print(f"Filtering for sectors: {', '.join(sectors)}")

        tickers = sp500_df['Symbol'].tolist()

        if max_stocks:
            tickers = tickers[:max_stocks]

        print(f"\n{'='*80}")
        print(f"SCANNING {len(tickers)} STOCKS FOR EARNINGS REVISIONS")
        if parallel:
            print(f"Using {self.max_workers} parallel workers for faster processing")
        print(f"{'='*80}\n")

        results = []
        total = len(tickers)
        self.progress_count = 0

        if parallel:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_ticker = {
                    executor.submit(self._process_single_stock, ticker, total): ticker
                    for ticker in tickers
                }

                # Collect results as they complete
                for future in as_completed(future_to_ticker):
                    try:
                        metrics = future.result()
                        if metrics:
                            results.append(metrics)
                    except Exception as e:
                        ticker = future_to_ticker[future]
                        print(f"\nError processing {ticker}: {e}")
        else:
            # Sequential processing (original method)
            for i, ticker in enumerate(tickers, 1):
                print(f"Progress: {i}/{total} - {ticker:<6} ({(i/total)*100:.1f}%)", end='\r')

                metrics = self.calculate_revision_metrics(ticker)
                if metrics:
                    results.append(metrics)

                time.sleep(0.2)

        print(f"\n\n{'='*80}")
        print(f"SCAN COMPLETE - Analyzed {len(results)} stocks")
        print(f"{'='*80}\n")

        # Convert to DataFrame
        df = pd.DataFrame(results)

        # Add sector information if available
        if 'Sector' in sp500_df.columns:
            sector_map = dict(zip(sp500_df['Symbol'], sp500_df['Sector']))
            industry_map = dict(zip(sp500_df['Symbol'], sp500_df.get('Industry', pd.Series())))
            df['sector'] = df['ticker'].map(sector_map)
            if 'Industry' in sp500_df.columns:
                df['industry'] = df['ticker'].map(industry_map)

        # Sort by revision strength score
        df = df.sort_values('revision_strength_score', ascending=False)
        df = df.reset_index(drop=True)

        return df

    def export_to_excel(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """
        Export results to Excel with formatting

        Args:
            df: Results DataFrame
            filename: Optional custom filename

        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'earnings_revisions_ranked_{timestamp}.xlsx'

        # Create Excel writer with formatting
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main results sheet
            df.to_excel(writer, sheet_name='Rankings', index=False)

            # Get workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Rankings']

            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter

                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass

                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

            # Create summary sheet with top performers
            top_20 = df.head(20)[['ticker', 'revision_strength_score', 'eps_revision_pct',
                                   'revenue_revision_pct', 'net_rating_change',
                                   'analyst_count_change']]
            top_20.to_excel(writer, sheet_name='Top 20', index=False)

        print(f"\n✓ Results saved to: {filename}")
        return filename

    def print_summary(self, df: pd.DataFrame, top_n: int = 20):
        """Print summary of top revision stocks"""
        print(f"\n{'='*80}")
        print(f"TOP {top_n} STOCKS BY EARNINGS REVISION STRENGTH")
        print(f"{'='*80}\n")

        top_stocks = df.head(top_n)

        print(f"{'Rank':<6} {'Ticker':<8} {'Score':<8} {'EPS Rev%':<10} {'Rev Rev%':<10} {'Upgrades':<10} {'Analysts':<8}")
        print(f"{'-'*80}")

        for idx, row in top_stocks.iterrows():
            eps_rev = f"{row['eps_revision_pct']:.2f}%" if pd.notna(row['eps_revision_pct']) else "N/A"
            rev_rev = f"{row['revenue_revision_pct']:.2f}%" if pd.notna(row['revenue_revision_pct']) else "N/A"
            net_rating = row['net_rating_change'] if pd.notna(row['net_rating_change']) else 0
            analyst_chg = row['analyst_count_change'] if pd.notna(row['analyst_count_change']) else 0

            print(f"{idx+1:<6} {row['ticker']:<8} {row['revision_strength_score']:<8.2f} "
                  f"{eps_rev:<10} {rev_rev:<10} {net_rating:<10} {analyst_chg:<8}")

        print(f"\n{'='*80}")
        print(f"STATISTICS")
        print(f"{'='*80}")
        print(f"Total stocks analyzed: {len(df)}")
        print(f"Stocks with positive revisions: {len(df[df['revision_strength_score'] > 0])}")
        print(f"Stocks with negative revisions: {len(df[df['revision_strength_score'] < 0])}")
        print(f"Average revision score: {df['revision_strength_score'].mean():.2f}")
        print(f"Median revision score: {df['revision_strength_score'].median():.2f}")
        print(f"{'='*80}\n")


def main():
    """Main execution function"""
    try:
        # Initialize ranker with parallel processing (10 workers)
        ranker = EarningsRevisionRanker(max_workers=10)

        # Test with small sample first (optional)
        print("Would you like to:")
        print("1. Test with 20 stocks first")
        print("2. Run full S&P 500 scan")

        choice = input("\nEnter choice (1 or 2): ").strip()

        if choice == "1":
            print("\nRunning test scan with 20 stocks...")
            df = ranker.scan_sp500(max_stocks=20)
        else:
            print("\nRunning FULL S&P 500 scan (this will take ~20 minutes)...")
            df = ranker.scan_sp500()

        # Print summary
        ranker.print_summary(df)

        # Export to Excel
        filename = ranker.export_to_excel(df)

        print(f"\n✓ Complete! Check '{filename}' for full results.")

        return df

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    df = main()
