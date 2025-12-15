"""
Technical Analysis Screener
Criteria:
- Stock must be above 20, 50, 100, and 150 SMA
- Volatility declining (comparing recent vs older periods)
- Volume declining (comparing recent vs older periods)
- Calculates 10, 20, 50, 100, 150, 200 SMAs
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import requests
import os
from dotenv import load_dotenv
warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

class SMAVolatilityScreener:
    def __init__(self, tickers):
        self.tickers = tickers
        self.results = []
        self.watchlist = []
        self.fail_list = []
        self.fmp_api_key = os.getenv('FMP_API_KEY')
        self.data_source_stats = {'FMP': 0, 'Yahoo': 0, 'Failed': 0}

    def fetch_fmp_data(self, ticker):
        """
        Fetch historical data from Financial Modeling Prep API
        Returns DataFrame in same format as yfinance for compatibility
        """
        if not self.fmp_api_key:
            return None

        try:
            # FMP historical price endpoint
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
            params = {
                'apikey': self.fmp_api_key
            }

            response = requests.get(url, params=params, timeout=10)

            if response.status_code != 200:
                return None

            data = response.json()

            if 'historical' not in data or not data['historical']:
                return None

            # Convert to DataFrame
            df = pd.DataFrame(data['historical'])

            # Convert date to datetime and set as index
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            df = df.sort_index()  # Sort by date ascending

            # Rename columns to match yfinance format
            df = df.rename(columns={
                'close': 'Close',
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'volume': 'Volume'
            })

            # Select only last 365 days
            df = df.tail(365)

            return df[['Open', 'High', 'Low', 'Close', 'Volume']]

        except Exception as e:
            return None

    def fetch_stock_data(self, ticker):
        """
        Fetch stock data with FMP first, fallback to yfinance
        Returns (dataframe, source_name)
        """
        # Try FMP first
        if self.fmp_api_key:
            df = self.fetch_fmp_data(ticker)
            if df is not None and not df.empty and len(df) >= 200:
                self.data_source_stats['FMP'] += 1
                return df, 'FMP'

        # Fallback to Yahoo Finance
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period='1y')
            if not df.empty and len(df) >= 200:
                self.data_source_stats['Yahoo'] += 1
                return df, 'Yahoo'
        except Exception as e:
            pass

        self.data_source_stats['Failed'] += 1
        return None, 'Failed'

    def calculate_smas(self, df):
        """Calculate Simple Moving Averages"""
        df['SMA_10'] = df['Close'].rolling(window=10).mean()
        df['SMA_20'] = df['Close'].rolling(window=20).mean()
        df['SMA_50'] = df['Close'].rolling(window=50).mean()
        df['SMA_100'] = df['Close'].rolling(window=100).mean()
        df['SMA_150'] = df['Close'].rolling(window=150).mean()
        df['SMA_200'] = df['Close'].rolling(window=200).mean()
        return df

    def check_volatility_declining(self, df, recent_period=20, older_period=60):
        """
        Check if volatility is declining
        Compare recent volatility to older volatility
        """
        if len(df) < older_period + recent_period:
            return False, None, None

        # Calculate rolling standard deviation (volatility measure)
        recent_volatility = df['Close'].iloc[-recent_period:].std()
        older_volatility = df['Close'].iloc[-(older_period + recent_period):-recent_period].std()

        is_declining = recent_volatility < older_volatility
        return is_declining, recent_volatility, older_volatility

    def check_volume_declining(self, df, recent_period=20, older_period=60):
        """
        Check if volume is declining
        Compare recent average volume to older average volume
        """
        if len(df) < older_period + recent_period:
            return False, None, None

        recent_avg_volume = df['Volume'].iloc[-recent_period:].mean()
        older_avg_volume = df['Volume'].iloc[-(older_period + recent_period):-recent_period].mean()

        is_declining = recent_avg_volume < older_avg_volume
        return is_declining, recent_avg_volume, older_avg_volume

    def check_price_above_smas(self, current_price, sma_20, sma_50, sma_100, sma_150):
        """Check if price is above required SMAs"""
        return (current_price > sma_20 and
                current_price > sma_50 and
                current_price > sma_100 and
                current_price > sma_150)

    def check_price_near_sma10(self, current_price, sma_10, tolerance=0.05):
        """
        Check if price is within tolerance % of SMA_10
        Default tolerance is 5% (0.05)
        """
        lower_bound = sma_10 * (1 - tolerance)
        upper_bound = sma_10 * (1 + tolerance)
        return lower_bound <= current_price <= upper_bound

    def calculate_grade(self, vol_decline_pct, volume_decline_pct, price_vs_sma10_pct):
        """
        Calculate grade based on key metrics
        A: Excellent (all metrics strong)
        B: Good (2 metrics strong)
        C: Fair (1 metric strong)
        """
        score = 0

        # Volatility decline (higher is better)
        if vol_decline_pct >= 50:
            score += 1

        # Volume decline (higher is better)
        if volume_decline_pct >= 20:
            score += 1

        # Close to SMA10 (lower absolute value is better)
        if abs(price_vs_sma10_pct) <= 5:
            score += 1

        if score >= 3:
            return 'A'
        elif score == 2:
            return 'B'
        else:
            return 'C'

    def scan_ticker(self, ticker):
        """Scan individual ticker"""
        try:
            print(f"Scanning {ticker}...")

            # Fetch data using FMP first, fallback to Yahoo Finance
            df, source = self.fetch_stock_data(ticker)

            if df is None or df.empty or len(df) < 200:
                fail_reason = "Insufficient data"
                print(f"  {ticker}: {fail_reason}")
                fail_result = {
                    'Ticker': ticker,
                    'Status': 'FAIL',
                    'Fail_Reason': fail_reason,
                    'Grade': 'N/A',
                    'Price': None
                }
                self.fail_list.append(fail_result)
                return None

            # Log data source
            if source == 'FMP':
                print(f"  {ticker}: Using FMP data")
            elif source == 'Yahoo':
                print(f"  {ticker}: Using Yahoo Finance data (FMP unavailable)")

            # Calculate SMAs
            df = self.calculate_smas(df)

            # Get latest values
            latest = df.iloc[-1]
            current_price = latest['Close']

            # Check if price is above required SMAs
            above_smas = self.check_price_above_smas(
                current_price,
                latest['SMA_20'],
                latest['SMA_50'],
                latest['SMA_100'],
                latest['SMA_150']
            )

            if not above_smas:
                fail_reason = "Price not above all required SMAs"
                print(f"  {ticker}: {fail_reason}")
                fail_result = {
                    'Ticker': ticker,
                    'Status': 'FAIL',
                    'Fail_Reason': fail_reason,
                    'Grade': 'N/A',
                    'Price': round(current_price, 2),
                    'SMA_20': round(latest['SMA_20'], 2),
                    'SMA_50': round(latest['SMA_50'], 2),
                    'SMA_100': round(latest['SMA_100'], 2),
                    'SMA_150': round(latest['SMA_150'], 2)
                }
                self.fail_list.append(fail_result)
                return None

            # Check volatility declining
            vol_declining, recent_vol, older_vol = self.check_volatility_declining(df)

            if not vol_declining:
                fail_reason = "Volatility not declining"
                print(f"  {ticker}: {fail_reason}")
                fail_result = {
                    'Ticker': ticker,
                    'Status': 'FAIL',
                    'Fail_Reason': fail_reason,
                    'Grade': 'N/A',
                    'Price': round(current_price, 2)
                }
                self.fail_list.append(fail_result)
                return None

            # Check volume declining
            volume_declining, recent_volume, older_volume = self.check_volume_declining(df)

            if not volume_declining:
                fail_reason = "Volume not declining"
                print(f"  {ticker}: {fail_reason}")
                fail_result = {
                    'Ticker': ticker,
                    'Status': 'FAIL',
                    'Fail_Reason': fail_reason,
                    'Grade': 'N/A',
                    'Price': round(current_price, 2)
                }
                self.fail_list.append(fail_result)
                return None

            # At this point, stock passes SMAs, volatility, and volume criteria
            # Now check if within 5% of SMA_10
            near_sma10 = self.check_price_near_sma10(current_price, latest['SMA_10'])

            # Calculate metrics
            vol_decline_pct = ((older_vol - recent_vol) / older_vol * 100)
            volume_decline_pct = ((older_volume - recent_volume) / older_volume * 100)
            price_vs_sma10_pct = ((current_price - latest['SMA_10']) / latest['SMA_10'] * 100)

            # Calculate grade
            grade = self.calculate_grade(vol_decline_pct, volume_decline_pct, price_vs_sma10_pct)

            # Determine status
            if near_sma10:
                status = "PASS"
                list_type = "results"
            else:
                status = "WATCHLIST"
                list_type = "watchlist"

            # All required conditions met!
            result = {
                'Ticker': ticker,
                'Status': status,
                'Grade': grade,
                'Price': round(current_price, 2),
                'SMA_10': round(latest['SMA_10'], 2),
                'SMA_20': round(latest['SMA_20'], 2),
                'SMA_50': round(latest['SMA_50'], 2),
                'SMA_100': round(latest['SMA_100'], 2),
                'SMA_150': round(latest['SMA_150'], 2),
                'SMA_200': round(latest['SMA_200'], 2),
                'Recent_Volatility': round(recent_vol, 2),
                'Older_Volatility': round(older_vol, 2),
                'Vol_Decline_%': round(vol_decline_pct, 2),
                'Recent_Avg_Volume': int(recent_volume),
                'Older_Avg_Volume': int(older_volume),
                'Volume_Decline_%': round(volume_decline_pct, 2),
                'Price_vs_SMA10_%': round(price_vs_sma10_pct, 2),
                'Price_vs_SMA20_%': round(((current_price - latest['SMA_20']) / latest['SMA_20'] * 100), 2),
                'Price_vs_SMA50_%': round(((current_price - latest['SMA_50']) / latest['SMA_50'] * 100), 2),
            }

            if status == "PASS":
                print(f"  {ticker}: PASSED - Grade {grade}")
                self.results.append(result)
            else:
                print(f"  {ticker}: WATCHLIST (>5% from SMA10) - Grade {grade}")
                self.watchlist.append(result)

            return list_type

        except Exception as e:
            print(f"  {ticker}: Error - {str(e)}")
            return None

    def run_scan(self):
        """Run scan on all tickers"""
        print(f"\n{'='*80}")
        print(f"SMA Volatility/Volume Screener")
        print(f"Criteria:")
        print(f"  - Price above 20, 50, 100, 150 SMA")
        print(f"  - Price within 5% of 10 SMA")
        print(f"  - Volatility declining (recent 20 days vs prior 60 days)")
        print(f"  - Volume declining (recent 20 days vs prior 60 days)")
        print(f"{'='*80}\n")

        for ticker in self.tickers:
            self.scan_ticker(ticker)

        return self.results, self.watchlist, self.fail_list

    def export_results(self):
        """Export results to Excel with separate sheets for PASS, WATCHLIST, and FAIL"""
        # Create filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'sma_volatility_screen_{timestamp}.xlsx'

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            if self.results:
                df_results = pd.DataFrame(self.results)
                df_results = df_results.sort_values('Volume_Decline_%', ascending=False)
                df_results.to_excel(writer, sheet_name='PASS', index=False)

            if self.watchlist:
                df_watchlist = pd.DataFrame(self.watchlist)
                df_watchlist = df_watchlist.sort_values('Volume_Decline_%', ascending=False)
                df_watchlist.to_excel(writer, sheet_name='WATCHLIST', index=False)

            if self.fail_list:
                df_fail = pd.DataFrame(self.fail_list)
                df_fail = df_fail.sort_values('Ticker')
                df_fail.to_excel(writer, sheet_name='FAIL', index=False)

        print(f"\n{'='*80}")
        print(f"Results exported to: {filename}")
        print(f"  PASS (within 5% of SMA10): {len(self.results)}")
        print(f"  WATCHLIST (>5% from SMA10): {len(self.watchlist)}")
        print(f"  FAIL (did not meet criteria): {len(self.fail_list)}")
        print(f"  Total stocks scanned: {len(self.results) + len(self.watchlist) + len(self.fail_list)}")
        print(f"\nData Sources:")
        print(f"  FMP (Financial Modeling Prep): {self.data_source_stats['FMP']} stocks")
        print(f"  Yahoo Finance (fallback): {self.data_source_stats['Yahoo']} stocks")
        print(f"  Failed to fetch: {self.data_source_stats['Failed']} stocks")
        print(f"{'='*80}\n")

        return filename


def load_tickers(filename='SP500_list.xlsx'):
    """Load tickers from Excel file"""
    try:
        df = pd.read_excel(filename)

        # Assuming ticker column is named 'Symbol' or 'Ticker'
        if 'Symbol' in df.columns:
            tickers = df['Symbol'].dropna().tolist()
        elif 'Ticker' in df.columns:
            tickers = df['Ticker'].dropna().tolist()
        else:
            # Try to find column with "Symbol" or "Ticker" as a value in first few rows
            found = False
            for col in df.columns:
                # Check if any of first 5 rows contain "Symbol" or "Ticker"
                if df[col].astype(str).str.contains('Symbol|Ticker', case=False, na=False).any():
                    # Find the row index where "Symbol" appears
                    symbol_idx = df[df[col].astype(str).str.contains('Symbol|Ticker', case=False, na=False)].index[0]
                    # Get tickers from rows after the header row
                    tickers = df.iloc[symbol_idx + 1:][col].dropna().tolist()
                    found = True
                    break

            if not found:
                # Try first non-empty column
                for col in df.columns:
                    tickers = df[col].dropna().tolist()
                    if tickers:
                        break

        # Clean up tickers - remove any non-string values and empty strings
        tickers = [str(t).strip().upper() for t in tickers if str(t).strip() and str(t).upper() != 'NAN']

        return tickers
    except FileNotFoundError:
        print(f"{filename} not found. Using default list of tickers.")
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',
                'BRK.B', 'V', 'JNJ', 'WMT', 'JPM', 'MA', 'PG', 'UNH']


if __name__ == "__main__":
    import sys

    # Check for command line argument for index file
    if len(sys.argv) > 1:
        index_file = sys.argv[1]
    else:
        # Show available index files
        import glob
        available_files = glob.glob('*_list.xlsx')
        if len(available_files) > 1:
            print("\nAvailable index files:")
            for i, file in enumerate(available_files, 1):
                print(f"  {i}. {file}")
            print("\nUsage: python sma_volatility_screener.py [filename]")
            print("Using default: SP500_list.xlsx\n")
        index_file = 'SP500_list.xlsx'

    # Load tickers
    print(f"Loading ticker list from {index_file}...")
    tickers = load_tickers(index_file)
    print(f"Loaded {len(tickers)} tickers")

    # Run screener
    screener = SMAVolatilityScreener(tickers)
    results, watchlist, fail_list = screener.run_scan()

    # Export results
    screener.export_results()

    # Print PASS summary
    if results:
        print("\n" + "="*80)
        print("TOP 10 PASS STOCKS (Within 5% of SMA10 - by Volume Decline %):")
        print("="*80)
        df_results = pd.DataFrame(results)
        df_results = df_results.sort_values('Volume_Decline_%', ascending=False)
        print(df_results[['Ticker', 'Grade', 'Price', 'Vol_Decline_%', 'Volume_Decline_%',
                          'Price_vs_SMA10_%']].head(10).to_string(index=False))

    # Print WATCHLIST summary
    if watchlist:
        print("\n" + "="*80)
        print("TOP 10 WATCHLIST STOCKS (>5% from SMA10 - by Volume Decline %):")
        print("="*80)
        df_watchlist = pd.DataFrame(watchlist)
        df_watchlist = df_watchlist.sort_values('Volume_Decline_%', ascending=False)
        print(df_watchlist[['Ticker', 'Grade', 'Price', 'Vol_Decline_%', 'Volume_Decline_%',
                            'Price_vs_SMA10_%']].head(10).to_string(index=False))

    # Print FAIL summary
    if fail_list:
        print("\n" + "="*80)
        print(f"FAILED STOCKS: {len(fail_list)} stocks did not meet criteria")
        print("="*80)
        fail_reasons = pd.DataFrame(fail_list)['Fail_Reason'].value_counts()
        print("\nFailure reasons breakdown:")
        for reason, count in fail_reasons.items():
            print(f"  - {reason}: {count} stocks")