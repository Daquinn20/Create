"""
Buy Trigger Screener
Criteria:
1. RSI > 45
2. Positive MACD cross within last 5 days (MACD line crossed above signal line)
3. Positive RSI cross within last 5 days (RSI crossed above 50)
4. MRS (Mansfield Relative Strength) positive OR sloping up over last 10 days
5. CMF (Chaikin Money Flow) positive OR sloping up over last 10 days
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import requests
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# API Keys
FMP_API_KEY = os.getenv('FMP_API_KEY')

# Stock Universe Files
ONEDRIVE_DATA_PATH = Path(r"C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\AI dashboard Data")
SP500_FILE = ONEDRIVE_DATA_PATH / "SP500_list_with_sectors.xlsx"
INDEX_FILE = ONEDRIVE_DATA_PATH / "Index_Broad_US.xlsx"

# ============================================================================
# INDICATOR CALCULATIONS
# ============================================================================

def calculate_rsi(data: pd.Series, period: int = 14) -> pd.Series:
    """RSI using Wilder's smoothing"""
    delta = data.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-10)
    return 100 - (100 / (1 + rs))


def calculate_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
    """Calculate MACD, Signal Line, and Histogram"""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def calculate_cmf(high: pd.Series, low: pd.Series, close: pd.Series,
                  volume: pd.Series, period: int = 20) -> pd.Series:
    """Calculate Chaikin Money Flow"""
    hl_range = (high - low).replace(0, 1e-10)
    mfm = ((close - low) - (high - close)) / hl_range
    mfv = mfm * volume
    return mfv.rolling(window=period).sum() / volume.rolling(window=period).sum()


def calculate_mansfield_rs(stock_close: pd.Series, benchmark_close: pd.Series,
                           lookback: int = 252) -> pd.Series:
    """Calculate Mansfield Relative Strength vs benchmark (SPY)"""
    common_idx = stock_close.index.intersection(benchmark_close.index)
    if len(common_idx) < 50:
        return pd.Series(0, index=stock_close.index)

    if len(common_idx) < lookback:
        lookback = len(common_idx)

    stock_aligned = stock_close.loc[common_idx]
    bench_aligned = benchmark_close.loc[common_idx]

    # Relative Strength = (Stock / Benchmark) normalized to SMA
    rs_ratio = stock_aligned / (bench_aligned + 1e-10)
    rs_sma = rs_ratio.rolling(window=lookback, min_periods=50).mean()

    # Mansfield RS = ((RS Ratio / RS SMA) - 1) * 100
    mrs = ((rs_ratio / (rs_sma + 1e-10)) - 1) * 100

    # Reindex to original stock index
    return mrs.reindex(stock_close.index, method='ffill').fillna(0)


def calculate_slope(series: pd.Series, period: int = 10) -> pd.Series:
    """Calculate slope over a period using linear regression"""
    slopes = pd.Series(index=series.index, dtype=float)
    for i in range(period, len(series)):
        window = series.iloc[i-period:i]
        if window.notna().sum() >= period // 2:
            x = np.arange(len(window))
            y = window.values
            # Simple linear regression slope
            slope = np.polyfit(x, y, 1)[0]
            slopes.iloc[i] = slope
    return slopes


# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_fmp_data(ticker: str) -> pd.DataFrame:
    """Fetch historical data from Financial Modeling Prep API"""
    if not FMP_API_KEY:
        return None

    try:
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
        params = {'apikey': FMP_API_KEY}
        response = requests.get(url, params=params, timeout=15)

        if response.status_code != 200:
            return None

        data = response.json()

        if 'historical' not in data or not data['historical']:
            return None

        df = pd.DataFrame(data['historical'])
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df = df.sort_index()

        df = df.rename(columns={
            'close': 'Close',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'volume': 'Volume'
        })

        return df[['Open', 'High', 'Low', 'Close', 'Volume']].tail(365)

    except Exception:
        return None


def fetch_stock_data(ticker: str) -> tuple:
    """Fetch stock data with FMP first, fallback to yfinance"""
    # Try FMP first
    if FMP_API_KEY:
        df = fetch_fmp_data(ticker)
        if df is not None and not df.empty and len(df) >= 60:
            return df, 'FMP'

    # Fallback to Yahoo Finance
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period='1y')
        if not df.empty and len(df) >= 60:
            return df, 'Yahoo'
    except Exception:
        pass

    return None, 'Failed'


def fetch_benchmark_data() -> pd.Series:
    """Fetch SPY benchmark data for Mansfield RS calculation"""
    try:
        spy = yf.Ticker('SPY')
        df = spy.history(period='2y')
        return df['Close']
    except Exception:
        return pd.Series()


# ============================================================================
# BUY TRIGGER SCREENER CLASS
# ============================================================================

class BuyTriggerScreener:
    def __init__(self, tickers: list):
        self.tickers = tickers
        self.results = []
        self.watchlist = []
        self.fail_list = []
        self.data_source_stats = {'FMP': 0, 'Yahoo': 0, 'Failed': 0}
        self.spy_close = fetch_benchmark_data()

    def check_criteria(self, df: pd.DataFrame, ticker: str) -> dict:
        """Check all buy trigger criteria and return results"""
        close = df['Close']
        high = df['High']
        low = df['Low']
        volume = df['Volume']

        # Calculate all indicators
        rsi = calculate_rsi(close, 14)
        macd_line, signal_line, histogram = calculate_macd(close)
        cmf = calculate_cmf(high, low, close, volume, 20)
        mrs = calculate_mansfield_rs(close, self.spy_close, 252)

        # Get current values
        current_rsi = rsi.iloc[-1]
        current_macd = macd_line.iloc[-1]
        current_signal = signal_line.iloc[-1]
        current_cmf = cmf.iloc[-1]
        current_mrs = mrs.iloc[-1]

        # ===== CRITERIA CHECKS =====

        # 1. RSI > 45
        rsi_above_45 = current_rsi > 45

        # 2. Positive MACD cross within last 20 days
        macd_cross_bullish = False
        for i in range(-20, 0):
            if i - 1 >= -len(macd_line):
                prev_macd = macd_line.iloc[i-1]
                prev_signal = signal_line.iloc[i-1]
                curr_macd = macd_line.iloc[i]
                curr_signal = signal_line.iloc[i]
                # Bullish cross: MACD crosses above signal line
                if prev_macd <= prev_signal and curr_macd > curr_signal:
                    macd_cross_bullish = True
                    break

        # 3. Positive RSI cross within last 20 days (RSI crossing above 50)
        rsi_cross_bullish = False
        for i in range(-20, 0):
            if i - 1 >= -len(rsi):
                prev_rsi = rsi.iloc[i-1]
                curr_rsi = rsi.iloc[i]
                # Bullish cross: RSI crosses above 50
                if prev_rsi <= 50 and curr_rsi > 50:
                    rsi_cross_bullish = True
                    break

        # 4. MRS positive OR sloping up over last 10 days
        mrs_slope = calculate_slope(mrs, 10)
        current_mrs_slope = mrs_slope.iloc[-1] if not mrs_slope.empty and not pd.isna(mrs_slope.iloc[-1]) else 0
        mrs_positive = current_mrs > 0
        mrs_sloping_up = current_mrs_slope > 0
        mrs_pass = mrs_positive or mrs_sloping_up

        # 5. CMF positive OR sloping up over last 10 days
        cmf_slope = calculate_slope(cmf, 10)
        current_cmf_slope = cmf_slope.iloc[-1] if not cmf_slope.empty and not pd.isna(cmf_slope.iloc[-1]) else 0
        cmf_positive = current_cmf > 0
        cmf_sloping_up = current_cmf_slope > 0
        cmf_pass = cmf_positive or cmf_sloping_up

        # Count how many criteria pass
        criteria_passed = sum([
            rsi_above_45,
            macd_cross_bullish,
            rsi_cross_bullish,
            mrs_pass,
            cmf_pass
        ])

        # Build result dictionary
        result = {
            'Ticker': ticker,
            'Price': round(close.iloc[-1], 2),
            'RSI': round(current_rsi, 2),
            'RSI > 45': 'PASS' if rsi_above_45 else 'FAIL',
            'MACD': round(current_macd, 4),
            'Signal': round(current_signal, 4),
            'MACD Cross 20d': 'PASS' if macd_cross_bullish else 'FAIL',
            'RSI Cross 50 20d': 'PASS' if rsi_cross_bullish else 'FAIL',
            'MRS': round(current_mrs, 2),
            'MRS Slope': round(current_mrs_slope, 4) if current_mrs_slope else 0,
            'MRS Check': 'PASS' if mrs_pass else 'FAIL',
            'CMF': round(current_cmf, 4) if not pd.isna(current_cmf) else 0,
            'CMF Slope': round(current_cmf_slope, 6) if current_cmf_slope else 0,
            'CMF Check': 'PASS' if cmf_pass else 'FAIL',
            'Criteria Met': f"{criteria_passed}/5",
            'All Pass': criteria_passed == 5
        }

        return result

    def scan_ticker(self, ticker: str) -> dict:
        """Scan individual ticker"""
        try:
            df, source = fetch_stock_data(ticker)

            if df is None or df.empty or len(df) < 60:
                self.data_source_stats['Failed'] += 1
                return {'Ticker': ticker, 'Status': 'FAIL', 'Reason': 'Insufficient data'}

            self.data_source_stats[source] += 1

            result = self.check_criteria(df, ticker)

            if result['All Pass']:
                result['Status'] = 'BUY TRIGGER'
                self.results.append(result)
            elif int(result['Criteria Met'].split('/')[0]) >= 3:
                result['Status'] = 'WATCHLIST'
                self.watchlist.append(result)
            else:
                result['Status'] = 'FAIL'
                self.fail_list.append(result)

            return result

        except Exception as e:
            self.data_source_stats['Failed'] += 1
            return {'Ticker': ticker, 'Status': 'FAIL', 'Reason': str(e)}

    def run_scan(self, max_workers: int = 10) -> tuple:
        """Run scan on all tickers with parallel processing"""
        print(f"\n{'='*80}")
        print("BUY TRIGGER SCREENER")
        print("="*80)
        print("Criteria:")
        print("  1. RSI > 45")
        print("  2. Positive MACD cross within last 20 days")
        print("  3. Positive RSI cross (above 50) within last 20 days")
        print("  4. MRS (Mansfield RS) positive OR sloping up over last 10 days")
        print("  5. CMF (Chaikin Money Flow) positive OR sloping up over last 10 days")
        print(f"{'='*80}\n")
        print(f"Scanning {len(self.tickers)} stocks...\n")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.scan_ticker, ticker): ticker
                      for ticker in self.tickers}

            completed = 0
            for future in as_completed(futures):
                completed += 1
                ticker = futures[future]
                try:
                    result = future.result()
                    status = result.get('Status', 'FAIL')
                    if status == 'BUY TRIGGER':
                        print(f"[{completed}/{len(self.tickers)}] {ticker}: BUY TRIGGER")
                    elif status == 'WATCHLIST':
                        print(f"[{completed}/{len(self.tickers)}] {ticker}: Watchlist ({result.get('Criteria Met', '?')})")
                except Exception as e:
                    print(f"[{completed}/{len(self.tickers)}] {ticker}: Error - {e}")

        return self.results, self.watchlist, self.fail_list

    def export_results(self, filename: str = None) -> str:
        """Export results to Excel"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'buy_trigger_screen_{timestamp}.xlsx'

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            if self.results:
                df_results = pd.DataFrame(self.results)
                # Sort by RSI descending
                df_results = df_results.sort_values('RSI', ascending=False)
                df_results.to_excel(writer, sheet_name='BUY_TRIGGERS', index=False)

            if self.watchlist:
                df_watchlist = pd.DataFrame(self.watchlist)
                df_watchlist = df_watchlist.sort_values('Criteria Met', ascending=False)
                df_watchlist.to_excel(writer, sheet_name='WATCHLIST', index=False)

            if self.fail_list:
                df_fail = pd.DataFrame(self.fail_list)
                df_fail.to_excel(writer, sheet_name='FAIL', index=False)

        print(f"\n{'='*80}")
        print(f"Results exported to: {filename}")
        print(f"  BUY TRIGGERS (all 5 criteria): {len(self.results)}")
        print(f"  WATCHLIST (3-4 criteria): {len(self.watchlist)}")
        print(f"  FAIL (<3 criteria): {len(self.fail_list)}")
        print(f"  Total scanned: {len(self.results) + len(self.watchlist) + len(self.fail_list)}")
        print(f"\nData Sources:")
        print(f"  FMP: {self.data_source_stats['FMP']}")
        print(f"  Yahoo Finance: {self.data_source_stats['Yahoo']}")
        print(f"  Failed: {self.data_source_stats['Failed']}")
        print(f"{'='*80}\n")

        return filename


# ============================================================================
# STOCK UNIVERSE LOADERS
# ============================================================================

def load_sp500_tickers() -> list:
    """Load S&P 500 tickers from FMP API"""
    try:
        url = f"https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=30)
        data = response.json()
        if data:
            return [item['symbol'] for item in data]
    except Exception:
        pass

    # Fallback to Excel
    try:
        df = pd.read_excel(SP500_FILE)
        if 'Symbol' in df.columns:
            return df['Symbol'].dropna().tolist()
        elif 'Ticker' in df.columns:
            return df['Ticker'].dropna().tolist()
    except Exception:
        pass

    return []


def load_nasdaq100_tickers() -> list:
    """Load NASDAQ 100 tickers from FMP API"""
    try:
        url = f"https://financialmodelingprep.com/api/v3/nasdaq_constituent?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=30)
        data = response.json()
        if data:
            return [item['symbol'] for item in data]
    except Exception:
        pass
    return []


def load_russell2000_tickers() -> list:
    """Load Russell 2000 tickers"""
    try:
        csv_path = Path(__file__).parent / "russell_2000.csv"
        df = pd.read_csv(csv_path)
        return df['Ticker'].str.upper().str.strip().dropna().tolist()
    except Exception:
        pass
    return []


def load_master_universe_tickers(us_only: bool = True) -> list:
    """Load tickers from master_universe.csv (all US stocks)"""
    try:
        csv_path = Path(__file__).parent / "master_universe.csv"
        df = pd.read_csv(csv_path, header=None, names=['Ticker', 'Name', 'Exchange'])
        # Filter out nan tickers and clean up
        tickers = df['Ticker'].dropna().astype(str).str.upper().str.strip().tolist()
        # Remove 'nan' strings and empty strings
        tickers = [t for t in tickers if t and t != 'NAN' and len(t) <= 5]
        # Filter to US-only (no dots in ticker - excludes .L, .PA, .CO, etc.)
        if us_only:
            tickers = [t for t in tickers if '.' not in t]
        return tickers
    except Exception as e:
        print(f"Error loading master_universe.csv: {e}")
        return []


def load_custom_tickers(filepath: str) -> list:
    """Load tickers from custom Excel or CSV file"""
    try:
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
        for col in ['Symbol', 'Ticker', 'symbol', 'ticker']:
            if col in df.columns:
                return df[col].dropna().astype(str).str.upper().str.strip().tolist()
        # Try first column
        return df.iloc[:, 0].dropna().astype(str).str.upper().str.strip().tolist()
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return []


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import sys

    # Default to S&P 500
    universe = "SP500"

    if len(sys.argv) > 1:
        arg = sys.argv[1].upper()
        if arg in ['SP500', 'S&P500', 'SPX']:
            universe = 'SP500'
        elif arg in ['NASDAQ', 'NASDAQ100', 'NDX', 'QQQ']:
            universe = 'NASDAQ100'
        elif arg in ['RUSSELL', 'RUSSELL2000', 'RUT', 'IWM']:
            universe = 'RUSSELL2000'
        elif arg in ['MASTER', 'MASTER_UNIVERSE', 'ALL', 'FULL']:
            universe = 'MASTER'
        elif os.path.exists(sys.argv[1]):
            universe = 'CUSTOM'
            custom_file = sys.argv[1]
        else:
            print(f"Unknown universe: {arg}")
            print("Usage: python buy_trigger_screener.py [SP500|NASDAQ100|RUSSELL2000|MASTER|filepath.xlsx]")
            sys.exit(1)

    # Load tickers based on universe
    print(f"Loading {universe} tickers...")

    if universe == 'SP500':
        tickers = load_sp500_tickers()
    elif universe == 'NASDAQ100':
        tickers = load_nasdaq100_tickers()
    elif universe == 'RUSSELL2000':
        tickers = load_russell2000_tickers()
    elif universe == 'MASTER':
        tickers = load_master_universe_tickers()
    elif universe == 'CUSTOM':
        tickers = load_custom_tickers(custom_file)
    else:
        tickers = load_sp500_tickers()

    if not tickers:
        print("No tickers loaded. Exiting.")
        sys.exit(1)

    print(f"Loaded {len(tickers)} tickers from {universe}")

    # Run screener
    screener = BuyTriggerScreener(tickers)
    results, watchlist, fail_list = screener.run_scan(max_workers=15)

    # Export results
    screener.export_results()

    # Print summary of BUY TRIGGERS
    if results:
        print("\n" + "="*80)
        print("BUY TRIGGER STOCKS (All 5 Criteria Met):")
        print("="*80)
        df = pd.DataFrame(results)
        cols = ['Ticker', 'Price', 'RSI', 'MACD', 'MRS', 'CMF', 'Criteria Met']
        print(df[cols].to_string(index=False))
    else:
        print("\nNo stocks met all 5 buy trigger criteria.")

    # Print watchlist summary
    if watchlist:
        print("\n" + "="*80)
        print(f"TOP 10 WATCHLIST STOCKS (3-4 Criteria Met):")
        print("="*80)
        df = pd.DataFrame(watchlist)
        df = df.sort_values('Criteria Met', ascending=False)
        cols = ['Ticker', 'Price', 'RSI', 'MACD', 'MRS', 'CMF', 'Criteria Met']
        print(df[cols].head(10).to_string(index=False))