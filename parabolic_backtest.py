"""
Parabolic Filter Backtest
Tests the parabolic detection criteria against historical data
and measures forward returns after signals.
"""

import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load API key
FMP_API_KEY = os.getenv("FMP_API_KEY")


class TechnicalIndicators:
    """Technical indicator calculations"""

    @staticmethod
    def sma(data: pd.Series, period: int) -> pd.Series:
        return data.rolling(window=period).mean()

    @staticmethod
    def ema(data: pd.Series, period: int) -> pd.Series:
        return data.ewm(span=period, adjust=False).mean()

    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def macd(data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
        ema_fast = data.ewm(span=fast, adjust=False).mean()
        ema_slow = data.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def bollinger_bands(data: pd.Series, period: int = 20, std_dev: float = 2.0):
        sma = data.rolling(window=period).mean()
        std = data.rolling(window=period).std()
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        return upper, sma, lower

    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()

    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        plus_dm = high.diff()
        minus_dm = -low.diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0

        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr_val = tr.rolling(window=period).mean()
        plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr_val)
        minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr_val)

        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx_val = dx.rolling(window=period).mean()
        return adx_val


def fetch_historical_data(symbol: str, years: int = 3) -> Optional[pd.DataFrame]:
    """Fetch historical data from FMP"""
    try:
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=15)
        data = response.json()

        if "historical" in data:
            df = pd.DataFrame(data["historical"])
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")
            df = df.set_index("date")

            # Filter to requested years
            cutoff = datetime.now() - timedelta(days=years * 365)
            df = df[df.index >= cutoff]

            df = df.rename(columns={
                "open": "Open", "high": "High", "low": "Low",
                "close": "Close", "volume": "Volume"
            })
            return df[["Open", "High", "Low", "Close", "Volume"]]
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
    return None


def check_parabolic_criteria(df: pd.DataFrame, as_of_idx: int, min_conditions: int = 6,
                             return_threshold: float = 0.015) -> Tuple[bool, int, Dict, Dict]:
    """
    Check parabolic criteria as of a specific index position.
    Returns (is_parabolic, score, criteria_details, metrics)

    Adjusted thresholds for more realistic detection:
    - return_threshold: 1.5% daily avg (was 3%) = ~16% in 10 days
    """
    ti = TechnicalIndicators()

    # Need at least 60 bars before the check date
    if as_of_idx < 60:
        return False, 0, {}, {}

    # Slice data up to (and including) the check date
    df_slice = df.iloc[:as_of_idx + 1].copy()

    if len(df_slice) < 60:
        return False, 0, {}, {}

    close = df_slice["Close"]
    high = df_slice["High"]
    low = df_slice["Low"]
    volume = df_slice["Volume"]

    # Calculate indicators on the slice
    rsi = ti.rsi(close, 14)
    macd_line, macd_signal, macd_hist = ti.macd(close)
    bb_upper, bb_middle, bb_lower = ti.bollinger_bands(close, 20, 2.0)
    atr = ti.atr(high, low, close, 14)
    adx = ti.adx(high, low, close, 14)

    # Recent 10-day window
    recent_close = close.iloc[-10:]
    recent_high = high.iloc[-10:]
    recent_volume = volume.iloc[-10:]
    recent_bb_upper = bb_upper.iloc[-10:]
    recent_macd_hist = macd_hist.iloc[-10:]
    recent_atr = atr.iloc[-10:]

    # === 10 Parabolic Criteria (ADJUSTED THRESHOLDS) ===

    # 1. Avg daily return > threshold (default 1.5%, ~16% in 10 days)
    recent_returns = recent_close.pct_change().dropna()
    avg_return = recent_returns.mean() if len(recent_returns) > 0 else 0
    c1 = avg_return > return_threshold

    # 2. Returns accelerating (or at least not decelerating badly)
    if len(recent_returns) >= 5:
        accelerating = np.diff(recent_returns.tail(5).values).mean() > -0.003
    else:
        accelerating = False
    c2 = accelerating

    # 3. Volume >= 1.3x 50-day avg (lowered from 1.5x)
    avg_vol_50 = volume.rolling(50).mean().iloc[-1] if len(volume) >= 50 else volume.mean()
    vol_ratio = recent_volume.mean() / avg_vol_50 if avg_vol_50 > 0 else 0
    c3 = vol_ratio >= 1.3

    # 4. >= 1 close above Upper BB (lowered from 2)
    bb_breaks = (recent_close > recent_bb_upper).sum()
    c4 = bb_breaks >= 1

    # 5. ATR increased >= 15% (lowered from 20%)
    if len(atr) >= 30:
        avg_atr_prior = atr.iloc[-30:-10].mean()
        atr_ratio = recent_atr.mean() / avg_atr_prior if avg_atr_prior > 0 else 0
    else:
        atr_ratio = 0
    c5 = atr_ratio >= 1.15

    # 6. RSI > 60 (lowered from 65)
    last_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 0
    c6 = last_rsi > 60

    # 7. MACD histogram positive
    c7 = recent_macd_hist.mean() > 0

    # 8. ADX > 20 (trend strength)
    last_adx = adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else 0
    c8 = last_adx > 20

    # 9. Broke 20-day resistance by 2% (lowered from 3%)
    resistance = high.rolling(20).max().shift(1).iloc[-1] if len(high) >= 21 else high.max()
    c9 = close.iloc[-1] >= 1.02 * resistance if not pd.isna(resistance) else False

    # 10. Shallow pullback (max drawdown > -10%, lowered from -15%)
    recent_cummax = recent_close.cummax()
    drawdown = (recent_close - recent_cummax) / recent_cummax
    max_dd = drawdown.min()
    c10 = max_dd > -0.10

    criteria = {
        "Avg Return": c1,
        "Accelerating": c2,
        "Vol Surge": c3,
        "BB Break": c4,
        "ATR Expand": c5,
        "RSI Strong": c6,
        "MACD Pos": c7,
        "ADX Trend": c8,
        "Breakout": c9,
        "Shallow DD": c10,
    }

    metrics = {
        "avg_return": avg_return * 100,
        "vol_ratio": vol_ratio,
        "bb_breaks": bb_breaks,
        "atr_ratio": atr_ratio,
        "rsi": last_rsi,
        "adx": last_adx,
        "max_dd": max_dd * 100,
    }

    score = sum(criteria.values())
    is_parabolic = score >= min_conditions

    return is_parabolic, score, criteria, metrics


def calculate_forward_returns(df: pd.DataFrame, signal_idx: int, periods: List[int] = [5, 10, 20, 30]) -> Dict:
    """Calculate forward returns after a signal"""
    results = {}
    signal_price = df["Close"].iloc[signal_idx]

    for period in periods:
        future_idx = signal_idx + period
        if future_idx < len(df):
            future_price = df["Close"].iloc[future_idx]
            returns = (future_price - signal_price) / signal_price * 100
            results[f"{period}d_return"] = returns

            # Max gain and max drawdown during the period
            future_slice = df["Close"].iloc[signal_idx:future_idx + 1]
            max_price = future_slice.max()
            min_price = future_slice.min()
            results[f"{period}d_max_gain"] = (max_price - signal_price) / signal_price * 100
            results[f"{period}d_max_dd"] = (min_price - signal_price) / signal_price * 100
        else:
            results[f"{period}d_return"] = None
            results[f"{period}d_max_gain"] = None
            results[f"{period}d_max_dd"] = None

    return results


def backtest_symbol(symbol: str, min_conditions: int = 6, scan_interval: int = 5,
                    return_threshold: float = 0.015) -> List[Dict]:
    """
    Backtest parabolic detection for a single symbol.
    scan_interval: check every N days (to avoid overlapping signals)
    return_threshold: daily return threshold (default 1.5%)
    """
    df = fetch_historical_data(symbol, years=5)  # Get 5 years of data
    if df is None or len(df) < 100:
        return []

    signals = []
    last_signal_idx = -30  # Prevent overlapping signals

    # Scan through historical data
    for i in range(60, len(df) - 30, scan_interval):
        # Skip if too close to last signal
        if i - last_signal_idx < 20:
            continue

        is_parabolic, score, criteria, metrics = check_parabolic_criteria(
            df, i, min_conditions, return_threshold
        )

        if is_parabolic:
            signal_date = df.index[i]
            signal_price = df["Close"].iloc[i]

            # Calculate forward returns
            fwd_returns = calculate_forward_returns(df, i, [5, 10, 20, 30])

            signal_data = {
                "Symbol": symbol,
                "Signal_Date": signal_date,
                "Signal_Price": round(signal_price, 2),
                "Score": score,
                "Avg_Ret%": round(metrics["avg_return"], 2),
                "Vol_Ratio": round(metrics["vol_ratio"], 2),
                "RSI": round(metrics["rsi"], 1),
                "ADX": round(metrics["adx"], 1),
                **fwd_returns,
                **{f"c_{k}": v for k, v in criteria.items()}
            }
            signals.append(signal_data)
            last_signal_idx = i

    return signals


def run_backtest(symbols: List[str], min_conditions: int = 6,
                 return_threshold: float = 0.015) -> pd.DataFrame:
    """Run backtest across multiple symbols"""
    all_signals = []

    print(f"Backtesting {len(symbols)} symbols...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {
            executor.submit(backtest_symbol, symbol, min_conditions, 5, return_threshold): symbol
            for symbol in symbols
        }

        for i, future in enumerate(as_completed(future_to_symbol)):
            symbol = future_to_symbol[future]
            try:
                signals = future.result()
                all_signals.extend(signals)
                print(f"  [{i+1}/{len(symbols)}] {symbol}: {len(signals)} signals found")
            except Exception as e:
                print(f"  [{i+1}/{len(symbols)}] {symbol}: Error - {e}")

    if not all_signals:
        return pd.DataFrame()

    df_results = pd.DataFrame(all_signals)
    df_results = df_results.sort_values("Signal_Date")
    return df_results


def analyze_results(df: pd.DataFrame) -> Dict:
    """Analyze backtest results"""
    if df.empty:
        return {}

    analysis = {
        "total_signals": len(df),
        "unique_symbols": df["Symbol"].nunique(),
        "date_range": f"{df['Signal_Date'].min().date()} to {df['Signal_Date'].max().date()}",
    }

    # Forward return statistics
    for period in [5, 10, 20, 30]:
        col = f"{period}d_return"
        if col in df.columns:
            valid = df[col].dropna()
            if len(valid) > 0:
                analysis[f"{period}d_avg_return"] = valid.mean()
                analysis[f"{period}d_median_return"] = valid.median()
                analysis[f"{period}d_win_rate"] = (valid > 0).mean() * 100
                analysis[f"{period}d_avg_winner"] = valid[valid > 0].mean() if (valid > 0).any() else 0
                analysis[f"{period}d_avg_loser"] = valid[valid < 0].mean() if (valid < 0).any() else 0

    return analysis


def print_analysis(analysis: Dict, df: pd.DataFrame):
    """Pretty print analysis results"""
    print("\n" + "=" * 70)
    print("PARABOLIC BACKTEST RESULTS")
    print("=" * 70)

    print(f"\nTotal Signals: {analysis.get('total_signals', 0)}")
    print(f"Unique Symbols: {analysis.get('unique_symbols', 0)}")
    print(f"Date Range: {analysis.get('date_range', 'N/A')}")

    print("\n" + "-" * 70)
    print("FORWARD RETURNS ANALYSIS")
    print("-" * 70)
    print(f"{'Period':<10} {'Avg Return':<12} {'Median':<10} {'Win Rate':<10} {'Avg Win':<10} {'Avg Loss':<10}")
    print("-" * 70)

    for period in [5, 10, 20, 30]:
        avg = analysis.get(f"{period}d_avg_return", 0)
        median = analysis.get(f"{period}d_median_return", 0)
        win_rate = analysis.get(f"{period}d_win_rate", 0)
        avg_win = analysis.get(f"{period}d_avg_winner", 0)
        avg_loss = analysis.get(f"{period}d_avg_loser", 0)
        print(f"{period}d{'':<7} {avg:>+10.2f}%  {median:>+8.2f}%  {win_rate:>8.1f}%  {avg_win:>+8.2f}%  {avg_loss:>+8.2f}%")

    # Show top performers
    if not df.empty and "20d_return" in df.columns:
        print("\n" + "-" * 70)
        print("TOP 10 SIGNALS (by 20-day return)")
        print("-" * 70)
        top = df.nlargest(10, "20d_return")[["Symbol", "Signal_Date", "Signal_Price", "Score", "20d_return", "20d_max_gain"]]
        top["Signal_Date"] = top["Signal_Date"].dt.strftime("%Y-%m-%d")
        print(top.to_string(index=False))

        print("\n" + "-" * 70)
        print("WORST 10 SIGNALS (by 20-day return)")
        print("-" * 70)
        worst = df.nsmallest(10, "20d_return")[["Symbol", "Signal_Date", "Signal_Price", "Score", "20d_return", "20d_max_dd"]]
        worst["Signal_Date"] = worst["Signal_Date"].dt.strftime("%Y-%m-%d")
        print(worst.to_string(index=False))


# ============================================================================
# MAIN - Run Backtest
# ============================================================================

if __name__ == "__main__":
    # Test on known parabolic movers + control group
    test_symbols = [
        # Known parabolic movers
        "GME", "AMC", "TSLA", "NVDA", "AMD", "SMCI", "PLTR", "MSTR",
        "COIN", "RIOT", "MARA", "SQ", "SHOP", "ROKU", "ZM", "DKNG",
        # High-growth tech
        "META", "GOOGL", "AMZN", "MSFT", "AAPL", "NFLX", "CRM", "NOW",
        # Control group (stable stocks)
        "JNJ", "PG", "KO", "PEP", "WMT", "UNH", "VZ", "T",
        # Volatile sectors
        "XOM", "CVX", "OXY", "FSLR", "ENPH", "LLY", "NVO", "MRNA"
    ]

    # Adjusted thresholds for more realistic detection
    MIN_CONDITIONS = 6  # Require 6 of 10 criteria (was 7)
    RETURN_THRESHOLD = 0.015  # 1.5% daily avg = ~16% in 10 days (was 3%)

    print("Starting Parabolic Filter Backtest...")
    print(f"Testing {len(test_symbols)} symbols")
    print(f"Criteria: {MIN_CONDITIONS}/10 conditions, {RETURN_THRESHOLD*100:.1f}% daily return threshold\n")

    # Run backtest
    results_df = run_backtest(test_symbols, min_conditions=MIN_CONDITIONS,
                              return_threshold=RETURN_THRESHOLD)

    if not results_df.empty:
        # Analyze
        analysis = analyze_results(results_df)
        print_analysis(analysis, results_df)

        # Save results
        output_path = "parabolic_backtest_results.csv"
        results_df.to_csv(output_path, index=False)
        print(f"\nResults saved to: {output_path}")

        # Summary by score threshold
        print("\n" + "=" * 70)
        print("RESULTS BY SCORE THRESHOLD")
        print("=" * 70)
        for threshold in [7, 8, 9, 10]:
            subset = results_df[results_df["Score"] >= threshold]
            if len(subset) > 0:
                avg_20d = subset["20d_return"].dropna().mean()
                win_rate = (subset["20d_return"].dropna() > 0).mean() * 100
                print(f"Score >= {threshold}: {len(subset):>4} signals, Avg 20d: {avg_20d:>+6.2f}%, Win Rate: {win_rate:>5.1f}%")
    else:
        print("No signals found in backtest.")
