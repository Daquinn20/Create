"""
Parabolic Filter Optimizer
Tests multiple parameter combinations to find optimal configuration.
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from itertools import product
from typing import Dict, List, Tuple
import warnings
import sys
warnings.filterwarnings('ignore')

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)

# Import from existing module
from Technical_Screen_Quinn import TechnicalIndicators


def get_historical_data(symbol: str, years: int = 5) -> pd.DataFrame:
    """Fetch historical data"""
    try:
        end = datetime.now()
        start = end - timedelta(days=years * 365)
        df = yf.download(symbol, start=start, end=end, progress=False)
        if df.empty:
            return None
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        return df
    except:
        return None


def check_criteria_with_params(
    df: pd.DataFrame,
    as_of_idx: int,
    params: Dict
) -> Tuple[bool, int, Dict]:
    """
    Check parabolic criteria with configurable parameters.

    params dict should contain:
    - min_score: minimum criteria to pass (default 6)
    - sma_filter: max % above 200 SMA (e.g., 0.30 for 30%, None to disable)
    - rsi_threshold: RSI must be above this (default 60)
    - vol_threshold: volume multiplier vs 50d avg (default 1.3)
    - return_threshold: min avg daily return (default 0.015)
    - atr_threshold: min ATR expansion (default 1.15)
    - bb_breaks_min: min BB breaks (default 1)
    - drawdown_threshold: max allowed drawdown (default -0.10)
    """
    ti = TechnicalIndicators()

    if as_of_idx < 200:  # Need 200 bars for SMA
        return False, 0, {}

    df_slice = df.iloc[:as_of_idx + 1].copy()
    if len(df_slice) < 200:
        return False, 0, {}

    close = df_slice["Close"]
    high = df_slice["High"]
    low = df_slice["Low"]
    volume = df_slice["Volume"]
    current_price = close.iloc[-1]

    # Calculate indicators
    rsi = ti.rsi(close, 14)
    macd_line, macd_signal, macd_hist = ti.macd(close)
    bb_upper, bb_middle, bb_lower = ti.bollinger_bands(close, 20, 2.0)
    atr = ti.atr(high, low, close, 14)
    adx = ti.adx(high, low, close, 14)
    sma_200 = ti.sma(close, 200)

    # SMA Filter
    sma_filter = params.get('sma_filter')
    if sma_filter is not None:
        current_sma_200 = sma_200.iloc[-1] if not pd.isna(sma_200.iloc[-1]) else None
        if current_sma_200 is not None and current_price > (1 + sma_filter) * current_sma_200:
            return False, 0, {}

    # Recent windows
    recent_close = close.iloc[-10:]
    recent_volume = volume.iloc[-10:]
    recent_bb_upper = bb_upper.iloc[-10:]
    recent_macd_hist = macd_hist.iloc[-10:]
    recent_atr = atr.iloc[-10:]

    # Get thresholds from params
    rsi_threshold = params.get('rsi_threshold', 60)
    vol_threshold = params.get('vol_threshold', 1.3)
    return_threshold = params.get('return_threshold', 0.015)
    atr_threshold = params.get('atr_threshold', 1.15)
    bb_breaks_min = params.get('bb_breaks_min', 1)
    drawdown_threshold = params.get('drawdown_threshold', -0.10)

    # === CRITERIA ===

    # 1. Avg daily return
    recent_returns = recent_close.pct_change().dropna()
    avg_return = recent_returns.mean() if len(recent_returns) > 0 else 0
    c1 = avg_return > return_threshold

    # 2. Returns accelerating
    if len(recent_returns) >= 5:
        c2 = np.diff(recent_returns.tail(5).values).mean() > -0.003
    else:
        c2 = False

    # 3. Volume surge
    avg_vol_50 = volume.rolling(50).mean().iloc[-1] if len(volume) >= 50 else volume.mean()
    vol_ratio = recent_volume.mean() / avg_vol_50 if avg_vol_50 > 0 else 0
    c3 = vol_ratio >= vol_threshold

    # 4. BB breaks
    bb_breaks = (recent_close > recent_bb_upper).sum()
    c4 = bb_breaks >= bb_breaks_min

    # 5. ATR expansion
    if len(atr) >= 30:
        avg_atr_prior = atr.iloc[-30:-10].mean()
        atr_ratio = recent_atr.mean() / avg_atr_prior if avg_atr_prior > 0 else 0
    else:
        atr_ratio = 0
    c5 = atr_ratio >= atr_threshold

    # 6. RSI strong
    last_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 0
    c6 = last_rsi > rsi_threshold

    # 7. MACD positive
    c7 = recent_macd_hist.mean() > 0

    # 8. ADX trending
    last_adx = adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else 0
    c8 = last_adx > 20

    # 9. Breakout
    resistance = high.rolling(20).max().shift(1).iloc[-1] if len(high) >= 21 else high.max()
    c9 = current_price >= 1.02 * resistance if not pd.isna(resistance) else False

    # 10. Shallow drawdown
    recent_cummax = recent_close.cummax()
    drawdown = (recent_close - recent_cummax) / recent_cummax
    max_dd = drawdown.min()
    c10 = max_dd > drawdown_threshold

    score = sum([c1, c2, c3, c4, c5, c6, c7, c8, c9, c10])
    min_score = params.get('min_score', 6)
    is_parabolic = score >= min_score

    metrics = {
        'score': score,
        'avg_return': avg_return * 100,
        'vol_ratio': vol_ratio,
        'rsi': last_rsi,
        'max_dd': max_dd * 100
    }

    return is_parabolic, score, metrics


def calculate_forward_returns(df: pd.DataFrame, signal_idx: int) -> Dict:
    """Calculate forward returns"""
    results = {}
    signal_price = df["Close"].iloc[signal_idx]

    for period in [5, 10, 20, 30]:
        future_idx = signal_idx + period
        if future_idx < len(df):
            future_slice = df["Close"].iloc[signal_idx:future_idx + 1]
            future_price = future_slice.iloc[-1]
            max_price = future_slice.max()
            min_price = future_slice.min()

            results[f'{period}d_return'] = ((future_price - signal_price) / signal_price) * 100
            results[f'{period}d_max_gain'] = ((max_price - signal_price) / signal_price) * 100
            results[f'{period}d_max_dd'] = ((min_price - signal_price) / signal_price) * 100

    return results


def backtest_params(symbols: List[str], params: Dict, data_cache: Dict) -> Dict:
    """Run backtest with specific parameters"""
    all_signals = []

    for symbol in symbols:
        df = data_cache.get(symbol)
        if df is None or len(df) < 250:
            continue

        # Scan every 7 days to speed up
        for i in range(200, len(df) - 30, 7):
            is_signal, score, metrics = check_criteria_with_params(df, i, params)

            if is_signal:
                forward = calculate_forward_returns(df, i)
                if '20d_return' in forward:
                    all_signals.append({
                        'symbol': symbol,
                        'date': df.index[i],
                        'score': score,
                        **forward
                    })

    if not all_signals:
        return None

    df_signals = pd.DataFrame(all_signals)

    # Calculate metrics
    total = len(df_signals)
    win_rate_20d = (df_signals['20d_return'] > 0).mean() * 100
    avg_return_20d = df_signals['20d_return'].mean()
    median_return_20d = df_signals['20d_return'].median()

    # Risk-adjusted metrics
    positive_returns = df_signals[df_signals['20d_return'] > 0]['20d_return']
    negative_returns = df_signals[df_signals['20d_return'] <= 0]['20d_return']
    avg_win = positive_returns.mean() if len(positive_returns) > 0 else 0
    avg_loss = negative_returns.mean() if len(negative_returns) > 0 else 0

    # Profit factor
    total_wins = positive_returns.sum() if len(positive_returns) > 0 else 0
    total_losses = abs(negative_returns.sum()) if len(negative_returns) > 0 else 1
    profit_factor = total_wins / total_losses if total_losses > 0 else 0

    # Expected value per trade
    expected_value = (win_rate_20d/100 * avg_win) + ((100-win_rate_20d)/100 * avg_loss)

    return {
        'total_signals': total,
        'win_rate_20d': win_rate_20d,
        'avg_return_20d': avg_return_20d,
        'median_return_20d': median_return_20d,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_factor': profit_factor,
        'expected_value': expected_value
    }


def run_optimization():
    """Run parameter optimization using staged approach"""

    # Test symbols - focused list for faster testing
    symbols = [
        # Volatile/parabolic movers
        "GME", "AMC", "TSLA", "NVDA", "AMD", "SMCI", "PLTR", "MSTR",
        "COIN", "RIOT", "MARA", "SHOP", "ROKU", "DKNG",
        # Large cap tech
        "META", "GOOGL", "AMZN", "MSFT", "AAPL", "NFLX",
        # Control group
        "JNJ", "PG", "KO", "WMT", "XOM", "CVX",
        # Biotech/pharma
        "LLY", "NVO", "MRNA", "ENPH", "FSLR"
    ]

    print("=" * 80)
    print("PARABOLIC FILTER OPTIMIZATION - COMPREHENSIVE")
    print("=" * 80)
    print(f"\nLoading data for {len(symbols)} symbols...")

    # Cache data
    data_cache = {}
    for i, sym in enumerate(symbols):
        print(f"  [{i+1}/{len(symbols)}] Loading {sym}...", end='\r')
        data_cache[sym] = get_historical_data(sym, years=5)
    loaded = sum(1 for v in data_cache.values() if v is not None)
    print(f"\nLoaded {loaded} symbols successfully\n")

    # STAGE 1: Test major parameters with defaults for others
    print("=" * 80)
    print("STAGE 1: Testing core parameters")
    print("=" * 80)

    stage1_grid = {
        'min_score': [5, 6, 7, 8],
        'sma_filter': [None, 0.20, 0.30, 0.50],
        'rsi_threshold': [55, 60, 65, 70],
        'vol_threshold': [1.0, 1.3, 1.5],
        'return_threshold': [0.015],  # fixed
        'drawdown_threshold': [-0.10],  # fixed
        'atr_threshold': [1.15],  # fixed
        'bb_breaks_min': [1],  # fixed
    }

    keys = list(stage1_grid.keys())
    combinations = list(product(*[stage1_grid[k] for k in keys]))

    print(f"Testing {len(combinations)} parameter combinations...\n")

    results = []
    for i, combo in enumerate(combinations):
        params = dict(zip(keys, combo))

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(combinations)}")

        result = backtest_params(symbols, params, data_cache)

        if result and result['total_signals'] >= 50:  # Min 50 signals
            results.append({
                **params,
                **result
            })

    if not results:
        print("No valid results found!")
        return

    df_results = pd.DataFrame(results)

    # Sort by different metrics
    print("\n" + "=" * 80)
    print("TOP 10 BY WIN RATE (min 50 signals)")
    print("=" * 80)
    top_wr = df_results.nlargest(10, 'win_rate_20d')
    for _, row in top_wr.iterrows():
        sma_str = f"{int(row['sma_filter']*100)}%" if row['sma_filter'] else "None"
        print(f"  Score>={int(row['min_score'])} | SMA<{sma_str:>4} | RSI>{int(row['rsi_threshold'])} | Vol>{row['vol_threshold']:.1f}x | "
              f"Ret>{row['return_threshold']*100:.1f}% | DD>{int(row['drawdown_threshold']*100)}% | "
              f"=> {int(row['total_signals']):>3} signals, WR: {row['win_rate_20d']:.1f}%, Avg: {row['avg_return_20d']:+.2f}%")

    print("\n" + "=" * 80)
    print("TOP 10 BY EXPECTED VALUE (win_rate * avg_win + loss_rate * avg_loss)")
    print("=" * 80)
    top_ev = df_results.nlargest(10, 'expected_value')
    for _, row in top_ev.iterrows():
        sma_str = f"{int(row['sma_filter']*100)}%" if row['sma_filter'] and not pd.isna(row['sma_filter']) else "None"
        print(f"  Score>={int(row['min_score'])} | SMA<{sma_str:>4} | RSI>{int(row['rsi_threshold'])} | Vol>{row['vol_threshold']:.1f}x | "
              f"Ret>{row['return_threshold']*100:.1f}% | DD>{int(row['drawdown_threshold']*100)}% | "
              f"=> {int(row['total_signals']):>3} signals, WR: {row['win_rate_20d']:.1f}%, EV: {row['expected_value']:+.2f}%")

    print("\n" + "=" * 80)
    print("TOP 10 BY PROFIT FACTOR (total_wins / total_losses)")
    print("=" * 80)
    top_pf = df_results.nlargest(10, 'profit_factor')
    for _, row in top_pf.iterrows():
        sma_str = f"{int(row['sma_filter']*100)}%" if row['sma_filter'] else "None"
        print(f"  Score>={int(row['min_score'])} | SMA<{sma_str:>4} | RSI>{int(row['rsi_threshold'])} | Vol>{row['vol_threshold']:.1f}x | "
              f"Ret>{row['return_threshold']*100:.1f}% | DD>{int(row['drawdown_threshold']*100)}% | "
              f"=> {int(row['total_signals']):>3} signals, WR: {row['win_rate_20d']:.1f}%, PF: {row['profit_factor']:.2f}")

    print("\n" + "=" * 80)
    print("TOP 10 BY AVG RETURN (with min 100 signals)")
    print("=" * 80)
    top_ret = df_results[df_results['total_signals'] >= 100].nlargest(10, 'avg_return_20d')
    for _, row in top_ret.iterrows():
        sma_str = f"{int(row['sma_filter']*100)}%" if row['sma_filter'] else "None"
        print(f"  Score>={int(row['min_score'])} | SMA<{sma_str:>4} | RSI>{int(row['rsi_threshold'])} | Vol>{row['vol_threshold']:.1f}x | "
              f"Ret>{row['return_threshold']*100:.1f}% | DD>{int(row['drawdown_threshold']*100)}% | "
              f"=> {int(row['total_signals']):>3} signals, WR: {row['win_rate_20d']:.1f}%, Avg: {row['avg_return_20d']:+.2f}%")

    # Find best balanced config
    print("\n" + "=" * 80)
    print("BEST BALANCED CONFIG (score combining WR, EV, and signal count)")
    print("=" * 80)

    # Normalize metrics
    df_results['norm_wr'] = (df_results['win_rate_20d'] - df_results['win_rate_20d'].min()) / (df_results['win_rate_20d'].max() - df_results['win_rate_20d'].min() + 0.001)
    df_results['norm_ev'] = (df_results['expected_value'] - df_results['expected_value'].min()) / (df_results['expected_value'].max() - df_results['expected_value'].min() + 0.001)
    df_results['norm_signals'] = (df_results['total_signals'] - df_results['total_signals'].min()) / (df_results['total_signals'].max() - df_results['total_signals'].min() + 0.001)
    df_results['norm_pf'] = (df_results['profit_factor'] - df_results['profit_factor'].min()) / (df_results['profit_factor'].max() - df_results['profit_factor'].min() + 0.001)

    # Combined score: 30% win rate, 30% expected value, 20% profit factor, 20% signal count
    df_results['combined_score'] = 0.3 * df_results['norm_wr'] + 0.3 * df_results['norm_ev'] + 0.2 * df_results['norm_pf'] + 0.2 * df_results['norm_signals']

    best = df_results.nlargest(10, 'combined_score')
    for _, row in best.iterrows():
        sma_str = f"{int(row['sma_filter']*100)}%" if row['sma_filter'] else "None"
        print(f"\n  Score>={int(row['min_score'])} | SMA<{sma_str:>4} | RSI>{int(row['rsi_threshold'])} | Vol>{row['vol_threshold']:.1f}x")
        print(f"    Signals: {int(row['total_signals'])} | Win Rate: {row['win_rate_20d']:.1f}% | "
              f"Avg Return: {row['avg_return_20d']:+.2f}% | Profit Factor: {row['profit_factor']:.2f} | EV: {row['expected_value']:+.2f}%")

    # STAGE 2: Fine-tune with best parameters from stage 1
    print("\n\n" + "=" * 80)
    print("STAGE 2: Fine-tuning secondary parameters with best core settings")
    print("=" * 80)

    # Get best core params
    best_row = best.iloc[0]
    best_core = {
        'min_score': int(best_row['min_score']),
        'sma_filter': best_row['sma_filter'],
        'rsi_threshold': int(best_row['rsi_threshold']),
        'vol_threshold': best_row['vol_threshold'],
    }

    print(f"\nUsing best core params: {best_core}")

    stage2_grid = {
        'min_score': [best_core['min_score']],
        'sma_filter': [best_core['sma_filter']],
        'rsi_threshold': [best_core['rsi_threshold']],
        'vol_threshold': [best_core['vol_threshold']],
        'return_threshold': [0.005, 0.01, 0.015, 0.02, 0.025, 0.03],
        'drawdown_threshold': [-0.05, -0.08, -0.10, -0.12, -0.15, -0.20],
        'atr_threshold': [1.0, 1.05, 1.10, 1.15, 1.20, 1.25, 1.30],
        'bb_breaks_min': [0, 1, 2, 3],
    }

    keys2 = list(stage2_grid.keys())
    combinations2 = list(product(*[stage2_grid[k] for k in keys2]))

    print(f"Testing {len(combinations2)} stage 2 combinations...\n")

    results2 = []
    for i, combo in enumerate(combinations2):
        params = dict(zip(keys2, combo))
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{len(combinations2)}")
        result = backtest_params(symbols, params, data_cache)
        if result and result['total_signals'] >= 30:
            results2.append({**params, **result})

    if results2:
        df_results2 = pd.DataFrame(results2)

        # Normalize and score
        df_results2['norm_wr'] = (df_results2['win_rate_20d'] - df_results2['win_rate_20d'].min()) / (df_results2['win_rate_20d'].max() - df_results2['win_rate_20d'].min() + 0.001)
        df_results2['norm_ev'] = (df_results2['expected_value'] - df_results2['expected_value'].min()) / (df_results2['expected_value'].max() - df_results2['expected_value'].min() + 0.001)
        df_results2['norm_pf'] = (df_results2['profit_factor'] - df_results2['profit_factor'].min()) / (df_results2['profit_factor'].max() - df_results2['profit_factor'].min() + 0.001)
        df_results2['combined_score'] = 0.35 * df_results2['norm_wr'] + 0.35 * df_results2['norm_ev'] + 0.30 * df_results2['norm_pf']

        print("\nTOP 10 FINE-TUNED CONFIGURATIONS:")
        print("-" * 80)
        best2 = df_results2.nlargest(10, 'combined_score')
        for _, row in best2.iterrows():
            print(f"  Ret>{row['return_threshold']*100:.1f}% | DD>{int(row['drawdown_threshold']*100)}% | "
                  f"ATR>{row['atr_threshold']:.2f} | BB>={int(row['bb_breaks_min'])}")
            print(f"    Signals: {int(row['total_signals'])} | WR: {row['win_rate_20d']:.1f}% | "
                  f"Avg: {row['avg_return_20d']:+.2f}% | PF: {row['profit_factor']:.2f}\n")

        # Final optimal config
        final_best = best2.iloc[0]
        print("\n" + "=" * 80)
        print("FINAL OPTIMAL CONFIGURATION")
        print("=" * 80)
        print(f"""
    min_score:          {int(best_core['min_score'])}
    sma_filter:         {f"{int(best_core['sma_filter']*100)}% above 200 SMA" if best_core['sma_filter'] else "None"}
    rsi_threshold:      > {int(best_core['rsi_threshold'])}
    vol_threshold:      >= {best_core['vol_threshold']:.1f}x
    return_threshold:   > {final_best['return_threshold']*100:.1f}%
    drawdown_threshold: > {int(final_best['drawdown_threshold']*100)}%
    atr_threshold:      >= {final_best['atr_threshold']:.2f}x
    bb_breaks_min:      >= {int(final_best['bb_breaks_min'])}

    EXPECTED PERFORMANCE:
    - Signals: {int(final_best['total_signals'])}
    - Win Rate (20d): {final_best['win_rate_20d']:.1f}%
    - Avg Return (20d): {final_best['avg_return_20d']:+.2f}%
    - Profit Factor: {final_best['profit_factor']:.2f}
    - Expected Value: {final_best['expected_value']:+.2f}%
        """)

        # Save stage 2 results
        df_results2.to_csv("parabolic_optimization_stage2.csv", index=False)

    # Save results
    output_path = "parabolic_optimization_results.csv"
    df_results.to_csv(output_path, index=False)
    print(f"\nFull results saved to: {output_path}")


if __name__ == "__main__":
    run_optimization()
