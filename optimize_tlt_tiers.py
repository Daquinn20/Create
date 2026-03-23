"""
TLT Tier Optimization - Find optimal thresholds for each tier
Tests different parameter combinations to maximize win rates
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from itertools import product
import warnings
warnings.filterwarnings('ignore')

# Full S&P 500 stocks by sector
SP500_BY_SECTOR = {
    'Information Technology': [
        'AAPL', 'MSFT', 'NVDA', 'AVGO', 'ORCL', 'CRM', 'AMD', 'CSCO', 'ACN', 'ADBE',
        'IBM', 'INTC', 'INTU', 'TXN', 'QCOM', 'AMAT', 'NOW', 'PANW', 'MU', 'ADI',
        'LRCX', 'KLAC', 'SNPS', 'CDNS', 'MCHP', 'APH', 'MSI', 'FTNT', 'HPQ', 'NXPI',
        'TEL', 'DELL', 'ROP', 'ON', 'WDC', 'HPE', 'KEYS', 'FSLR', 'MPWR',
        'TYL', 'NTAP', 'PTC', 'ZBRA', 'TRMB', 'JBL', 'GEN', 'SWKS', 'EPAM', 'AKAM',
        'FFIV', 'QRVO'
    ],
    'Health Care': [
        'UNH', 'JNJ', 'LLY', 'ABBV', 'MRK', 'PFE', 'TMO', 'ABT', 'DHR', 'AMGN',
        'BMY', 'MDT', 'ISRG', 'GILD', 'CVS', 'ELV', 'VRTX', 'SYK', 'CI', 'REGN',
        'ZTS', 'BSX', 'BDX', 'HUM', 'MCK', 'HCA', 'MRNA', 'IDXX', 'EW', 'DXCM',
        'A', 'IQV', 'MTD', 'GEHC', 'RMD', 'CAH', 'CNC', 'ALGN', 'BIIB', 'LH',
        'COR', 'BAX', 'WAT', 'HOLX', 'MOH', 'VTRS', 'DGX', 'INCY', 'HSIC', 'CRL', 'BIO'
    ],
    'Financials': [
        'BRK-B', 'JPM', 'V', 'MA', 'BAC', 'WFC', 'GS', 'MS', 'SPGI', 'AXP',
        'BLK', 'C', 'SCHW', 'PGR', 'CB', 'ICE', 'CME', 'AON', 'PNC',
        'USB', 'MCO', 'AJG', 'TFC', 'MET', 'AFL', 'AIG', 'TRV', 'ALL', 'PRU',
        'MSCI', 'BK', 'COF', 'AMP', 'NDAQ', 'FITB', 'STT', 'HIG',
        'WTW', 'RJF', 'ACGL', 'TROW', 'HBAN', 'CINF', 'MTB', 'RF', 'CBOE',
        'KEY', 'CFG', 'FDS', 'NTRS', 'SYF', 'BRO', 'WRB', 'L', 'GL', 'IVZ', 'JKHY', 'AIZ', 'BEN', 'ZION'
    ],
    'Consumer Discretionary': [
        'AMZN', 'TSLA', 'HD', 'MCD', 'NKE', 'LOW', 'BKNG', 'SBUX', 'TJX', 'CMG',
        'ORLY', 'MAR', 'GM', 'AZO', 'F', 'ROST', 'HLT', 'YUM', 'DHI', 'LULU',
        'LVS', 'NVR', 'EBAY', 'LEN', 'DECK', 'GPC', 'PHM', 'APTV', 'CCL', 'BBY',
        'DRI', 'ULTA', 'POOL', 'GRMN', 'WYNN', 'RCL', 'TPR', 'KMX', 'EXPE', 'BWA',
        'TSCO', 'MGM', 'DPZ', 'CZR', 'HAS', 'RL', 'MHK', 'NWL', 'NCLH'
    ],
    'Communication Services': [
        'GOOGL', 'GOOG', 'META', 'NFLX', 'DIS', 'CMCSA', 'VZ', 'T', 'TMUS', 'CHTR',
        'EA', 'WBD', 'TTWO', 'OMC', 'LYV', 'MTCH', 'FOXA', 'FOX', 'NWS', 'NWSA'
    ],
    'Industrials': [
        'GE', 'CAT', 'RTX', 'UNP', 'HON', 'UPS', 'BA', 'DE', 'LMT', 'ADP',
        'ETN', 'ITW', 'GD', 'NOC', 'WM', 'CSX', 'NSC', 'TT', 'EMR', 'PH',
        'CTAS', 'CARR', 'JCI', 'PCAR', 'TDG', 'CPRT', 'GWW', 'ODFL', 'PAYX', 'FAST',
        'AME', 'VRSK', 'RSG', 'FDX', 'CMI', 'PWR', 'IR', 'HWM', 'ROK', 'EFX',
        'WAB', 'OTIS', 'HUBB', 'XYL', 'DOV', 'IEX', 'LHX', 'LDOS', 'DAL',
        'URI', 'SWK', 'BR', 'J', 'MAS', 'AXON', 'NDSN', 'EXPD', 'TXT',
        'SNA', 'PNR', 'CHRW', 'ALLE', 'AOS', 'LUV', 'UAL', 'AAL', 'PAYC'
    ],
    'Consumer Staples': [
        'WMT', 'PG', 'COST', 'KO', 'PEP', 'PM', 'MDLZ', 'MO', 'CL', 'TGT',
        'ADM', 'GIS', 'STZ', 'KMB', 'SYY', 'KHC', 'HSY', 'KDP', 'MKC', 'EL',
        'CLX', 'TSN', 'MNST', 'KR', 'CAG', 'SJM', 'CHD', 'HRL', 'BG', 'CPB', 'TAP'
    ],
    'Energy': [
        'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'MPC', 'PSX', 'VLO', 'OXY',
        'WMB', 'KMI', 'DVN', 'HAL', 'FANG', 'BKR', 'OKE', 'TRGP', 'CTRA', 'EQT', 'APA'
    ],
    'Utilities': [
        'NEE', 'SO', 'DUK', 'CEG', 'SRE', 'AEP', 'D', 'PCG', 'EXC', 'XEL',
        'ED', 'PEG', 'WEC', 'AWK', 'EIX', 'ETR', 'ES', 'DTE', 'PPL', 'FE',
        'AEE', 'CMS', 'CNP', 'EVRG', 'ATO', 'NI', 'LNT', 'NRG', 'PNW'
    ],
    'Real Estate': [
        'PLD', 'AMT', 'EQIX', 'WELL', 'SPG', 'PSA', 'O', 'DLR', 'CCI', 'VICI',
        'CBRE', 'AVB', 'EXR', 'IRM', 'SBAC', 'WY', 'ARE', 'EQR', 'MAA', 'VTR',
        'ESS', 'INVH', 'KIM', 'UDR', 'HST', 'CPT', 'REG', 'BXP', 'FRT'
    ],
    'Materials': [
        'LIN', 'APD', 'SHW', 'FCX', 'NEM', 'ECL', 'NUE', 'DOW', 'DD', 'VMC',
        'PPG', 'CTVA', 'MLM', 'IFF', 'LYB', 'ALB', 'BALL', 'STLD', 'PKG', 'AVY',
        'CF', 'IP', 'MOS', 'FMC', 'CE', 'EMN', 'AMCR', 'SEE'
    ]
}

# Flatten to get all stocks
ALL_SP500_STOCKS = []
for sector, stocks in SP500_BY_SECTOR.items():
    ALL_SP500_STOCKS.extend(stocks)

def calculate_rsi(data, period=14):
    delta = data.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(100)

def calculate_mfi(high, low, close, volume, period=14):
    tp = (high + low + close) / 3
    rmf = tp * volume
    tp_diff = tp.diff()
    mf_positive = np.where(tp_diff > 0, rmf, 0)
    mf_negative = np.where(tp_diff < 0, rmf, 0)
    mf_pos_sum = pd.Series(mf_positive, index=close.index).rolling(window=period).sum()
    mf_neg_sum = pd.Series(mf_negative, index=close.index).rolling(window=period).sum()
    return 100 - (100 / (1 + mf_pos_sum / (mf_neg_sum + 1e-10)))

def calculate_cmf(high, low, close, volume, period=20):
    hl_range = (high - low).replace(0, 1e-10)
    mfm = ((close - low) - (high - close)) / hl_range
    mfv = mfm * volume
    return mfv.rolling(window=period).sum() / volume.rolling(window=period).sum()

def calculate_mansfield_rs(stock_close, benchmark_close, lookback=252):
    common_idx = stock_close.index.intersection(benchmark_close.index)
    if len(common_idx) < 50:
        return pd.Series(0, index=stock_close.index)
    stock_aligned = stock_close.loc[common_idx]
    bench_aligned = benchmark_close.loc[common_idx]
    rs_ratio = (stock_aligned / bench_aligned) * 100
    rs_sma = rs_ratio.rolling(window=min(lookback, len(rs_ratio))).mean()
    mrs = ((rs_ratio / rs_sma) - 1) * 10
    result = pd.Series(index=stock_close.index, dtype=float)
    result.loc[common_idx] = mrs
    return result.fillna(0)

def load_stock_data(symbols, spy_close, years=5):
    """Load and prepare data for all stocks"""
    all_data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365 + 300)

    total = len(symbols)
    loaded = 0
    failed = 0

    for i, symbol in enumerate(symbols):
        try:
            df = yf.download(symbol, start=start_date, end=end_date, progress=False)
            if df.empty or len(df) < 300:
                failed += 1
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            close = df['Close']
            high = df['High']
            low = df['Low']
            volume = df['Volume']

            rsi = calculate_rsi(close, 14)
            mfi = calculate_mfi(high, low, close, volume, 14)
            cmf = calculate_cmf(high, low, close, volume, 20)
            mrs = calculate_mansfield_rs(close, spy_close, 252)
            lr_ratio = mfi / (rsi + 1e-10)

            ma20 = close.rolling(20).mean()
            ma50 = close.rolling(50).mean()
            ma200 = close.rolling(200).mean()

            signals = pd.DataFrame(index=df.index)
            signals['Symbol'] = symbol
            signals['Close'] = close
            signals['RSI'] = rsi
            signals['MFI'] = mfi
            signals['CMF'] = cmf
            signals['MRS'] = mrs
            signals['LR_Ratio'] = lr_ratio
            signals['Above_MA20'] = close > ma20
            signals['Above_MA50'] = close > ma50
            signals['Above_MA200'] = close > ma200
            signals['MRS_Rising'] = mrs > mrs.shift(1)
            signals['CMF_Rising'] = cmf > cmf.shift(5)
            signals['Dist_MA50'] = ((close - ma50) / ma50) * 100

            # Forward returns
            signals['Fwd_21d'] = close.shift(-21) / close - 1
            signals['Fwd_63d'] = close.shift(-63) / close - 1
            signals['Fwd_126d'] = close.shift(-126) / close - 1

            # Drop warmup and forward-looking NaN
            signals = signals.iloc[250:-130].dropna()
            all_data.append(signals)
            loaded += 1

            # Progress update every 50 stocks
            if (i + 1) % 50 == 0:
                print(f"  Progress: {i+1}/{total} stocks ({loaded} loaded, {failed} failed)")

        except Exception as e:
            failed += 1
            continue

    print(f"  Final: {loaded}/{total} stocks loaded, {failed} failed")
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()


def test_tier_params(data, tier_name, params):
    """Test a specific parameter set for a tier and return win rate"""

    if tier_name == 'OVERSOLD':
        lr_min, cmf_min, rsi_max, mrs_rising_req = params
        mask = (
            (data['LR_Ratio'] >= lr_min) &
            (data['CMF'] > cmf_min) &
            (data['RSI'] < rsi_max)
        )
        if mrs_rising_req:
            mask &= data['MRS_Rising']

    elif tier_name == 'SPRING':
        cmf_min, lr_min, above_ma50_req, dist_ma50_min = params
        mask = (
            (data['CMF'] > cmf_min) &
            (data['LR_Ratio'] > lr_min)
        )
        if above_ma50_req == False:
            mask &= ~data['Above_MA50']
        if dist_ma50_min is not None:
            mask &= (data['Dist_MA50'] < dist_ma50_min)

    elif tier_name == 'SURGE':
        lr_min, cmf_min, rsi_min, mrs_min = params
        mask = (
            (data['LR_Ratio'] >= lr_min) &
            (data['CMF'] > cmf_min) &
            (data['RSI'] >= rsi_min) &
            (data['MRS_Rising'])
        )
        if mrs_min is not None:
            mask &= (data['MRS'] >= mrs_min)

    elif tier_name == 'LEADER':
        lr_min, cmf_min, mrs_min, above_ma200_req, rsi_min = params
        mask = (
            (data['LR_Ratio'] >= lr_min) &
            (data['CMF'] > cmf_min) &
            (data['MRS'] >= mrs_min) &
            (data['MRS_Rising'])
        )
        if above_ma200_req:
            mask &= data['Above_MA200']
        if rsi_min is not None:
            mask &= (data['RSI'] >= rsi_min)
    else:
        return None

    subset = data[mask]
    if len(subset) < 30:
        return None

    win_rate_1m = (subset['Fwd_21d'] > 0).mean() * 100
    win_rate_3m = (subset['Fwd_63d'] > 0).mean() * 100
    win_rate_6m = (subset['Fwd_126d'] > 0).mean() * 100
    avg_ret_3m = subset['Fwd_63d'].mean() * 100

    return {
        'params': params,
        'signals': len(subset),
        'win_1m': win_rate_1m,
        'win_3m': win_rate_3m,
        'win_6m': win_rate_6m,
        'avg_ret_3m': avg_ret_3m,
        'score': win_rate_3m + avg_ret_3m  # Combined score
    }


def optimize_oversold(data):
    """Find optimal OVERSOLD parameters"""
    print("\n" + "="*70)
    print("OPTIMIZING OVERSOLD TIER")
    print("="*70)
    print("Current: LR >= 1.25, CMF > 0.05, MRS_Rising, RSI < 40")

    # Parameter grid
    lr_mins = [1.0, 1.15, 1.25, 1.35, 1.5]
    cmf_mins = [0.0, 0.03, 0.05, 0.08, 0.1]
    rsi_maxs = [30, 35, 40, 45, 50]
    mrs_rising = [True, False]

    results = []
    for params in product(lr_mins, cmf_mins, rsi_maxs, mrs_rising):
        result = test_tier_params(data, 'OVERSOLD', params)
        if result:
            results.append(result)

    if not results:
        print("No valid parameter combinations found")
        return None

    # Sort by score
    results.sort(key=lambda x: x['score'], reverse=True)

    print(f"\nTop 5 parameter combinations:")
    print("-" * 70)
    for i, r in enumerate(results[:5]):
        lr, cmf, rsi, mrs = r['params']
        print(f"{i+1}. LR>={lr}, CMF>{cmf}, RSI<{rsi}, MRS_Rising={mrs}")
        print(f"   Signals: {r['signals']:,} | Win 3M: {r['win_3m']:.1f}% | Avg Ret: {r['avg_ret_3m']:.2f}%")

    return results[0]


def optimize_spring(data):
    """Find optimal SPRING parameters"""
    print("\n" + "="*70)
    print("OPTIMIZING SPRING TIER")
    print("="*70)
    print("Current: Below MA50, CMF > 0, LR > 1.0")

    # Parameter grid
    cmf_mins = [-0.05, 0.0, 0.03, 0.05, 0.1]
    lr_mins = [0.8, 0.9, 1.0, 1.1, 1.2]
    above_ma50 = [False, None]  # None = don't filter
    dist_ma50_mins = [None, -5, -10, -15]  # Extended below MA50

    results = []
    for params in product(cmf_mins, lr_mins, above_ma50, dist_ma50_mins):
        result = test_tier_params(data, 'SPRING', params)
        if result:
            results.append(result)

    results.sort(key=lambda x: x['score'], reverse=True)

    print(f"\nTop 5 parameter combinations:")
    print("-" * 70)
    for i, r in enumerate(results[:5]):
        cmf, lr, ma50, dist = r['params']
        print(f"{i+1}. CMF>{cmf}, LR>{lr}, BelowMA50={ma50==False}, Dist<{dist}")
        print(f"   Signals: {r['signals']:,} | Win 3M: {r['win_3m']:.1f}% | Avg Ret: {r['avg_ret_3m']:.2f}%")

    return results[0] if results else None


def optimize_surge(data):
    """Find optimal SURGE parameters"""
    print("\n" + "="*70)
    print("OPTIMIZING SURGE TIER")
    print("="*70)
    print("Current: LR >= 1.25, CMF > 0.05, MRS_Rising, RSI >= 40")

    # Parameter grid
    lr_mins = [1.1, 1.2, 1.25, 1.3, 1.4, 1.5]
    cmf_mins = [0.03, 0.05, 0.08, 0.1, 0.15]
    rsi_mins = [40, 45, 50, 55, 60]
    mrs_mins = [None, 0, 0.5, 1.0]  # Add MRS threshold

    results = []
    for params in product(lr_mins, cmf_mins, rsi_mins, mrs_mins):
        result = test_tier_params(data, 'SURGE', params)
        if result:
            results.append(result)

    results.sort(key=lambda x: x['score'], reverse=True)

    print(f"\nTop 5 parameter combinations:")
    print("-" * 70)
    for i, r in enumerate(results[:5]):
        lr, cmf, rsi, mrs = r['params']
        print(f"{i+1}. LR>={lr}, CMF>{cmf}, RSI>={rsi}, MRS>={mrs}")
        print(f"   Signals: {r['signals']:,} | Win 3M: {r['win_3m']:.1f}% | Avg Ret: {r['avg_ret_3m']:.2f}%")

    return results[0] if results else None


def optimize_leader(data):
    """Find optimal LEADER parameters"""
    print("\n" + "="*70)
    print("OPTIMIZING LEADER TIER")
    print("="*70)
    print("Current: LR >= 1.0, CMF > 0.1, MRS >= 0, MRS_Rising, Above_MA200")

    # Parameter grid
    lr_mins = [0.9, 1.0, 1.1, 1.15, 1.2]
    cmf_mins = [0.05, 0.08, 0.1, 0.12, 0.15]
    mrs_mins = [0, 0.5, 1.0, 1.5]
    above_ma200 = [True, False]
    rsi_mins = [None, 50, 55, 60]

    results = []
    for params in product(lr_mins, cmf_mins, mrs_mins, above_ma200, rsi_mins):
        result = test_tier_params(data, 'LEADER', params)
        if result:
            results.append(result)

    results.sort(key=lambda x: x['score'], reverse=True)

    print(f"\nTop 5 parameter combinations:")
    print("-" * 70)
    for i, r in enumerate(results[:5]):
        lr, cmf, mrs, ma200, rsi = r['params']
        print(f"{i+1}. LR>={lr}, CMF>{cmf}, MRS>={mrs}, AboveMA200={ma200}, RSI>={rsi}")
        print(f"   Signals: {r['signals']:,} | Win 3M: {r['win_3m']:.1f}% | Avg Ret: {r['avg_ret_3m']:.2f}%")

    return results[0] if results else None


def test_additional_filters(data):
    """Test additional filters that could improve any tier"""
    print("\n" + "="*70)
    print("TESTING ADDITIONAL FILTERS (applicable to all tiers)")
    print("="*70)

    # Extended from MA50 filter
    print("\n1. EXTENDED FROM MA50 FILTER")
    print("-" * 50)
    for dist_thresh in [-15, -10, -5, 5, 10, 15]:
        if dist_thresh < 0:
            mask = data['Dist_MA50'] < dist_thresh
            label = f"Extended {abs(dist_thresh)}%+ BELOW MA50"
        else:
            mask = data['Dist_MA50'] > dist_thresh
            label = f"Extended {dist_thresh}%+ ABOVE MA50"

        subset = data[mask]
        if len(subset) < 50:
            continue
        win_3m = (subset['Fwd_63d'] > 0).mean() * 100
        avg_ret = subset['Fwd_63d'].mean() * 100
        print(f"  {label}: {len(subset):,} signals | Win 3M: {win_3m:.1f}% | Avg Ret: {avg_ret:.2f}%")

    # RSI extreme filters
    print("\n2. RSI EXTREME FILTERS")
    print("-" * 50)
    for rsi_low, rsi_high in [(0, 30), (30, 40), (40, 50), (50, 60), (60, 70), (70, 100)]:
        mask = (data['RSI'] >= rsi_low) & (data['RSI'] < rsi_high)
        subset = data[mask]
        if len(subset) < 50:
            continue
        win_3m = (subset['Fwd_63d'] > 0).mean() * 100
        avg_ret = subset['Fwd_63d'].mean() * 100
        print(f"  RSI {rsi_low}-{rsi_high}: {len(subset):,} signals | Win 3M: {win_3m:.1f}% | Avg Ret: {avg_ret:.2f}%")

    # MFI filters
    print("\n3. MFI EXTREME FILTERS")
    print("-" * 50)
    for mfi_low, mfi_high in [(0, 30), (30, 50), (50, 70), (70, 100)]:
        mask = (data['MFI'] >= mfi_low) & (data['MFI'] < mfi_high)
        subset = data[mask]
        if len(subset) < 50:
            continue
        win_3m = (subset['Fwd_63d'] > 0).mean() * 100
        avg_ret = subset['Fwd_63d'].mean() * 100
        print(f"  MFI {mfi_low}-{mfi_high}: {len(subset):,} signals | Win 3M: {win_3m:.1f}% | Avg Ret: {avg_ret:.2f}%")

    # CMF filters
    print("\n4. CMF FILTERS")
    print("-" * 50)
    for cmf_thresh in [-0.1, -0.05, 0, 0.05, 0.1, 0.15, 0.2]:
        mask = data['CMF'] > cmf_thresh
        subset = data[mask]
        if len(subset) < 50:
            continue
        win_3m = (subset['Fwd_63d'] > 0).mean() * 100
        avg_ret = subset['Fwd_63d'].mean() * 100
        print(f"  CMF > {cmf_thresh}: {len(subset):,} signals | Win 3M: {win_3m:.1f}% | Avg Ret: {avg_ret:.2f}%")

    # MRS filters
    print("\n5. MANSFIELD RS FILTERS")
    print("-" * 50)
    for mrs_thresh in [-1, -0.5, 0, 0.5, 1.0, 1.5, 2.0]:
        mask = data['MRS'] > mrs_thresh
        subset = data[mask]
        if len(subset) < 50:
            continue
        win_3m = (subset['Fwd_63d'] > 0).mean() * 100
        avg_ret = subset['Fwd_63d'].mean() * 100
        print(f"  MRS > {mrs_thresh}: {len(subset):,} signals | Win 3M: {win_3m:.1f}% | Avg Ret: {avg_ret:.2f}%")

    # 52-week position filter
    print("\n6. 52-WEEK RANGE POSITION")
    print("-" * 50)
    if 'Pct_52w_Range' not in data.columns:
        # Calculate it
        data['Pct_52w_Range'] = 50  # Placeholder
    for pct_low, pct_high in [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100)]:
        mask = (data['Pct_52w_Range'] >= pct_low) & (data['Pct_52w_Range'] < pct_high)
        subset = data[mask]
        if len(subset) < 50:
            continue
        win_3m = (subset['Fwd_63d'] > 0).mean() * 100
        avg_ret = subset['Fwd_63d'].mean() * 100
        print(f"  52W Range {pct_low}-{pct_high}%: {len(subset):,} signals | Win 3M: {win_3m:.1f}% | Avg Ret: {avg_ret:.2f}%")


if __name__ == "__main__":
    print("="*70)
    print("TLT TIER OPTIMIZATION - FULL S&P 500")
    print("="*70)
    print(f"Testing {len(ALL_SP500_STOCKS)} stocks across {len(SP500_BY_SECTOR)} sectors")
    print("="*70)

    # Download SPY benchmark
    print("\nDownloading SPY benchmark...")
    spy = yf.download('SPY', start=datetime.now() - timedelta(days=5*365+300),
                      end=datetime.now(), progress=False)
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = spy.columns.get_level_values(0)
    spy_close = spy['Close']

    # Load all S&P 500 stock data
    print("\nLoading S&P 500 stock data...")
    data = load_stock_data(ALL_SP500_STOCKS, spy_close)

    if data.empty:
        print("ERROR: No data loaded")
        exit(1)

    print(f"\nTotal data points: {len(data):,}")

    # Show current baseline
    print("\n" + "="*70)
    print("CURRENT BASELINE (3-Month Forward Returns)")
    print("="*70)

    # Apply current tier logic to sample
    def current_tier(row):
        if row['LR_Ratio'] > 1.5 and not row['MRS_Rising']:
            return "DANGER"
        if row['LR_Ratio'] >= 1.0 and row['CMF'] > 0.1 and row['MRS'] >= 0 and row['MRS_Rising'] and row['Above_MA200']:
            return "LEADER"
        if row['LR_Ratio'] >= 1.25 and row['CMF'] > 0.05 and row['MRS_Rising'] and row['RSI'] >= 40:
            return "SURGE"
        if row['LR_Ratio'] >= 1.25 and row['CMF'] > 0.05 and row['MRS_Rising'] and row['RSI'] < 40:
            return "OVERSOLD"
        if not row['Above_MA50'] and row['CMF'] > 0 and row['LR_Ratio'] > 1.0:
            return "SPRING"
        return "NEUTRAL"

    data['Current_Tier'] = data.apply(current_tier, axis=1)

    for tier in ['LEADER', 'SURGE', 'OVERSOLD', 'SPRING', 'DANGER']:
        subset = data[data['Current_Tier'] == tier]
        if len(subset) < 10:
            continue
        win_3m = (subset['Fwd_63d'] > 0).mean() * 100
        avg_ret = subset['Fwd_63d'].mean() * 100
        print(f"  {tier:10}: {len(subset):5,} signals | Win 3M: {win_3m:.1f}% | Avg Ret: {avg_ret:+.2f}%")

    # Optimize each tier
    best_oversold = optimize_oversold(data)
    best_spring = optimize_spring(data)
    best_surge = optimize_surge(data)
    best_leader = optimize_leader(data)

    # Test additional filters
    test_additional_filters(data)

    # Final summary
    print("\n" + "="*70)
    print("OPTIMIZATION SUMMARY - RECOMMENDED CHANGES")
    print("="*70)

    print("\n1. OVERSOLD:")
    if best_oversold:
        lr, cmf, rsi, mrs = best_oversold['params']
        print(f"   CURRENT: LR >= 1.25, CMF > 0.05, MRS_Rising, RSI < 40")
        print(f"   OPTIMAL: LR >= {lr}, CMF > {cmf}, MRS_Rising={mrs}, RSI < {rsi}")
        print(f"   IMPROVEMENT: {best_oversold['win_3m']:.1f}% win rate, {best_oversold['avg_ret_3m']:.2f}% avg return")

    print("\n2. SPRING:")
    if best_spring:
        cmf, lr, ma50, dist = best_spring['params']
        print(f"   CURRENT: Below MA50, CMF > 0, LR > 1.0")
        print(f"   OPTIMAL: CMF > {cmf}, LR > {lr}, BelowMA50={ma50==False}, Dist<{dist}")
        print(f"   IMPROVEMENT: {best_spring['win_3m']:.1f}% win rate, {best_spring['avg_ret_3m']:.2f}% avg return")

    print("\n3. SURGE:")
    if best_surge:
        lr, cmf, rsi, mrs = best_surge['params']
        print(f"   CURRENT: LR >= 1.25, CMF > 0.05, MRS_Rising, RSI >= 40")
        print(f"   OPTIMAL: LR >= {lr}, CMF > {cmf}, RSI >= {rsi}, MRS >= {mrs}")
        print(f"   IMPROVEMENT: {best_surge['win_3m']:.1f}% win rate, {best_surge['avg_ret_3m']:.2f}% avg return")

    print("\n4. LEADER:")
    if best_leader:
        lr, cmf, mrs, ma200, rsi = best_leader['params']
        print(f"   CURRENT: LR >= 1.0, CMF > 0.1, MRS >= 0, MRS_Rising, Above_MA200")
        print(f"   OPTIMAL: LR >= {lr}, CMF > {cmf}, MRS >= {mrs}, AboveMA200={ma200}, RSI >= {rsi}")
        print(f"   IMPROVEMENT: {best_leader['win_3m']:.1f}% win rate, {best_leader['avg_ret_3m']:.2f}% avg return")

    print("\n" + "="*70)
    print("DONE")
    print("="*70)
