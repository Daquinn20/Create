"""
Full S&P 500 TLT Tier Backtest - 5 Year Analysis
Runs analysis on all ~500 S&P 500 stocks
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import io
import os
import sys
from dotenv import load_dotenv

warnings.filterwarnings('ignore')
load_dotenv()

# ============================================================================
# S&P 500 STOCKS BY SECTOR
# ============================================================================

SP500_BY_SECTOR = {
    'Information Technology': [
        'AAPL', 'MSFT', 'NVDA', 'AVGO', 'ORCL', 'CRM', 'AMD', 'CSCO', 'ACN', 'ADBE',
        'IBM', 'INTC', 'INTU', 'TXN', 'QCOM', 'AMAT', 'NOW', 'PANW', 'MU', 'ADI',
        'LRCX', 'KLAC', 'SNPS', 'CDNS', 'MCHP', 'APH', 'MSI', 'FTNT', 'HPQ', 'NXPI',
        'TEL', 'DELL', 'ROP', 'ON', 'WDC', 'HPE', 'KEYS', 'ANSS', 'FSLR', 'MPWR',
        'TYL', 'NTAP', 'PTC', 'ZBRA', 'TRMB', 'JBL', 'GEN', 'SWKS', 'EPAM', 'AKAM',
        'JNPR', 'FFIV', 'QRVO'
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
        'BLK', 'C', 'SCHW', 'PGR', 'CB', 'MMC', 'ICE', 'CME', 'AON', 'PNC',
        'USB', 'MCO', 'AJG', 'TFC', 'MET', 'AFL', 'AIG', 'TRV', 'ALL', 'PRU',
        'MSCI', 'BK', 'COF', 'AMP', 'NDAQ', 'FITB', 'STT', 'DFS', 'HIG',
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
        'EA', 'WBD', 'TTWO', 'OMC', 'LYV', 'PARA', 'MTCH', 'IPG', 'FOXA', 'FOX', 'NWS', 'NWSA'
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
        'K', 'CLX', 'TSN', 'MNST', 'KR', 'CAG', 'SJM', 'CHD', 'HRL', 'WBA', 'BG', 'CPB', 'TAP'
    ],
    'Energy': [
        'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'MPC', 'PSX', 'VLO', 'OXY',
        'WMB', 'HES', 'KMI', 'DVN', 'HAL', 'FANG', 'BKR', 'OKE', 'TRGP', 'CTRA', 'EQT', 'MRO', 'APA'
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
        'CF', 'IP', 'MOS', 'FMC', 'CE', 'EMN', 'AMCR', 'WRK', 'SEE'
    ]
}

# ============================================================================
# INDICATOR CALCULATIONS
# ============================================================================

def calculate_rsi(data: pd.Series, period: int = 14) -> pd.Series:
    delta = data.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(100)

def calculate_mfi(high, low, close, volume, period=14):
    tp = (high + low + close) / 3
    rmf = tp * volume
    tp_diff = tp.diff()
    mf_positive = np.where(tp_diff > 0, rmf, 0)
    mf_negative = np.where(tp_diff < 0, rmf, 0)
    mf_pos_sum = pd.Series(mf_positive, index=close.index).rolling(window=period).sum()
    mf_neg_sum = pd.Series(mf_negative, index=close.index).rolling(window=period).sum()
    mfr = mf_pos_sum / (mf_neg_sum + 1e-10)
    return 100 - (100 / (1 + mfr))

def calculate_cmf(high, low, close, volume, period=20):
    hl_range = (high - low).replace(0, 1e-10)
    mfm = ((close - low) - (high - close)) / hl_range
    mfv = mfm * volume
    return mfv.rolling(window=period).sum() / volume.rolling(window=period).sum()

def calculate_mansfield_rs(stock_close, benchmark_close, lookback=252):
    common_idx = stock_close.index.intersection(benchmark_close.index)
    if len(common_idx) < 50:
        return pd.Series(0, index=stock_close.index)
    if len(common_idx) < lookback:
        lookback = max(50, len(common_idx) // 2)
    stock_aligned = stock_close.loc[common_idx]
    bench_aligned = benchmark_close.loc[common_idx]
    rs_ratio = (stock_aligned / bench_aligned) * 100
    rs_sma = rs_ratio.rolling(window=min(lookback, len(rs_ratio))).mean()
    mrs = ((rs_ratio / rs_sma) - 1) * 10
    result = pd.Series(index=stock_close.index, dtype=float)
    result.loc[common_idx] = mrs
    return result.fillna(0)

def calculate_sma(data, period):
    return data.rolling(window=period).mean()

def classify_tlt_tier(lr_ratio, cmf, cmf_rising, mrs, mrs_rising, above_ma20, above_ma50, above_ma200, rsi):
    if lr_ratio > 1.5 and not mrs_rising:
        return "DANGER"
    if lr_ratio >= 1.0 and cmf > 0.1 and mrs >= 0 and mrs_rising and above_ma200:
        return "LEADER"
    if lr_ratio >= 1.25 and cmf > 0.05 and mrs_rising and rsi >= 40:
        return "SURGE"
    if lr_ratio >= 1.25 and cmf > 0.05 and mrs_rising and rsi < 40:
        return "OVERSOLD"
    if not above_ma50 and cmf > 0 and lr_ratio > 1.0:
        return "SPRING"
    return "NEUTRAL"

# ============================================================================
# STOCK ANALYSIS
# ============================================================================

def analyze_stock(symbol: str, sector: str, spy_close: pd.Series, years: int = 5) -> Optional[pd.DataFrame]:
    """Analyze a single stock"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years*365 + 300)

        df = yf.download(symbol, start=start_date, end=end_date, progress=False)
        if df.empty or len(df) < 300:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        close = df['Close']
        high = df['High']
        low = df['Low']
        volume = df['Volume']

        # Calculate indicators
        rsi = calculate_rsi(close, 14)
        mfi = calculate_mfi(high, low, close, volume, 14)
        cmf = calculate_cmf(high, low, close, volume, 20)
        mrs = calculate_mansfield_rs(close, spy_close, 252)
        lr_ratio = mfi / (rsi + 1e-10)

        ma20 = calculate_sma(close, 20)
        ma50 = calculate_sma(close, 50)
        ma200 = calculate_sma(close, 200)

        # Build signals dataframe
        signals = pd.DataFrame(index=df.index)
        signals['Symbol'] = symbol
        signals['Sector'] = sector
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

        # Volume
        vol_sma20 = volume.rolling(20).mean()
        signals['Rel_Volume'] = volume / vol_sma20
        signals['Vol_Surge'] = signals['Rel_Volume'] > 1.5

        # 52-week range
        high_52w = close.rolling(252).max()
        low_52w = close.rolling(252).min()
        signals['Pct_52w_Range'] = ((close - low_52w) / (high_52w - low_52w + 1e-10)) * 100

        # Classify tier
        tiers = []
        for i in range(len(signals)):
            if i < 200 or pd.isna(signals['LR_Ratio'].iloc[i]):
                tiers.append(None)
            else:
                tier = classify_tlt_tier(
                    signals['LR_Ratio'].iloc[i],
                    signals['CMF'].iloc[i],
                    signals['CMF_Rising'].iloc[i],
                    signals['MRS'].iloc[i],
                    signals['MRS_Rising'].iloc[i],
                    signals['Above_MA20'].iloc[i],
                    signals['Above_MA50'].iloc[i],
                    signals['Above_MA200'].iloc[i],
                    signals['RSI'].iloc[i]
                )
                tiers.append(tier)
        signals['Tier'] = tiers

        # Forward returns
        for period in [21, 63, 126]:
            signals[f'Fwd_Ret_{period}d'] = close.shift(-period) / close - 1
            signals[f'Win_{period}d'] = signals[f'Fwd_Ret_{period}d'] > 0

        # Trim
        signals = signals.iloc[200:-126]
        signals = signals.dropna(subset=['Tier'])

        return signals

    except Exception as e:
        return None

# ============================================================================
# MAIN BACKTEST
# ============================================================================

def run_full_sp500_backtest():
    """Run backtest on all S&P 500 stocks"""

    print("\n" + "="*80)
    print("FULL S&P 500 TLT TIER BACKTEST - 5 YEAR ANALYSIS")
    print("="*80)

    # Count total stocks
    total_stocks = sum(len(stocks) for stocks in SP500_BY_SECTOR.values())
    print(f"\nTotal stocks to analyze: {total_stocks}")

    # Download SPY
    print("\nDownloading SPY benchmark...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5*365 + 300)
    spy = yf.download('SPY', start=start_date, end=end_date, progress=False)
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = spy.columns.get_level_values(0)
    spy_close = spy['Close']

    # Analyze all stocks
    all_signals = []
    sector_results = {}
    processed = 0
    successful = 0

    for sector, stocks in SP500_BY_SECTOR.items():
        print(f"\n{sector} ({len(stocks)} stocks):")
        sector_signals = []

        for symbol in stocks:
            processed += 1
            sys.stdout.write(f"\r  [{processed}/{total_stocks}] {symbol}...          ")
            sys.stdout.flush()

            result = analyze_stock(symbol, sector, spy_close)

            if result is not None and len(result) > 0:
                sector_signals.append(result)
                all_signals.append(result)
                successful += 1

        if sector_signals:
            sector_df = pd.concat(sector_signals, ignore_index=True)
            sector_results[sector] = sector_df

        print(f"\r  {sector}: {len(sector_signals)}/{len(stocks)} stocks analyzed")

    if not all_signals:
        print("ERROR: No data collected")
        return None, None

    combined = pd.concat(all_signals, ignore_index=True)
    print(f"\n\nTotal signal days collected: {len(combined):,}")
    print(f"Stocks successfully analyzed: {successful}/{total_stocks}")

    return combined, sector_results

def analyze_results(combined, sector_results):
    """Analyze and generate report"""

    report = []
    report.append("="*80)
    report.append("FULL S&P 500 TLT TIER BACKTEST - 5 YEAR ANALYSIS")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"Total Stocks: {combined['Symbol'].nunique()}")
    report.append(f"Total Signal Days: {len(combined):,}")
    report.append("="*80)

    tiers = ['LEADER', 'SURGE', 'OVERSOLD', 'SPRING', 'DANGER', 'NEUTRAL']
    periods = [(21, '1M'), (63, '3M'), (126, '6M')]

    # Overall tier performance
    report.append("\n\n" + "="*80)
    report.append("OVERALL TIER PERFORMANCE (ALL S&P 500)")
    report.append("="*80)

    overall_results = []
    for tier in tiers:
        tier_data = combined[combined['Tier'] == tier]
        if len(tier_data) == 0:
            continue

        row = {'Tier': tier, 'Signals': len(tier_data)}
        for period_days, period_name in periods:
            col = f'Fwd_Ret_{period_days}d'
            valid = tier_data[col].dropna()
            if len(valid) > 0:
                row[f'{period_name}_WinRate'] = (valid > 0).mean() * 100
                row[f'{period_name}_AvgRet'] = valid.mean() * 100
            else:
                row[f'{period_name}_WinRate'] = 0
                row[f'{period_name}_AvgRet'] = 0
        overall_results.append(row)

    overall_df = pd.DataFrame(overall_results)

    report.append(f"\n{'Tier':<12} | {'Signals':>10} | {'1M Win%':>8} {'1M Ret%':>8} | {'3M Win%':>8} {'3M Ret%':>8} | {'6M Win%':>8} {'6M Ret%':>8}")
    report.append("-"*105)

    for _, row in overall_df.iterrows():
        report.append(f"{row['Tier']:<12} | {row['Signals']:>10,} | {row['1M_WinRate']:>7.1f}% {row['1M_AvgRet']:>+7.2f}% | "
                     f"{row['3M_WinRate']:>7.1f}% {row['3M_AvgRet']:>+7.2f}% | {row['6M_WinRate']:>7.1f}% {row['6M_AvgRet']:>+7.2f}%")

    # Best by period
    report.append("\n\n" + "="*80)
    report.append("BEST PERFORMING TIERS BY PERIOD")
    report.append("="*80)

    for period_days, period_name in periods:
        if len(overall_df) > 0:
            best = overall_df.loc[overall_df[f'{period_name}_WinRate'].idxmax()]
            best_ret = overall_df.loc[overall_df[f'{period_name}_AvgRet'].idxmax()]
            report.append(f"\n{period_name} Best Win Rate: {best['Tier']} ({best[f'{period_name}_WinRate']:.1f}%)")
            report.append(f"{period_name} Best Avg Return: {best_ret['Tier']} ({best_ret[f'{period_name}_AvgRet']:+.2f}%)")

    # Sector analysis
    report.append("\n\n" + "="*80)
    report.append("SECTOR-BY-SECTOR BEST TIER (3-MONTH)")
    report.append("="*80)

    report.append(f"\n{'Sector':<30} | {'Best Tier':<10} | {'Win Rate':>10} | {'Avg Return':>12} | Signals")
    report.append("-"*80)

    for sector, sector_df in sector_results.items():
        best_tier = None
        best_win_rate = 0
        best_return = 0
        best_signals = 0

        for tier in tiers:
            tier_data = sector_df[sector_df['Tier'] == tier]
            valid = tier_data['Fwd_Ret_63d'].dropna()
            if len(valid) >= 10:
                win_rate = (valid > 0).mean() * 100
                if win_rate > best_win_rate:
                    best_win_rate = win_rate
                    best_tier = tier
                    best_return = valid.mean() * 100
                    best_signals = len(valid)

        if best_tier:
            report.append(f"{sector:<30} | {best_tier:<10} | {best_win_rate:>9.1f}% | {best_return:>+11.2f}% | {best_signals:>5}")

    # Optimal filters analysis
    report.append("\n\n" + "="*80)
    report.append("OPTIMAL ENTRY FILTERS BY TIER (3-MONTH)")
    report.append("="*80)

    for tier in ['DANGER', 'OVERSOLD', 'SPRING', 'LEADER', 'SURGE']:
        tier_data = combined[combined['Tier'] == tier].copy()
        if len(tier_data) < 50:
            continue

        baseline = (tier_data['Win_63d'] == True).mean() * 100
        report.append(f"\n{tier} (Baseline: {baseline:.1f}% win rate, {len(tier_data):,} signals)")
        report.append("-"*60)

        filters = [
            ('RSI < 40', tier_data['RSI'] < 40),
            ('RSI < 50', tier_data['RSI'] < 50),
            ('Dist MA50 < -10%', tier_data['Dist_MA50'] < -10),
            ('Dist MA50 < -5%', tier_data['Dist_MA50'] < -5),
            ('Volume Surge', tier_data['Vol_Surge'] == True),
            ('Near 52wk Low (<20%)', tier_data['Pct_52w_Range'] < 20),
            ('Near 52wk High (>80%)', tier_data['Pct_52w_Range'] > 80),
            ('Above MA200', tier_data['Above_MA200'] == True),
            ('Below MA200', tier_data['Above_MA200'] == False),
            ('CMF Rising', tier_data['CMF_Rising'] == True),
            ('MRS Rising', tier_data['MRS_Rising'] == True),
        ]

        best_filters = []
        for name, mask in filters:
            filtered = tier_data[mask]
            if len(filtered) >= 20:
                win_rate = (filtered['Win_63d'] == True).mean() * 100
                avg_ret = filtered['Fwd_Ret_63d'].mean() * 100
                improvement = win_rate - baseline
                if improvement > 2:
                    best_filters.append((name, len(filtered), win_rate, avg_ret, improvement))

        if best_filters:
            best_filters.sort(key=lambda x: -x[2])
            for name, signals, win_rate, avg_ret, improvement in best_filters[:5]:
                report.append(f"  {name:<25} | {signals:>6} signals | {win_rate:>6.1f}% win | {avg_ret:>+6.2f}% ret | +{improvement:.1f}%")

    # Top stock-tier combinations
    report.append("\n\n" + "="*80)
    report.append("TOP 30 STOCK-TIER COMBINATIONS (3-MONTH, min 10 signals)")
    report.append("="*80)

    stock_tier_results = []
    for symbol in combined['Symbol'].unique():
        stock_data = combined[combined['Symbol'] == symbol]
        sector = stock_data['Sector'].iloc[0]

        for tier in tiers:
            tier_data = stock_data[stock_data['Tier'] == tier]
            valid = tier_data['Fwd_Ret_63d'].dropna()
            if len(valid) >= 10:
                stock_tier_results.append({
                    'Symbol': symbol,
                    'Sector': sector,
                    'Tier': tier,
                    'Signals': len(valid),
                    'WinRate': (valid > 0).mean() * 100,
                    'AvgRet': valid.mean() * 100,
                })

    if stock_tier_results:
        stock_tier_df = pd.DataFrame(stock_tier_results)
        top30 = stock_tier_df.nlargest(30, 'WinRate')

        report.append(f"\n{'Symbol':<8} | {'Sector':<25} | {'Tier':<10} | {'Signals':>8} | {'Win Rate':>10} | {'Avg Ret':>10}")
        report.append("-"*90)

        for _, row in top30.iterrows():
            report.append(f"{row['Symbol']:<8} | {row['Sector']:<25} | {row['Tier']:<10} | {row['Signals']:>8} | {row['WinRate']:>9.1f}% | {row['AvgRet']:>+9.2f}%")

    # Summary
    report.append("\n\n" + "="*80)
    report.append("KEY FINDINGS SUMMARY")
    report.append("="*80)

    if len(overall_df) > 0:
        best_3m = overall_df.loc[overall_df['3M_WinRate'].idxmax()]
        best_6m = overall_df.loc[overall_df['6M_WinRate'].idxmax()]

        report.append(f"""
BEST TIERS:
- 3-Month: {best_3m['Tier']} ({best_3m['3M_WinRate']:.1f}% win rate, {best_3m['3M_AvgRet']:+.2f}% avg return)
- 6-Month: {best_6m['Tier']} ({best_6m['6M_WinRate']:.1f}% win rate, {best_6m['6M_AvgRet']:+.2f}% avg return)

SIGNAL DISTRIBUTION:
""")
        for _, row in overall_df.iterrows():
            pct = row['Signals'] / len(combined) * 100
            report.append(f"  {row['Tier']}: {row['Signals']:,} signals ({pct:.1f}%)")

    report.append("\n" + "="*80)
    report.append("END OF REPORT")
    report.append("="*80)

    return "\n".join(report), overall_df, stock_tier_df if stock_tier_results else None

def send_report_email(report_text, overall_df, stock_tier_df=None):
    """Send report via email"""

    EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    EMAIL_RECIPIENT = "daquinn@targetedequityconsulting.com"

    if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        print("ERROR: Email credentials not found")
        return False

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"FULL S&P 500 TLT Tier Backtest - {datetime.now().strftime('%Y-%m-%d')}"
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = EMAIL_RECIPIENT

        # HTML summary
        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
        <h1>Full S&P 500 TLT Tier Backtest</h1>
        <h3>5-Year Analysis - {datetime.now().strftime('%Y-%m-%d')}</h3>
        <h2>Overall Tier Performance</h2>
        <table border="1" cellpadding="8" style="border-collapse: collapse;">
        <tr style="background-color: #4CAF50; color: white;">
            <th>Tier</th><th>Signals</th><th>1M Win%</th><th>3M Win%</th><th>6M Win%</th><th>6M Avg Ret</th>
        </tr>
        """

        for _, row in overall_df.iterrows():
            html += f"""<tr>
                <td><b>{row['Tier']}</b></td>
                <td>{row['Signals']:,}</td>
                <td>{row['1M_WinRate']:.1f}%</td>
                <td>{row['3M_WinRate']:.1f}%</td>
                <td>{row['6M_WinRate']:.1f}%</td>
                <td>{row['6M_AvgRet']:+.2f}%</td>
            </tr>"""

        html += """</table>
        <p><b>Full report attached.</b></p>
        </body></html>"""

        msg.attach(MIMEText(html, 'html'))

        # Attach report
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(report_text.encode('utf-8'))
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment',
                            filename=f"SP500_Full_TLT_Backtest_{datetime.now().strftime('%Y%m%d')}.txt")
        msg.attach(attachment)

        # Attach CSV
        if overall_df is not None:
            csv_buffer = io.StringIO()
            overall_df.to_csv(csv_buffer, index=False)
            csv_attach = MIMEBase('application', 'octet-stream')
            csv_attach.set_payload(csv_buffer.getvalue().encode('utf-8'))
            encoders.encode_base64(csv_attach)
            csv_attach.add_header('Content-Disposition', 'attachment',
                                filename=f"SP500_Tier_Performance_{datetime.now().strftime('%Y%m%d')}.csv")
            msg.attach(csv_attach)

        # Send
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)

        print(f"\nReport emailed to {EMAIL_RECIPIENT}")
        return True

    except Exception as e:
        print(f"\nEmail failed: {e}")
        return False

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Run backtest
    combined, sector_results = run_full_sp500_backtest()

    if combined is not None:
        # Analyze
        report_text, overall_df, stock_tier_df = analyze_results(combined, sector_results)

        # Print summary
        print("\n" + "="*80)
        print("RESULTS SUMMARY")
        print("="*80)
        print(f"\nStocks analyzed: {combined['Symbol'].nunique()}")
        print(f"Signal days: {len(combined):,}")
        print("\nTier Performance (3-Month):")
        for _, row in overall_df.iterrows():
            print(f"  {row['Tier']:<12}: {row['3M_WinRate']:.1f}% win rate, {row['3M_AvgRet']:+.2f}% avg return ({row['Signals']:,} signals)")

        # Save locally
        filename = f"SP500_Full_TLT_Backtest_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        with open(filename, 'w') as f:
            f.write(report_text)
        print(f"\nSaved to: {filename}")

        # Email
        send_report_email(report_text, overall_df, stock_tier_df)
