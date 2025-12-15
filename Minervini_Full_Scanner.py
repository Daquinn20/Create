import yfinance as yf
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime
import os
import time
import numpy as np

load_dotenv()

# Get API keys
FISCAL_API_KEY = os.getenv('FISCAL_API_KEY')
FMP_API_KEY = os.getenv('FMP_API_KEY')

print("=" * 80)
print("MARK MINERVINI TREND TEMPLATE & VCP SCANNER")
print("S&P 500 + NASDAQ 100 | 7 of 8 Criteria (Market-Adjusted)")
print("=" * 80)

# Check API keys
if FMP_API_KEY:
    print("✓ FMP API key loaded - Enhanced fundamentals available")
else:
    print("⚠ FMP API key not found - Using yfinance only")

if FISCAL_API_KEY:
    print("✓ fiscal.ai API key loaded")

print("\n" + "=" * 80)
print("LOADING STOCK LISTS...")
print("=" * 80)

# Load US Broad Index
try:
    sp500_df = pd.read_excel(r'C:\Users\daqui\PycharmProjects\PythonProject1\Index_Broad_US.xlsx')
    sp500_tickers = sp500_df['Ticker'].tolist()
    print(f"✓ US Broad Index: {len(sp500_tickers)} stocks loaded")
except:
    print("⚠ Index_Broad_US.xlsx not found - downloading from Wikipedia...")
    sp500_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    sp500_tickers = sp500_df['Symbol'].tolist()
    print(f"✓ S&P 500: {len(sp500_tickers)} stocks downloaded")

# Get NASDAQ 100 list
print("Downloading NASDAQ 100 list...")
nasdaq100_tickers = [
    'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO', 'COST',
    'NFLX', 'AMD', 'ADBE', 'PEP', 'CSCO', 'CMCSA', 'TMUS', 'INTC', 'TXN', 'INTU',
    'QCOM', 'AMGN', 'HON', 'AMAT', 'SBUX', 'BKNG', 'ISRG', 'VRTX', 'ADP', 'MDLZ',
    'GILD', 'ADI', 'REGN', 'PANW', 'LRCX', 'MU', 'KLAC', 'PYPL', 'SNPS', 'CDNS',
    'NXPI', 'MRVL', 'ORLY', 'CSX', 'ABNB', 'MELI', 'ASML', 'CHTR', 'WDAY', 'CRWD',
    'ADSK', 'FTNT', 'DASH', 'ROP', 'PCAR', 'MNST', 'CPRT', 'AEP', 'PAYX', 'ROST',
    'ODFL', 'MCHP', 'KDP', 'FAST', 'EA', 'CTSH', 'KHC', 'DXCM', 'VRSK', 'CTAS',
    'GEHC', 'BKR', 'LULU', 'EXC', 'TEAM', 'IDXX', 'CCEP', 'XEL', 'TTD', 'ZS',
    'FANG', 'ANSS', 'ON', 'DDOG', 'CDW', 'WBD', 'BIIB', 'ILMN', 'MDB', 'GFS',
    'CSGP', 'WBA', 'ARM', 'MRNA', 'DLTR', 'SMCI', 'ALGN', 'ZM', 'LCID', 'RIVN'
]

print(f"✓ NASDAQ 100: {len(nasdaq100_tickers)} stocks loaded\n")

# Combine and remove duplicates
all_tickers = list(set(sp500_tickers + nasdaq100_tickers))
print(f"✓ TOTAL UNIVERSE: {len(all_tickers)} unique stocks\n")

print("=" * 80)
print("STARTING MINERVINI SCAN - THIS WILL TAKE 15-20 MINUTES")
print("=" * 80)
print("\nCriteria Being Applied (Need 7 of 8):")
print("1. ✓ Price > 150-day AND 200-day MA")
print("2. ✓ 150-day MA > 200-day MA")
print("3. ✓ 200-day MA trending up for 1+ months")
print("4. ✓ 50-day MA > 150-day AND 200-day MA")
print("5. ✓ Price > 50-day MA")
print("6. ✓ Price 30%+ above 52-week low")
print("7. ✓ Price within 25% of 52-week high")
print("8. ✓ Relative Strength > 0 (vs SPY)")
print("\n💡 NOTE: Requiring 7/8 due to current market conditions")
print("\nPlus: Fundamental filters & VCP pattern indicators\n")
print("-" * 80)


def calculate_relative_strength(ticker_data, spy_data):
    """Calculate relative strength vs S&P 500"""
    try:
        # Get 1-year returns
        stock_return = (ticker_data['Close'].iloc[-1] / ticker_data['Close'].iloc[-252] - 1) * 100
        spy_return = (spy_data['Close'].iloc[-1] / spy_data['Close'].iloc[-252] - 1) * 100

        # RS score (higher is better)
        rs_score = stock_return - spy_return
        return round(rs_score, 2)
    except:
        return None


def check_vcp_pattern(data):
    """
    Check for VCP characteristics:
    - Multiple contractions with each pullback smaller than previous
    - Volume decreasing on pullbacks
    """
    try:
        # Look at last 3 months for contractions
        recent_data = data.tail(60)

        # Find local highs and lows
        highs = recent_data['High'].rolling(5).max()
        lows = recent_data['Low'].rolling(5).min()

        # Calculate volatility contraction
        early_volatility = recent_data['High'].head(20).std()
        recent_volatility = recent_data['High'].tail(20).std()

        contraction_ratio = recent_volatility / early_volatility if early_volatility > 0 else 1

        # Volume trend (should be decreasing)
        early_volume = recent_data['Volume'].head(20).mean()
        recent_volume = recent_data['Volume'].tail(20).mean()
        volume_ratio = recent_volume / early_volume if early_volume > 0 else 1

        # VCP score (lower is better)
        vcp_score = (contraction_ratio * 0.6) + (volume_ratio * 0.4)

        # Classify VCP quality
        if vcp_score < 0.5:
            return 'Tight VCP', round(vcp_score, 2)
        elif vcp_score < 0.7:
            return 'Good VCP', round(vcp_score, 2)
        elif vcp_score < 0.9:
            return 'Possible VCP', round(vcp_score, 2)
        else:
            return 'No VCP', round(vcp_score, 2)
    except:
        return 'Unknown', None


def get_fmp_metrics(ticker):
    """Get FMP data - simplified for speed"""
    if not FMP_API_KEY:
        return {}

    try:
        # Get key metrics only
        url = f"https://financialmodelingprep.com/api/v3/key-metrics-ttm/{ticker}?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=5)

        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                m = data[0]
                return {
                    'fmp_pe': m.get('peRatioTTM'),
                    'fmp_roe': m.get('roeTTM'),
                    'fmp_roa': m.get('roaTTM')
                }
        return {}
    except:
        return {}


def safe_num(val, multiplier=1):
    """Helper function to safely get numeric values"""
    if val is None or val == 'N/A' or pd.isna(val):
        return None
    try:
        return round(float(val) * multiplier, 2)
    except:
        return None


# Download SPY data for relative strength calculation
print("Downloading SPY data for relative strength calculation...")
spy = yf.Ticker("SPY")
spy_data = spy.history(period="1y")

# Results storage
minervini_stocks = []
failed_criteria = []
errors = []

for i, ticker in enumerate(all_tickers, 1):
    ticker_clean = ticker.replace('.', '-')

    print(f"[{i}/{len(all_tickers)}] Scanning {ticker_clean:8}", end=" ")

    try:
        stock = yf.Ticker(ticker_clean)
        data = stock.history(period="1y")

        if len(data) < 200:
            print("❌ Insufficient data")
            errors.append(ticker_clean)
            continue

        # Get current metrics
        current_price = data['Close'].iloc[-1]
        current_volume = data['Volume'].iloc[-1]
        avg_volume = data['Volume'].tail(50).mean()

        # Calculate moving averages
        sma_50 = data['Close'].rolling(50).mean().iloc[-1]
        sma_150 = data['Close'].rolling(150).mean().iloc[-1]
        sma_200 = data['Close'].rolling(200).mean().iloc[-1]

        # Calculate 52-week high/low
        week_52_high = data['Close'].max()
        week_52_low = data['Close'].min()

        # Check if 200-day MA is rising (compare to 1 month ago)
        sma_200_month_ago = data['Close'].rolling(200).mean().iloc[-22]
        sma_200_rising = sma_200 > sma_200_month_ago

        # MINERVINI'S 8 CRITERIA CHECK
        criteria_met = 0
        criteria_details = {}

        # 1. Price > 150 and 200-day MA
        criteria_1 = current_price > sma_150 and current_price > sma_200
        criteria_details['Price > 150 & 200 MA'] = criteria_1
        if criteria_1: criteria_met += 1

        # 2. 150-day MA > 200-day MA
        criteria_2 = sma_150 > sma_200
        criteria_details['150 MA > 200 MA'] = criteria_2
        if criteria_2: criteria_met += 1

        # 3. 200-day MA trending up
        criteria_3 = sma_200_rising
        criteria_details['200 MA Rising'] = criteria_3
        if criteria_3: criteria_met += 1

        # 4. 50-day MA > 150 and 200-day MA
        criteria_4 = sma_50 > sma_150 and sma_50 > sma_200
        criteria_details['50 MA > 150 & 200 MA'] = criteria_4
        if criteria_4: criteria_met += 1

        # 5. Price > 50-day MA
        criteria_5 = current_price > sma_50
        criteria_details['Price > 50 MA'] = criteria_5
        if criteria_5: criteria_met += 1

        # 6. Price 30%+ above 52-week low
        criteria_6 = current_price >= (week_52_low * 1.30)
        criteria_details['Price 30%+ Above Low'] = criteria_6
        if criteria_6: criteria_met += 1

        # 7. Price within 25% of 52-week high
        criteria_7 = current_price >= (week_52_high * 0.75)
        criteria_details['Within 25% of High'] = criteria_7
        if criteria_7: criteria_met += 1

        # 8. Relative Strength
        rs_score = calculate_relative_strength(data, spy_data)
        criteria_8 = rs_score is not None and rs_score > 0  # Outperforming S&P 500
        criteria_details['RS > 0 (Beat SPY)'] = criteria_8
        if criteria_8: criteria_met += 1

        # Changed from 8 to 7 - market-adjusted!
        if criteria_met >= 7:
            # Get fundamental data
            info = stock.info
            fmp_data = get_fmp_metrics(ticker)

            # Check VCP pattern
            vcp_pattern, vcp_score = check_vcp_pattern(data)

            # Calculate distance from moving averages
            distance_50 = ((current_price - sma_50) / sma_50 * 100)
            distance_high = ((week_52_high - current_price) / week_52_high * 100)

            setup = {
                'Ticker': ticker_clean,
                'Company': info.get('longName', 'N/A'),
                'Sector': info.get('sector', 'N/A'),
                'Industry': info.get('industry', 'N/A'),
                'Index': 'Both' if ticker in sp500_tickers and ticker in nasdaq100_tickers else (
                    'S&P 500' if ticker in sp500_tickers else 'NASDAQ 100'),

                # Price & Technical
                'Price': round(current_price, 2),
                '52W High': round(week_52_high, 2),
                '52W Low': round(week_52_low, 2),
                'Distance from High %': round(distance_high, 2),
                'Distance from 50 MA %': round(distance_50, 2),

                # Minervini Criteria Score
                'Criteria Met': f"{criteria_met}/8",
                'Minervini Score': criteria_met,

                # Relative Strength
                'RS Score (vs SPY)': rs_score,
                'RS Rating': 'Excellent' if rs_score and rs_score > 20 else (
                    'Strong' if rs_score and rs_score > 10 else ('Good' if rs_score and rs_score > 0 else 'Negative')),

                # VCP Analysis
                'VCP Pattern': vcp_pattern,
                'VCP Score': vcp_score,

                # Moving Averages
                '50 MA': round(sma_50, 2),
                '150 MA': round(sma_150, 2),
                '200 MA': round(sma_200, 2),
                '200 MA Rising': 'Yes' if sma_200_rising else 'No',

                # Volume
                'Volume': int(current_volume),
                'Avg Volume': int(avg_volume),
                'Volume vs Avg': round((current_volume / avg_volume * 100), 0) if avg_volume > 0 else None,

                # Fundamentals (yfinance)
                'Market Cap (B)': safe_num(info.get('marketCap'), 1 / 1e9),
                'P/E Ratio': safe_num(info.get('trailingPE')),
                'Forward P/E': safe_num(info.get('forwardPE')),
                'PEG Ratio': safe_num(info.get('pegRatio')),
                'Revenue Growth %': safe_num(info.get('revenueGrowth'), 100),
                'Earnings Growth %': safe_num(info.get('earningsGrowth'), 100),
                'Profit Margin %': safe_num(info.get('profitMargins'), 100),
                'Operating Margin %': safe_num(info.get('operatingMargins'), 100),
                'ROE %': safe_num(info.get('returnOnEquity'), 100),
                'ROA %': safe_num(info.get('returnOnAssets'), 100),
                'Debt/Equity': safe_num(info.get('debtToEquity'), 0.01),
                'Current Ratio': safe_num(info.get('currentRatio')),
                'Beta': safe_num(info.get('beta')),

                # FMP data if available
                'FMP P/E': safe_num(fmp_data.get('fmp_pe')) if fmp_data else None,
                'FMP ROE %': safe_num(fmp_data.get('fmp_roe'), 100) if fmp_data else None,
                'FMP ROA %': safe_num(fmp_data.get('fmp_roa'), 100) if fmp_data else None,

                # Metadata
                'Scan Date': datetime.now().strftime('%Y-%m-%d'),
            }

            minervini_stocks.append(setup)
            print(f"✅ PASSED {criteria_met}/8 | RS: {rs_score:+6.1f}% | VCP: {vcp_pattern}")

        else:
            # Track stocks that failed criteria
            failed_criteria.append({
                'Ticker': ticker_clean,
                'Criteria Met': f"{criteria_met}/8",
                **criteria_details
            })
            print(f"❌ Failed ({criteria_met}/8)")

    except Exception as e:
        errors.append(ticker_clean)
        print(f"⚠️  Error: {str(e)[:30]}")
        continue

# Print Summary
print("\n" + "=" * 80)
print("SCAN COMPLETE!")
print("=" * 80)
print(f"\n✅ MINERVINI STOCKS (7-8 of 8 criteria): {len(minervini_stocks)}")
print(f"❌ Failed Criteria: {len(failed_criteria)}")
print(f"⚠️  Errors: {len(errors)}")

# ALWAYS create Excel file (even with 0 results) - THIS IS THE FIX!
excel_filename = f'Minervini_Full_Scan_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'

with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
    # Always create Failed Criteria sheet first (this is most valuable!)
    if failed_criteria:
        failed_df = pd.DataFrame(failed_criteria)
        # Sort by criteria met (highest first)
        failed_df['Criteria_Num'] = failed_df['Criteria Met'].str.extract('(\d+)').astype(int)
        failed_df = failed_df.sort_values('Criteria_Num', ascending=False)
        failed_df = failed_df.drop('Criteria_Num', axis=1)
        failed_df.to_excel(writer, sheet_name='Failed Criteria', index=False)

    # If we have results, create other sheets
    if minervini_stocks:
        results_df = pd.DataFrame(minervini_stocks)

        # Sort by Minervini Score first, then RS Score
        results_df = results_df.sort_values(['Minervini Score', 'RS Score (vs SPY)'], ascending=[False, False])

        # Sheet 1: All Minervini Stocks
        results_df.to_excel(writer, sheet_name='All Minervini Stocks', index=False)

        # Sheet 2: Perfect 8/8 only
        perfect = results_df[results_df['Minervini Score'] == 8].copy()
        if len(perfect) > 0:
            perfect.to_excel(writer, sheet_name='Perfect 8 of 8', index=False)

        # Sheet 3: Tight VCP Patterns
        vcp_tight = results_df[results_df['VCP Pattern'] == 'Tight VCP'].copy()
        if len(vcp_tight) > 0:
            vcp_tight.to_excel(writer, sheet_name='Tight VCP', index=False)

        # Sheet 4: High RS (Relative Strength > 20%)
        high_rs = results_df[results_df['RS Score (vs SPY)'].notna() & (results_df['RS Score (vs SPY)'] > 20)].copy()
        if len(high_rs) > 0:
            high_rs.to_excel(writer, sheet_name='High RS Leaders', index=False)

        # Sheet 5: Near 52-Week High (within 5%)
        near_high = results_df[results_df['Distance from High %'] < 5].copy()
        if len(near_high) > 0:
            near_high.to_excel(writer, sheet_name='Near 52W High', index=False)

        # Sheet 6: By Sector
        sector_summary = results_df.groupby('Sector').agg({
            'Ticker': 'count',
            'Minervini Score': 'mean',
            'RS Score (vs SPY)': 'mean'
        }).reset_index()
        sector_summary.columns = ['Sector', 'Count', 'Avg Score', 'Avg RS']
        sector_summary = sector_summary.sort_values('Count', ascending=False)
        sector_summary.to_excel(writer, sheet_name='By Sector', index=False)

    # Auto-adjust column widths
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
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

print("\n" + "=" * 80)
print("📊 EXCEL FILE CREATED!")
print("=" * 80)
print(f"📂 File: {excel_filename}")
print(f"📂 Location: {os.path.abspath(excel_filename)}")

if minervini_stocks:
    results_df = pd.DataFrame(minervini_stocks)
    print(f"\n📋 RESULTS:")
    print(f"   ✅ Total Qualified: {len(results_df)} stocks")
    perfect_count = len(results_df[results_df['Minervini Score'] == 8])
    if perfect_count > 0:
        print(f"   🏆 Perfect 8/8: {perfect_count} stocks")
    seven_count = len(results_df[results_df['Minervini Score'] == 7])
    if seven_count > 0:
        print(f"   ⭐ Strong 7/8: {seven_count} stocks")

    # Print top 15 stocks
    print("\n" + "=" * 80)
    print("TOP 15 STOCKS (Sorted by Score then RS):")
    print("=" * 80)
    print(f"{'Ticker':<8} {'Score':<7} {'RS Score':<10} {'VCP':<15} {'Price':<10} {'Sector':<20}")
    print("-" * 80)

    for idx, row in results_df.head(15).iterrows():
        ticker = row['Ticker']
        score = row['Minervini Score']
        rs = row['RS Score (vs SPY)'] if row['RS Score (vs SPY)'] else 0
        vcp = row['VCP Pattern']
        price = row['Price']
        sector = row['Sector'][:18] if pd.notna(row['Sector']) else 'N/A'

        print(f"{ticker:<8} {score}/8     {rs:>+6.1f}%    {vcp:<15} ${price:>7.2f}  {sector:<20}")

else:
    print(f"\n📋 NO STOCKS passed 7+ criteria")
    print(f"   ⚠️  Market is likely in correction/transition")
    print(f"   📊 Check 'Failed Criteria' sheet to see what's close")

print("\n✅ Scan complete!")
print(f"\n💡 NEXT STEPS:")
print(f"   1. Open: {excel_filename}")
print(f"   2. Review: 'Failed Criteria' sheet (stocks that got 6/8)")
print(f"   3. Watch: Stocks that are close to qualifying")
print(f"   4. Wait: For market to improve before trading")

