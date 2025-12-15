"""
Fetch sector information for S&P 500 companies and update the list
"""
import pandas as pd
import requests
import os
from dotenv import load_dotenv
import time

load_dotenv()

FMP_API_KEY = os.getenv('FMP_API_KEY')
BASE_URL = "https://financialmodelingprep.com/api/v3"


def get_company_profile(ticker):
    """Get company profile including sector"""
    url = f"{BASE_URL}/profile/{ticker}"
    params = {'apikey': FMP_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data and len(data) > 0:
            profile = data[0]
            return {
                'sector': profile.get('sector', 'Unknown'),
                'industry': profile.get('industry', 'Unknown'),
                'market_cap': profile.get('mktCap', 0)
            }
    except Exception as e:
        print(f"Error fetching profile for {ticker}: {e}")

    return {'sector': 'Unknown', 'industry': 'Unknown', 'market_cap': 0}


def add_sectors_to_sp500():
    """Add sector information to SP500 list"""
    print("Loading S&P 500 list...")
    df = pd.read_excel('SP500_list.xlsx')

    print(f"\nFetching sector data for {len(df)} stocks...")

    sectors = []
    industries = []
    market_caps = []

    for i, ticker in enumerate(df['Symbol'], 1):
        print(f"Progress: {i}/{len(df)} - {ticker:<6} ({(i/len(df))*100:.1f}%)", end='\r')

        profile = get_company_profile(ticker)
        sectors.append(profile['sector'])
        industries.append(profile['industry'])
        market_caps.append(profile['market_cap'])

        # Rate limiting
        time.sleep(0.15)

    print("\n\nAdding sector data to DataFrame...")
    df['Sector'] = sectors
    df['Industry'] = industries
    df['MarketCap'] = market_caps

    # Save updated file
    output_file = 'SP500_list_with_sectors.xlsx'
    df.to_excel(output_file, index=False)

    print(f"\n✓ Saved to {output_file}")

    # Print sector breakdown
    print("\n" + "="*60)
    print("SECTOR BREAKDOWN")
    print("="*60)
    sector_counts = df['Sector'].value_counts()
    for sector, count in sector_counts.items():
        print(f"{sector:<30} {count:>3} stocks")

    return df


if __name__ == "__main__":
    df = add_sectors_to_sp500()
