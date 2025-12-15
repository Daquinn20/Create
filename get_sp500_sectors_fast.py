"""
Fast parallel sector fetching for S&P 500 companies
"""
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

load_dotenv()

FMP_API_KEY = os.getenv('FMP_API_KEY')
BASE_URL = "https://financialmodelingprep.com/api/v3"


class SectorFetcher:
    def __init__(self, max_workers=15):
        self.max_workers = max_workers
        self.lock = Lock()
        self.progress_count = 0

    def get_company_profile(self, ticker):
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
                    'ticker': ticker,
                    'sector': profile.get('sector', 'Unknown'),
                    'industry': profile.get('industry', 'Unknown'),
                    'market_cap': profile.get('mktCap', 0)
                }
        except Exception as e:
            print(f"\nError fetching {ticker}: {e}")

        return {
            'ticker': ticker,
            'sector': 'Unknown',
            'industry': 'Unknown',
            'market_cap': 0
        }

    def fetch_single(self, ticker, total):
        """Fetch profile for single ticker"""
        result = self.get_company_profile(ticker)

        with self.lock:
            self.progress_count += 1
            print(f"Progress: {self.progress_count}/{total} - {ticker:<6} ({(self.progress_count/total)*100:.1f}%)", end='\r')

        return result

    def fetch_all(self, tickers):
        """Fetch all profiles in parallel"""
        results = []
        total = len(tickers)

        print(f"\nFetching sector data for {total} stocks using {self.max_workers} parallel workers...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_ticker = {
                executor.submit(self.fetch_single, ticker, total): ticker
                for ticker in tickers
            }

            for future in as_completed(future_to_ticker):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    ticker = future_to_ticker[future]
                    print(f"\nError processing {ticker}: {e}")

        print("\n")
        return results


def main():
    print("Loading S&P 500 list...")
    df = pd.read_excel('SP500_list.xlsx')

    fetcher = SectorFetcher(max_workers=15)
    tickers = df['Symbol'].tolist()

    # Fetch all sector data in parallel
    results = fetcher.fetch_all(tickers)

    # Create results dataframe
    results_df = pd.DataFrame(results)

    # Merge with original dataframe
    df_merged = df.merge(
        results_df[['ticker', 'sector', 'industry', 'market_cap']],
        left_on='Symbol',
        right_on='ticker',
        how='left'
    )

    # Drop duplicate ticker column
    df_merged = df_merged.drop('ticker', axis=1)

    # Rename columns
    df_merged = df_merged.rename(columns={
        'sector': 'Sector',
        'industry': 'Industry',
        'market_cap': 'MarketCap'
    })

    # Save updated file
    output_file = 'SP500_list_with_sectors.xlsx'
    df_merged.to_excel(output_file, index=False)

    print(f"\n✓ Saved to {output_file}")

    # Print sector breakdown
    print("\n" + "="*60)
    print("SECTOR BREAKDOWN")
    print("="*60)
    sector_counts = df_merged['Sector'].value_counts()
    for sector, count in sector_counts.items():
        print(f"{sector:<30} {count:>3} stocks")

    print("\n✓ Sector data ready! Refresh your dashboard to use 'By Sector' mode.")


if __name__ == "__main__":
    main()
