import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import json
from dotenv import load_dotenv


class EarningsRevisionScreener:
    def __init__(self, api_key=None):
        """
        Initialize the screener with FMP API key

        Args:
            api_key (str, optional): Your Financial Modeling Prep API key. 
                                   If not provided, will try to load from .env file
        """
        # Load environment variables from .env file
        load_dotenv()
        
        # Try to get API key from parameter first, then from environment
        self.api_key = api_key or os.getenv('FMP_API_KEY') or os.getenv('API_KEY')
        
        if not self.api_key:
            raise ValueError(
                "API key not found. Please either:\n"
                "1. Pass api_key parameter when creating EarningsRevisionScreener\n"
                "2. Create a .env file with FMP_API_KEY=your_api_key\n"
                "3. Set FMP_API_KEY environment variable"
            )
        
        self.base_url = "https://financialmodelingprep.com/api/v3"
        print(f"✓ API key loaded successfully")

    def get_analyst_estimates(self, ticker):
        """
        Get analyst estimates for a stock

        Args:
            ticker (str): Stock ticker symbol

        Returns:
            dict: Analyst estimates data
        """
        url = f"{self.base_url}/analyst-estimates/{ticker}?apikey={self.api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching analyst estimates for {ticker}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error fetching analyst estimates for {ticker}: {e}")
            return None

    def get_earnings_surprises(self, ticker):
        """
        Get earnings surprises which can show estimate changes

        Args:
            ticker (str): Stock ticker symbol

        Returns:
            dict: Earnings surprises data
        """
        url = f"{self.base_url}/earnings-surprises/{ticker}?apikey={self.api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching earnings surprises for {ticker}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error fetching earnings surprises for {ticker}: {e}")
            return None

    def get_upgrades_downgrades(self, ticker):
        """
        Get analyst upgrades and downgrades

        Args:
            ticker (str): Stock ticker symbol

        Returns:
            list: Upgrades/downgrades data
        """
        url = f"{self.base_url}/upgrades-downgrades?symbol={ticker}&apikey={self.api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching upgrades/downgrades for {ticker}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error fetching upgrades/downgrades for {ticker}: {e}")
            return None

    def get_analyst_estimates_historical(self, ticker):
        """
        Get historical analyst estimates to track revisions

        Args:
            ticker (str): Stock ticker symbol

        Returns:
            list: Historical estimates data
        """
        url = f"{self.base_url}/historical/analyst-estimates/{ticker}?apikey={self.api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching historical estimates for {ticker}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error fetching historical estimates for {ticker}: {e}")
            return None

    def analyze_earnings_revisions(self, ticker):
        """
        Analyze earnings revisions for a stock

        Args:
            ticker (str): Stock ticker symbol

        Returns:
            dict: Analysis results with revision direction and details
        """
        print(f"\n{'=' * 60}")
        print(f"Analyzing {ticker}...")
        print(f"{'=' * 60}")

        # Get current analyst estimates
        estimates = self.get_analyst_estimates(ticker)

        if not estimates or len(estimates) == 0:
            return {
                'ticker': ticker,
                'status': 'No data available',
                'revisions': {}
            }

        # Get historical estimates to compare
        historical_estimates = self.get_analyst_estimates_historical(ticker)

        # Get upgrades/downgrades for additional context
        upgrades_downgrades = self.get_upgrades_downgrades(ticker)

        analysis = {
            'ticker': ticker,
            'status': 'success',
            'current_estimates': {},
            'revisions': {},
            'recent_analyst_actions': []
        }

        # Process current estimates
        if estimates and len(estimates) > 0:
            # Sort by date to get most recent quarters
            estimates_sorted = sorted(estimates, key=lambda x: x.get('date', ''), reverse=True)

            quarters = ['Q+1', 'Q+2', 'Q+3']
            for i, estimate in enumerate(estimates_sorted[:3]):
                quarter_label = quarters[i] if i < 3 else f'Q+{i + 1}'

                analysis['current_estimates'][quarter_label] = {
                    'date': estimate.get('date', 'N/A'),
                    'estimated_eps_avg': estimate.get('estimatedEpsAvg', 'N/A'),
                    'estimated_eps_high': estimate.get('estimatedEpsHigh', 'N/A'),
                    'estimated_eps_low': estimate.get('estimatedEpsLow', 'N/A'),
                    'number_analysts': estimate.get('numberAnalystsEstimatedEps', 'N/A'),
                    'estimated_revenue_avg': estimate.get('estimatedRevenueAvg', 'N/A'),
                    'number_analysts_revenue': estimate.get('numberAnalystsEstimatedRevenue', 'N/A')
                }

        # Analyze revisions by comparing historical data
        if historical_estimates and len(historical_estimates) > 1:
            # Compare most recent estimates with previous ones
            recent = historical_estimates[0] if len(historical_estimates) > 0 else None
            previous = historical_estimates[1] if len(historical_estimates) > 1 else None

            if recent and previous:
                recent_eps = recent.get('estimatedEpsAvg', 0)
                previous_eps = previous.get('estimatedEpsAvg', 0)

                if recent_eps and previous_eps:
                    try:
                        revision_change = float(recent_eps) - float(previous_eps)
                        revision_pct = (revision_change / abs(float(previous_eps))) * 100 if previous_eps != 0 else 0

                        analysis['revisions']['eps_revision'] = {
                            'current': recent_eps,
                            'previous': previous_eps,
                            'change': round(revision_change, 4),
                            'change_pct': round(revision_pct, 2),
                            'direction': 'POSITIVE' if revision_change > 0 else 'NEGATIVE' if revision_change < 0 else 'NEUTRAL',
                            'date_current': recent.get('date', 'N/A'),
                            'date_previous': previous.get('date', 'N/A')
                        }
                    except (ValueError, TypeError) as e:
                        print(f"Error calculating revision: {e}")

        # Process recent analyst actions (last 90 days)
        if upgrades_downgrades:
            cutoff_date = datetime.now() - timedelta(days=90)
            recent_actions = []

            for action in upgrades_downgrades[:10]:  # Get last 10 actions
                action_date_str = action.get('publishedDate', action.get('date', ''))
                try:
                    action_date = datetime.strptime(action_date_str.split('T')[0], '%Y-%m-%d')
                    if action_date >= cutoff_date:
                        recent_actions.append({
                            'date': action_date_str.split('T')[0],
                            'analyst': action.get('gradingCompany', 'N/A'),
                            'action': action.get('action', 'N/A'),
                            'from': action.get('previousGrade', 'N/A'),
                            'to': action.get('newGrade', 'N/A'),
                            'price_target': action.get('priceTarget', 'N/A')
                        })
                except ValueError:
                    # Skip if date parsing fails
                    pass
                except Exception as e:
                    print(f"Error processing analyst action: {e}")

            analysis['recent_analyst_actions'] = recent_actions

        return analysis

    def screen_stocks(self, tickers):
        """
        Screen multiple stocks for earnings revisions

        Args:
            tickers (list): List of stock ticker symbols

        Returns:
            dict: Categorized results
        """
        results = {
            'positive_revisions': [],
            'negative_revisions': [],
            'neutral_or_no_data': []
        }

        total_stocks = len(tickers)
        print(f"\nScreening {total_stocks} stocks...")

        for i, ticker in enumerate(tickers, 1):
            print(f"Progress: {i}/{total_stocks} stocks processed", end='\r')
            
            analysis = self.analyze_earnings_revisions(ticker)

            # Categorize based on revisions
            if analysis['status'] == 'success' and 'eps_revision' in analysis['revisions']:
                revision_direction = analysis['revisions']['eps_revision']['direction']

                if revision_direction == 'POSITIVE':
                    results['positive_revisions'].append(analysis)
                elif revision_direction == 'NEGATIVE':
                    results['negative_revisions'].append(analysis)
                else:
                    results['neutral_or_no_data'].append(analysis)
            else:
                results['neutral_or_no_data'].append(analysis)

            # Be respectful of API rate limits
            time.sleep(0.3)

        print(f"\nCompleted screening {total_stocks} stocks!")
        return results

    def print_results(self, results):
        """
        Print screening results in a formatted way

        Args:
            results (dict): Screening results
        """
        print("\n" + "=" * 80)
        print("EARNINGS REVISION SCREENING RESULTS")
        print("=" * 80)

        # Positive Revisions
        print(f"\n{'=' * 80}")
        print(f"POSITIVE REVISIONS ({len(results['positive_revisions'])} stocks)")
        print(f"{'=' * 80}")

        if results['positive_revisions']:
            for stock in results['positive_revisions']:
                ticker = stock['ticker']
                rev = stock['revisions']['eps_revision']

                print(f"\n{ticker}:")
                print(f"  EPS Estimate Change: ${rev['previous']} → ${rev['current']}")
                print(f"  Absolute Change: ${rev['change']}")
                print(f"  Percentage Change: {rev['change_pct']}%")
                print(f"  Direction: {rev['direction']}")

                # Print current quarter estimates
                if stock['current_estimates']:
                    print(f"\n  Current Quarter Estimates:")
                    for quarter, data in stock['current_estimates'].items():
                        if data['estimated_eps_avg'] != 'N/A':
                            print(f"    {quarter} ({data['date']}): EPS ${data['estimated_eps_avg']} " +
                                  f"({data['number_analysts']} analysts)")

                # Print recent analyst actions
                if stock['recent_analyst_actions']:
                    print(f"\n  Recent Analyst Actions (Last 90 days):")
                    for action in stock['recent_analyst_actions'][:5]:
                        print(f"    {action['date']}: {action['analyst']} - {action['action']} " +
                              f"({action['from']} → {action['to']})")
        else:
            print("\nNo stocks with positive revisions found.")

        # Negative Revisions
        print(f"\n{'=' * 80}")
        print(f"NEGATIVE REVISIONS ({len(results['negative_revisions'])} stocks)")
        print(f"{'=' * 80}")

        if results['negative_revisions']:
            for stock in results['negative_revisions']:
                ticker = stock['ticker']
                rev = stock['revisions']['eps_revision']

                print(f"\n{ticker}:")
                print(f"  EPS Estimate Change: ${rev['previous']} → ${rev['current']}")
                print(f"  Absolute Change: ${rev['change']}")
                print(f"  Percentage Change: {rev['change_pct']}%")
                print(f"  Direction: {rev['direction']}")

                # Print current quarter estimates
                if stock['current_estimates']:
                    print(f"\n  Current Quarter Estimates:")
                    for quarter, data in stock['current_estimates'].items():
                        if data['estimated_eps_avg'] != 'N/A':
                            print(f"    {quarter} ({data['date']}): EPS ${data['estimated_eps_avg']} " +
                                  f"({data['number_analysts']} analysts)")

                # Print recent analyst actions
                if stock['recent_analyst_actions']:
                    print(f"\n  Recent Analyst Actions (Last 90 days):")
                    for action in stock['recent_analyst_actions'][:5]:
                        print(f"    {action['date']}: {action['analyst']} - {action['action']} " +
                              f"({action['from']} → {action['to']})")
        else:
            print("\nNo stocks with negative revisions found.")

        # Summary
        print(f"\n{'=' * 80}")
        print("SUMMARY")
        print(f"{'=' * 80}")
        total_stocks = len(results['positive_revisions']) + len(results['negative_revisions']) + len(
            results['neutral_or_no_data'])
        print(f"Total Stocks Screened: {total_stocks}")
        print(f"Positive Revisions: {len(results['positive_revisions'])}")
        print(f"Negative Revisions: {len(results['negative_revisions'])}")
        print(f"Neutral/No Data: {len(results['neutral_or_no_data'])}")
        print(f"{'=' * 80}\n")

    def save_results(self, results, filename='earnings_revision_results.json'):
        """
        Save results to JSON file

        Args:
            results (dict): Screening results
            filename (str): Output filename
        """
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"✓ Results saved to '{filename}'")
        except Exception as e:
            print(f"Error saving results: {e}")


def create_env_template():
    """
    Create a template .env file if it doesn't exist
    """
    env_template = """# Financial Modeling Prep API Configuration
# Get your free API key at: https://financialmodelingprep.com/developer/docs
FMP_API_KEY=your_api_key_here

# Alternative variable name (the script will check both)
# API_KEY=your_api_key_here
"""
    
    if not os.path.exists('.env'):
        try:
            with open('.env', 'w') as f:
                f.write(env_template)
            print("✓ Created .env template file. Please add your FMP API key.")
            return False
        except Exception as e:
            print(f"Error creating .env template: {e}")
            return False
    return True


# Usage Example
if __name__ == "__main__":
    try:
        # Check if .env file exists, create template if not
        env_exists = create_env_template()
        
        # Initialize screener (API key will be loaded automatically from .env)
        screener = EarningsRevisionScreener()

        # List of stocks to screen
        stocks_to_screen = [
            'AAPL',
            'MSFT',
            'GOOGL',
            'AMZN',
            'NVDA',
            'META',
            'TSLA',
            'JPM',
            'V',
            'WMT'
        ]

        # Run the screening
        print("Starting Earnings Revision Screening...")
        print(f"Screening {len(stocks_to_screen)} stocks...")

        results = screener.screen_stocks(stocks_to_screen)

        # Print formatted results
        screener.print_results(results)

        # Save results to JSON
        screener.save_results(results)

    except ValueError as e:
        print(f"\n❌ Configuration Error: {e}")
        print("\nTo fix this:")
        print("1. Create a .env file in the same directory as this script")
        print("2. Add the line: FMP_API_KEY=your_actual_api_key")
        print("3. Get a free API key at: https://financialmodelingprep.com/developer/docs")
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("Please check your internet connection and API key.")