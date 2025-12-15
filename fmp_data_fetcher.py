"""
FMP Data Fetcher Module
Fetches financial data from Financial Modeling Prep API
"""
import os
import requests
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/api/v3"


@dataclass
class FinancialSnapshot:
    """Data class to hold financial snapshot for a single period."""
    ticker: str
    period_label: str
    revenue: Optional[float] = None
    cogs: Optional[float] = None
    operating_income: Optional[float] = None
    net_income: Optional[float] = None
    operating_cash_flow: Optional[float] = None
    capex: Optional[float] = None
    total_debt: Optional[float] = None
    cash: Optional[float] = None
    accounts_receivable: Optional[float] = None
    inventory: Optional[float] = None
    goodwill: Optional[float] = None
    share_based_comp: Optional[float] = None
    shares_outstanding: Optional[float] = None


class DataSourceError(Exception):
    """Custom exception for data source errors."""
    pass


def _fmp_get(path: str, params: Dict[str, Any]) -> Any:
    """
    Make a GET request to the Financial Modeling Prep API.

    Args:
        path: API endpoint path (e.g., "income-statement/AAPL")
        params: Query parameters dictionary

    Returns:
        JSON response from the API

    Raises:
        DataSourceError: If API key is missing or request fails
    """
    if not FMP_API_KEY:
        raise DataSourceError("Missing FMP_API_KEY in .env")

    params = dict(params)
    params["apikey"] = FMP_API_KEY
    url = f"{FMP_BASE}/{path}"

    try:
        resp = requests.get(url, params=params, timeout=15)

        if resp.status_code != 200:
            raise DataSourceError(
                f"FMP API error {resp.status_code}: {resp.text[:200]}"
            )

        data = resp.json()

        # Handle potential error responses that still return 200
        if isinstance(data, dict) and "Error Message" in data:
            raise DataSourceError(f"FMP API error: {data['Error Message']}")

        return data

    except requests.exceptions.Timeout:
        raise DataSourceError("Request to FMP API timed out after 15 seconds")
    except requests.exceptions.RequestException as e:
        raise DataSourceError(f"Network error connecting to FMP API: {str(e)}")
    except ValueError as e:
        raise DataSourceError(f"Invalid JSON response from FMP API: {str(e)}")


def fetch_fmp_financials(
        ticker: str,
        limit: int = 5,
        period: str = "annual",
) -> List[FinancialSnapshot]:
    """
    Fetch financial data from Financial Modeling Prep API.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        limit: Number of periods to fetch (default: 5, max recommended: 120)
        period: "annual" or "quarter" (default: "annual")

    Returns:
        List of FinancialSnapshot objects, ordered by date (most recent first)

    Raises:
        DataSourceError: If API requests fail or data is invalid
    """
    ticker = ticker.upper()

    # Validate period parameter
    if period not in ["annual", "quarter"]:
        raise ValueError("period must be 'annual' or 'quarter'")

    # Validate limit
    if limit < 1:
        raise ValueError("limit must be at least 1")

    # Fetch all three financial statement types
    try:
        inc = _fmp_get(
            f"income-statement/{ticker}",
            {"period": period, "limit": limit}
        )
        bs = _fmp_get(
            f"balance-sheet-statement/{ticker}",
            {"period": period, "limit": limit}
        )
        cf = _fmp_get(
            f"cash-flow-statement/{ticker}",
            {"period": period, "limit": limit}
        )
    except DataSourceError as e:
        raise DataSourceError(
            f"Failed to fetch financial data for {ticker}: {str(e)}"
        )

    # Validate that we got data
    if not inc or not isinstance(inc, list):
        raise DataSourceError(f"No income statement data found for {ticker}")
    if not bs or not isinstance(bs, list):
        raise DataSourceError(f"No balance sheet data found for {ticker}")
    if not cf or not isinstance(cf, list):
        raise DataSourceError(f"No cash flow data found for {ticker}")

    # Merge data by period date
    periods: Dict[str, Dict[str, Any]] = {}

    def merge(block: List[Dict], key: str) -> None:
        """Merge statement data into periods dictionary."""
        for row in block:
            date = row.get("date")
            if not date:
                continue
            periods.setdefault(date, {})[key] = row

    merge(inc, "inc")
    merge(bs, "bs")
    merge(cf, "cf")

    # Create FinancialSnapshot objects
    snapshots = []
    for date in sorted(periods.keys(), reverse=True):
        p = periods[date]
        inc_row = p.get("inc", {})
        bs_row = p.get("bs", {})
        cf_row = p.get("cf", {})

        # Create snapshot with safe None handling
        snapshot = FinancialSnapshot(
            ticker=ticker,
            period_label=date,
            revenue=inc_row.get("revenue"),
            cogs=inc_row.get("costOfRevenue"),
            operating_income=inc_row.get("operatingIncome"),
            net_income=inc_row.get("netIncome"),
            operating_cash_flow=cf_row.get("operatingCashFlow"),
            capex=cf_row.get("capitalExpenditure"),
            total_debt=bs_row.get("totalDebt"),
            cash=bs_row.get("cashAndCashEquivalents"),
            accounts_receivable=bs_row.get("netReceivables"),
            inventory=bs_row.get("inventory"),
            goodwill=bs_row.get("goodwill"),
            share_based_comp=inc_row.get("stockBasedCompensation"),
            shares_outstanding=inc_row.get("weightedAverageShsOutDil"),
        )
        snapshots.append(snapshot)

    if not snapshots:
        raise DataSourceError(
            f"No valid financial data could be constructed for {ticker}"
        )

    return snapshots


# Example usage and testing
if __name__ == "__main__":
    try:
        # Test with Apple stock
        print("Fetching financial data for AAPL...")
        snapshots = fetch_fmp_financials("AAPL", limit=3, period="annual")

        print(f"\nFound {len(snapshots)} periods of data:")
        for snap in snapshots:
            print(f"\nPeriod: {snap.period_label}")
            print(f"  Revenue: ${snap.revenue:,.0f}" if snap.revenue else "  Revenue: N/A")
            print(f"  Net Income: ${snap.net_income:,.0f}" if snap.net_income else "  Net Income: N/A")
            print(f"  Operating Cash Flow: ${snap.operating_cash_flow:,.0f}" if snap.operating_cash_flow else "  Operating Cash Flow: N/A")

    except DataSourceError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")