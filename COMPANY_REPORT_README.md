# Stock Company Report Dashboard

A comprehensive dashboard that generates detailed company reports by fetching credible data from Financial Modeling Prep (FMP) and Fiscal.ai APIs.

## Features

The dashboard generates reports with the following sections:

1. **Business Overview** - Company description, sector, industry, employees, headquarters, market cap, and current price
2. **Revenue by Segment** - Revenue breakdown by business segments plus Gross, Operating, and Net Margins
3. **Competitive Advantages** - Key differentiators derived from financial metrics
4. **Highlights from Recent Quarters** - Last 4 quarters of earnings data with QoQ growth
5. **Competition** - Top 5 competitors with market cap comparison
6. **Management** - Key executives with titles and compensation
7. **Valuations** - P/E, P/S, P/B, EV/EBITDA, PEG, and P/FCF ratios

## Data Sources

All data comes from credible sources:
- **Primary**: Financial Modeling Prep (FMP) API - provides comprehensive financial data
- **Secondary**: Fiscal.ai API - provides additional insights (configured but not yet fully integrated)

## Files

- `stock_report_dashboard.html` - Frontend dashboard interface
- `company_report_backend.py` - Flask backend that fetches data from APIs
- `Launch_Company_Report_Dashboard.bat` - Quick launch script for Windows
- `.env` - Contains API keys (FMP_API_KEY and FISCAL_AI_API_KEY)

## How to Use

### Option 1: Quick Launch (Windows)
1. Double-click `Launch_Company_Report_Dashboard.bat`
2. Wait for the server to start
3. Your browser should open automatically at http://localhost:5000
4. Enter a stock symbol (e.g., AAPL, MSFT, GOOGL) and click "Generate Report"

### Option 2: Manual Launch
1. Install dependencies:
   ```bash
   pip install flask flask-cors python-dotenv requests
   ```

2. Start the backend server:
   ```bash
   python company_report_backend.py
   ```

3. Open your browser and go to http://localhost:5000

4. Enter a stock symbol and click "Generate Report"

## API Keys

The dashboard requires API keys stored in the `.env` file:
- `FMP_API_KEY` - Financial Modeling Prep API key
- `FISCAL_AI_API_KEY` - Fiscal.ai API key

Both keys are already configured in your `.env` file.

## Example Stock Symbols to Try

- AAPL (Apple)
- MSFT (Microsoft)
- GOOGL (Alphabet/Google)
- AMZN (Amazon)
- NVDA (NVIDIA)
- TSLA (Tesla)
- META (Meta/Facebook)

## Troubleshooting

**Error: "Make sure the backend server is running"**
- Ensure you've started the backend server using the launch script or manual method
- Check that port 5000 is not being used by another application

**Error: "FMP API key not found"**
- Verify your `.env` file contains `FMP_API_KEY=your_key_here`

**No data showing for a symbol**
- Verify the stock symbol is correct
- Some symbols may not have complete data available
- Check the browser console (F12) for detailed error messages

## Technical Details

### Backend Endpoints

- `GET /` - Serves the dashboard HTML
- `GET /api/report/<symbol>` - Returns complete company report JSON
- `GET /api/health` - Health check endpoint

### Data Flow

1. User enters stock symbol in frontend
2. Frontend makes AJAX request to backend API
3. Backend fetches data from FMP API:
   - Company profile
   - Revenue segments
   - Financial ratios
   - Income statements
   - Key executives
   - Competitor data
4. Backend processes and formats the data
5. Frontend receives JSON and populates all report sections
6. Report is displayed with professional formatting

## Future Enhancements

- Full integration with Fiscal.ai for additional insights
- Export report to PDF
- Compare multiple companies side-by-side
- Historical trend charts
- Email report functionality
- Save/bookmark favorite companies
