@echo off
echo Starting Earnings Revision Screener Dashboard...
echo.
echo Opening browser to http://localhost:8501
echo.
start http://localhost:8501
streamlit run earnings_dashboard.py --server.headless=true
