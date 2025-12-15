@echo off
echo Starting Company Report Dashboard...
echo.
echo Installing/updating required packages...
pip install -q flask flask-cors python-dotenv requests
echo.
echo Starting backend server...
echo Dashboard will be available at http://localhost:5000
echo.
echo Press Ctrl+C to stop the server
echo.
python company_report_backend.py
pause
