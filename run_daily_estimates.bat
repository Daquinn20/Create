@echo off
REM Daily Estimates Tracker - Run via Windows Task Scheduler
REM Uses Master Universe (centralized ticker source with US + International stocks)
REM This captures analyst estimates daily to track real revisions over time

cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"
python estimates_tracker.py --universe master

REM Backup database to OneDrive
set BACKUP_PATH=C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\AI dashboard Data\Earnings Estimates data
powershell -Command "Copy-Item 'estimates_history.db' -Destination '%BACKUP_PATH%\estimates_history.db' -Force"

REM Weekly backup with timestamp (every Sunday)
for /f "tokens=1" %%d in ('powershell -command "(Get-Date).DayOfWeek"') do set DOW=%%d
if "%DOW%"=="Sunday" (
    powershell -Command "Copy-Item 'estimates_history.db' -Destination '%BACKUP_PATH%\estimates_history_$(Get-Date -Format yyyy-MM-dd).db' -Force"
    echo %date% %time% - Weekly backup created >> estimates_log.txt
)

REM Log completion
echo %date% %time% - Estimates captured and backed up to OneDrive >> estimates_log.txt
