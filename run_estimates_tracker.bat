@echo off
REM Daily Estimates Tracker - Run via Windows Task Scheduler
REM Uses Master Universe (centralized ticker source with US + International stocks)
cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"
python estimates_tracker.py --universe master

REM Backup database to OneDrive
set BACKUP_PATH=C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\AI dashboard Data\Earnings Estimates data
powershell -Command "Copy-Item 'estimates_history.db' -Destination '%BACKUP_PATH%\estimates_history.db' -Force"
echo %date% %time% - Database backed up to OneDrive >> estimates_log.txt
