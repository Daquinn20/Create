@echo off
REM PINK (Simplify Health Care ETF) daily holdings tracker
REM Scheduled: daily via Task Scheduler (task name: PinkHoldingsTracker)

cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"

echo ========================================
echo Starting PINK Holdings Tracker
echo %date% %time%
echo ========================================

python pink_holdings_tracker.py --email

echo ========================================
echo Tracker Complete
echo ========================================

echo %date% %time% - PINK holdings tracker completed >> pink_holdings_log.txt
