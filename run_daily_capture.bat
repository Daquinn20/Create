@echo off
REM Daily Estimates Capture Script
REM Run this manually or schedule via Task Scheduler

cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"

echo ========================================
echo Starting Daily Estimates Capture
echo %date% %time%
echo ========================================

python estimates_tracker.py --universe master

echo ========================================
echo Capture Complete
echo ========================================

REM Log completion time
echo %date% %time% - Capture completed >> capture_log.txt
