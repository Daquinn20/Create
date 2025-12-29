@echo off
REM Daily Estimates Tracker - Run via Windows Task Scheduler
REM This captures analyst estimates daily to track real revisions over time

cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"
python estimates_tracker.py --universe both

REM Log completion
echo %date% %time% - Estimates captured >> estimates_log.txt
