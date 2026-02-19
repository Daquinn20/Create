@echo off
REM Daily Estimates Tracker - Run via Windows Task Scheduler
REM Uses Master Universe (centralized ticker source with US + International stocks)
REM This captures analyst estimates daily to track real revisions over time

cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"
python estimates_tracker.py --universe master

REM Log completion
echo %date% %time% - Estimates captured (Master Universe) >> estimates_log.txt
