@echo off
REM Daily Estimates Tracker - Run via Windows Task Scheduler
REM Uses Master Universe (centralized ticker source with US + International stocks)
cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"
python estimates_tracker.py --universe master
