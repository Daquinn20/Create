@echo off
echo ============================================
echo Setting up PINK Holdings Tracker Scheduler
echo Time: 7:30 AM local (Mon-Fri)
echo ============================================
echo.
echo This must be run as Administrator!
echo.

schtasks /create /tn "PinkHoldingsTracker" /tr "\"C:\Users\daqui\PycharmProjects\PythonProject1\run_pink_holdings.bat\"" /sc weekly /d MON,TUE,WED,THU,FRI /st 07:30 /ru "%USERNAME%" /f

if %errorlevel%==0 (
    echo.
    echo SUCCESS! Task "PinkHoldingsTracker" created.
    echo It will run Mon-Fri at 7:30 AM.
) else (
    echo.
    echo ERROR: Could not create task. Make sure you run as Administrator.
)

echo.
pause
