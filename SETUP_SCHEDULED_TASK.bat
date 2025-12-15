@echo off
echo ============================================
echo Setting up Daily Brief Scheduler
echo Time: 8:30 AM Boston Time (Mon-Fri)
echo ============================================
echo.
echo This must be run as Administrator!
echo.

schtasks /create /tn "DailyBriefGenerator" /tr "python \"C:\Users\daqui\PycharmProjects\PythonProject1\daily_note_generator.py\"" /sc weekly /d MON,TUE,WED,THU,FRI /st 08:30 /ru "%USERNAME%" /f

if %errorlevel%==0 (
    echo.
    echo SUCCESS! Task "DailyBriefGenerator" created.
    echo It will run Mon-Fri at 8:30 AM.
) else (
    echo.
    echo ERROR: Could not create task. Make sure you run as Administrator.
)

echo.
pause
