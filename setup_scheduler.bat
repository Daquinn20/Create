@echo off
schtasks /create /tn "DailyBriefGenerator" /tr "python C:\Users\daqui\PycharmProjects\PythonProject1\daily_note_generator.py" /sc weekly /d MON,TUE,WED,THU,FRI /st 08:30 /f
echo Task created successfully!
pause
