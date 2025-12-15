$action = New-ScheduledTaskAction -Execute "python" -Argument "daily_note_generator.py" -WorkingDirectory "C:\Users\daqui\PycharmProjects\PythonProject1"
Set-ScheduledTask -TaskName "DailyBriefGenerator" -Action $action
