# Remove existing task and recreate with correct settings
Unregister-ScheduledTask -TaskName "DailyBriefGenerator" -Confirm:$false -ErrorAction SilentlyContinue

$action = New-ScheduledTaskAction -Execute "python" -Argument "daily_note_generator.py" -WorkingDirectory "C:\Users\daqui\PycharmProjects\PythonProject1"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 8:30AM
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

Register-ScheduledTask -TaskName "DailyBriefGenerator" -Action $action -Trigger $trigger -Settings $settings -Description "Generate Daily Brief every weekday at 8:30 AM"
