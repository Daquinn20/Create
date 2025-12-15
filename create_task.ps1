$action = New-ScheduledTaskAction -Execute 'python' -Argument 'C:\Users\daqui\PycharmProjects\PythonProject1\daily_note_generator.py' -WorkingDirectory 'C:\Users\daqui\PycharmProjects\PythonProject1'
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 8:30AM
Register-ScheduledTask -TaskName 'DailyBriefGenerator' -Action $action -Trigger $trigger -Description 'Generates daily market brief and sends email' -Force
Write-Host "Scheduled task 'DailyBriefGenerator' created successfully for 8:30 AM Boston time, Monday-Friday"
