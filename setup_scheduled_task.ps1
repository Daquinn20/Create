$taskName = "DailyNoteGenerator"
$pythonPath = "python"
$scriptPath = "C:\Users\daqui\PycharmProjects\PythonProject1\daily_note_generator.py"
$workingDir = "C:\Users\daqui\PycharmProjects\PythonProject1"

# Remove existing task if it exists
Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

# Create the action
$action = New-ScheduledTaskAction -Execute $pythonPath -Argument $scriptPath -WorkingDirectory $workingDir

# Create the trigger for 8:30 AM daily
$trigger = New-ScheduledTaskTrigger -Daily -At "8:30AM"

# Create settings
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd

# Register the task
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Description "Generates and emails daily market brief at 8:30 AM Eastern"

Write-Host "Scheduled task '$taskName' created successfully to run at 8:30 AM daily."
