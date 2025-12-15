# PowerShell script to set up Windows Task Scheduler for Daily Note Generator
# Run this script as Administrator

$TaskName = "DailyNoteGenerator"
$TaskDescription = "Generates and emails daily investment brief every weekday at 8:15 AM ET"
$ScriptPath = "C:\Users\daqui\PycharmProjects\PythonProject1\run_daily_note.bat"
$Time = "08:15AM"

# Check if task already exists and delete it
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing task..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create the action (what to run)
$Action = New-ScheduledTaskAction -Execute $ScriptPath

# Create the trigger (when to run) - Weekdays at 8:15 AM
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $Time

# Create the settings
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

# Register the scheduled task
Register-ScheduledTask -TaskName $TaskName -Description $TaskDescription -Action $Action -Trigger $Trigger -Settings $Settings -User $env:USERNAME

Write-Host ""
Write-Host "================================"
Write-Host "Scheduled Task Created Successfully!"
Write-Host "================================"
Write-Host "Task Name: $TaskName"
Write-Host "Schedule: Every weekday (Mon-Fri) at 8:15 AM Eastern Time"
Write-Host "Script: $ScriptPath"
Write-Host ""
Write-Host "The daily note will be automatically generated and emailed to:"
Write-Host "daquinn@targetedequityconsulting.com"
Write-Host ""
Write-Host "To view the task in Task Scheduler, run: taskschd.msc"
Write-Host "================================"