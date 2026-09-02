# Setup — fresh machine

Steps to reproduce this environment after a clone.

## 1. Python + packages

Requires Python 3.12+. Core packages used by the DCA pipeline:

```
pip install pandas numpy yfinance requests python-dotenv openpyxl
```

## 2. Environment variables (`.env`)

The signal-log emailer reads Gmail credentials from a `.env` file in the
project root. This file is intentionally NOT committed. Create it as:

```
EMAIL_ADDRESS=your.address@gmail.com
EMAIL_PASSWORD=your_gmail_app_password
```

`EMAIL_PASSWORD` must be a Gmail **app password** (not the account
password) — generate one at https://myaccount.google.com/apppasswords .

Optional, only if using the FMP data source instead of yfinance:

```
FMP_API_KEY=your_fmp_key
```

## 3. Windows Task Scheduler — DCA signal log

Registers the weekly runner. Run in PowerShell as your user:

```powershell
$action   = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c C:\Users\daqui\PycharmProjects\PythonProject1\run_dca_signal_log.bat"
$trigger  = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At 8:45am
$settings = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable
Register-ScheduledTask -TaskName "DCASignalLog" -Action $action -Trigger $trigger -Settings $settings
```

Verify:

```powershell
Get-ScheduledTask -TaskName DCASignalLog
Get-ScheduledTaskInfo -TaskName DCASignalLog | Select LastRunTime, NextRunTime, LastTaskResult
```

## 4. Reseeding the signal log (if lost)

`dca_signal_log.csv` grows over time. If missing after a fresh clone (or
after the file is deleted), reseed with backfilled history:

```
python dca_signal_log.py --tickers-file disruption_index.csv --backfill 8
```

The `--backfill 8` flag adds all validated signals from the last 8 years
of price history in one pass.

## 5. Ticker sources

Both live in the project root, both tracked in git:

- `disruption_index.csv` — the DCA overlay universe
- `SP500_list.xlsx` — used by analysis scripts when passed `--universe sp500`

## 6. Other scheduled tasks in this project

See `.claude/projects/*/memory/project_scheduled_screens.md` for the
Pink / Evolution / WRFreshCross / DCASignalLog daily/weekly jobs and
their Task Scheduler names.
