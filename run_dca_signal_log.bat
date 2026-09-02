@echo off
REM DCA Matrix — weekly signal log & running edge tracker
REM Scheduled: Monday 08:45 via Task Scheduler (task name: DCASignalLog)

cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"

echo ========================================
echo Starting DCA Signal Log
echo %date% %time%
echo ========================================

python dca_signal_log.py --tickers-file disruption_index.csv --email

echo ========================================
echo DCA Signal Log Complete
echo ========================================

echo %date% %time% - DCA signal log completed >> dca_signal_log_run.txt
