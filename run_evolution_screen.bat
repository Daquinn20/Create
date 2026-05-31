@echo off
REM Evolution Fund + Disruption Index technical screen runner
REM Scheduled: Mon-Fri at 08:00 via Task Scheduler (task name: EvolutionFundScreener)

cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"

echo ========================================
echo Starting Evolution Fund Screen
echo %date% %time%
echo ========================================

python screen_evolution_fund.py --email

echo ========================================
echo Screen Complete
echo ========================================

echo %date% %time% - Evolution screen completed >> evolution_screen_log.txt
