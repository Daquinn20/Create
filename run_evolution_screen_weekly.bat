@echo off
REM Evolution Fund + Disruption Index weekly technical screen runner
REM Scheduled: Fridays at 17:00 via Task Scheduler (task name: EvolutionFundScreenerWeekly)
REM Flips Buy Trigger to weekly bars (2-week cross lookback). Other screens unchanged.

cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"

echo ========================================
echo Starting Evolution Fund Weekly Screen
echo %date% %time%
echo ========================================

python screen_evolution_fund.py --weekly --email

echo ========================================
echo Weekly Screen Complete
echo ========================================

echo %date% %time% - Evolution weekly screen completed >> evolution_screen_weekly_log.txt
