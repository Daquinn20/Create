@echo off
REM Daily Note Generator - Automated Run
REM This batch file runs the daily note generator script

cd /d "C:\Users\daqui\PycharmProjects\PythonProject1"
python daily_note_generator.py

REM Log completion
echo Daily note generation completed at %date% %time% >> daily_note_log.txt
