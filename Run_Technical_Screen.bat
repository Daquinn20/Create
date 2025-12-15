@echo off
cd /d C:\Users\daqui\PycharmProjects\PythonProject1
call .venv\Scripts\activate
streamlit run Technical_Screen_Quinn.py --server.port 8502 --server.headless true
pause
