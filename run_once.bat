@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

for /f "usebackq eol=# tokens=1,* delims==" %%A in ("ecoflow.env") do (
    if not "%%A"=="" set "%%A=%%B"
)

"C:\Users\Rid\AppData\Local\Programs\Python\Python314\python.exe" "%~dp0ecoflow_telegram_monitor.py" --once >> "%~dp0run.log" 2>&1
