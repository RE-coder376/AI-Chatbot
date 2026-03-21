@echo off
REM start_server.bat — Reliably starts the FastAPI server on Windows
REM Kills any existing process on port 8000 first, then starts fresh.

echo [%time%] Checking for existing process on port 8000...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
    echo [%time%] Killing PID %%a
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 3 /nobreak >nul

echo [%time%] Starting server...
cd /d "%~dp0"
start "ChatbotServer" /B python -u app.py >> server_out.log 2>> server_err.log

echo [%time%] Server starting in background. Logs: server_out.log / server_err.log
echo [%time%] Run: python sequential_crawl.py    to start crawling all DBs
