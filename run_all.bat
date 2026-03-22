@echo off
setlocal

echo Starting Beehive Digital Twin system (PRODUCTION MODE)...

REM 1) Start backend (no reload)
start "Backend" cmd /k "cd /d %~dp0backend && call venv\Scripts\activate && uvicorn app.main:app --host 0.0.0.0 --port 8000"

timeout /t 50 /nobreak > nul

REM 2) Start frontend (built version)
start "Frontend" cmd /k "cd /d %~dp0frontend && serve -s dist"

REM 3) Start simulator
start "Simulator" cmd /k "cd /d %~dp0backend && call venv\Scripts\activate && python simulator.py --cycle-seconds 900 --enable-demo-noise --missing-prob 0.3 --spike-prob 0.02"

echo.
echo System running in STABLE mode
echo Backend:  http://127.0.0.1:8000
echo Frontend: http://127.0.0.1:3000
echo.
pause
