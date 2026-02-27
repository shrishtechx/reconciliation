@echo off
echo Starting Ledger Reconciliation...
echo.
echo Backend: http://localhost:8000
echo Frontend: http://localhost:3000
echo.
start "Backend" cmd /k "cd /d %~dp0 && python server.py"
timeout /t 3 /nobreak >nul
start "Frontend" cmd /k "cd /d %~dp0\frontend && npm run dev"
echo.
echo Both servers starting. Open http://localhost:3000 in your browser.
pause
