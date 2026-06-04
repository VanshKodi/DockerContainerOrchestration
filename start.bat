@echo off
title DockerRProxy Launcher

echo ===============================
echo  DockerRProxy — Starting all 3
echo ===============================
echo.

start "Backend :20000" cmd /c "cd /d "%~dp0backend" && echo [backend] starting & uv run uvicorn main:app --reload --port 20000"
start "Proxy"       cmd /c "cd /d "%~dp0proxy"    && echo [proxy] starting   & uv run python main.py"
start "Frontend :20001" cmd /c "cd /d "%~dp0frontend" && echo [frontend] starting & npm run dev"

echo.
echo Launched:
echo   Backend  → http://127.0.0.1:20000
echo   Proxy    → (binds ports from port_mappings)
echo   Frontend → http://127.0.0.1:20001
echo.
echo Close each window to stop the corresponding service.
pause
