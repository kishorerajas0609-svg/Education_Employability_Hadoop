@echo off
title Launch All Services
cls
echo ===================================================================
echo     EDU-PREDICT: LAUNCHING HADOOP + BACKEND + WEBSITE
echo ===================================================================
echo.
echo [1/3] Starting Apache Hadoop (NameNode and DataNode)...
start "Hadoop DFS Services" cmd /k "call C:\hadoop\sbin\start-dfs.cmd"

echo.
echo [2/3] Starting FastAPI Backend on Port 8000...
start "FastAPI Backend" cmd /k "cd /d %~dp0BACKEND && call venv\Scripts\activate.bat && python -m uvicorn main:app --reload --port 8000"

echo.
echo [3/3] Opening Web Application in Browser...
timeout /t 3 /nobreak >nul
start "" "%~dp0index.html"

echo.
echo ===================================================================
echo  All services are running:
echo   - Website: Opened in your default browser
echo   - NameNode Web UI: http://localhost:9870
echo   - Backend Swagger API: http://127.0.0.1:8000/docs
echo ===================================================================
echo.
pause
