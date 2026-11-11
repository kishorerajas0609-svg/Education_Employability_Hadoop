@echo off
title FastAPI Backend Server (Uvicorn)
cls
echo ===================================================================
echo             FASTAPI & APACHE WEBSERVER BACKEND
echo ===================================================================
echo.
cd /d "%~dp0BACKEND"

if exist "venv\Scripts\activate.bat" (
    echo Activating Python virtual environment...
    call venv\Scripts\activate.bat
) else (
    echo [WARNING] venv\Scripts\activate.bat not found, running using system Python...
)

echo.
echo Starting Uvicorn Server on http://127.0.0.1:8000 ...
echo Swagger API Docs: http://127.0.0.1:8000/docs
echo.
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
pause
