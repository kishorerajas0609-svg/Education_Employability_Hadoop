@echo off
title Run Spark & Big Data Analytics
cls
echo ===================================================================
echo   RUNNING BIG DATA & SPARK ML ANALYTICS ON 1M RECORD DATASET
echo ===================================================================
echo.
cd /d "%~dp0"
"BACKEND\venv\Scripts\python.exe" spark_employability_analysis.py
echo.
pause
