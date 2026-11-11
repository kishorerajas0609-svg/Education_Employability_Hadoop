@echo off
title Hadoop Cluster Launcher
cls
echo ===================================================================
echo             APACHE HADOOP HDFS LAUNCHER
echo ===================================================================
echo.
echo Starting NameNode and DataNode daemons...
echo (Two CMD daemon windows will open: NameNode and DataNode)
echo.

if exist "C:\hadoop\sbin\start-dfs.cmd" (
    call "C:\hadoop\sbin\start-dfs.cmd"
) else (
    echo [ERROR] C:\hadoop\sbin\start-dfs.cmd not found!
    echo Please verify your Hadoop installation path.
    pause
    exit /b 1
)

echo.
echo Waiting 5 seconds for services to initialize...
timeout /t 5 /nobreak >nul

echo.
echo Active Java Daemons (jps):
jps

echo.
echo ===================================================================
echo  NameNode Web UI is accessible at: http://localhost:9870
echo ===================================================================
echo.
pause
