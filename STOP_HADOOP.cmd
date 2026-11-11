@echo off
title Stop Hadoop Cluster
cls
echo ===================================================================
echo             STOPPING APACHE HADOOP DAEMONS
echo ===================================================================
echo.
if exist "C:\hadoop\sbin\stop-dfs.cmd" (
    call "C:\hadoop\sbin\stop-dfs.cmd"
) else (
    echo [ERROR] C:\hadoop\sbin\stop-dfs.cmd not found!
)
echo.
echo Checking remaining Java processes (jps):
jps
echo.
echo Hadoop daemons have been stopped.
pause
