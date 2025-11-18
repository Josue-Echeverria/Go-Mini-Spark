@echo off

if not exist "bin\" mkdir bin

start "Master" bin\master.exe

timeout /t 3 /nobreak >nul

start "Worker 1" bin\worker.exe localhost:8080 localhost 8081

start "Worker 2" bin\worker.exe localhost:8080 localhost 8082

echo All services started!
echo Master: http://localhost:8080
echo Worker 1: http://localhost:8081
echo Worker 2: http://localhost:8082
echo.
echo Use client.exe to submit jobs:
echo   bin\client.exe localhost:8080 submit batch "Test Job"