@echo off

if not exist "bin\" mkdir bin

start "Driver" bin\Driver.exe -port 9000

timeout /t 1 /nobreak >nul

start "Worker 1" bin\worker.exe localhost:9000 localhost:9100

timeout /t 1 /nobreak >nul

start "Worker 2" bin\worker.exe localhost:9000 localhost:9101

timeout /t 1 /nobreak >nul

start "Worker 3" bin\worker.exe localhost:9000 localhost:9102

timeout /t 1 /nobreak >nul

start "Worker 4" bin\worker.exe localhost:9000 localhost:9103

echo Services started:
echo Driver: http://localhost:9000
echo Worker 1: http://localhost:9100
echo Worker 2: http://localhost:9101
echo Worker 3: http://localhost:9102
echo Worker 4: http://localhost:9103