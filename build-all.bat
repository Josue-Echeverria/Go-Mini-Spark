@echo off
REM Build script for Go-Mini-Spark

echo Building Go-Mini-Spark components...
echo.

REM Create bin directory if it doesn't exist
if not exist "bin" mkdir bin

REM Build Driver
echo [1/3] Building Driver...
go build -o bin\driver.exe driver\main.go
if %errorlevel% neq 0 (
    echo ERROR: Driver build failed
    exit /b 1
)
echo ✓ Driver built successfully

REM Build Worker
echo [2/3] Building Worker...
go build -o bin\worker.exe worker\main.go
if %errorlevel% neq 0 (
    echo ERROR: Worker build failed
    exit /b 1
)
echo ✓ Worker built successfully

REM Build Client
echo [3/3] Building Client...
go build -o bin\client.exe client\main.go
if %errorlevel% neq 0 (
    echo ERROR: Client build failed
    exit /b 1
)
echo ✓ Client built successfully

echo.
echo ============================================
echo All components built successfully!
echo.
echo Binaries available in bin\:
echo   - driver.exe  (Master/Coordinator)
echo   - worker.exe  (Worker nodes)
echo   - client.exe  (CLI client)
echo ============================================
