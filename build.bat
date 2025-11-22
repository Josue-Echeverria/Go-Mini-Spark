@echo off
echo Building Mini-Spark...

echo Building Driver...
cd driver
go build -o ..\bin\driver.exe .
cd ..

echo Building Worker...
cd worker
go build -o ..\bin\worker.exe .
cd ..

echo Building Client...
cd client
go build -o ..\bin\client.exe .
cd ..

echo Build complete! Binaries are in the bin\ directory.