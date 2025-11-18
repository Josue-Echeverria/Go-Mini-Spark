@echo off
echo Building Procesamiento Distribuido...

echo Building Master...
cd master
go build -o ..\bin\master.exe .
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