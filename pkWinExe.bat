@echo on

call pkg ./bin/http-server  -t node18-win --out-path ./bin
call copy /Y "bin\http-server.exe"  "..\..\Exec\"

pause
