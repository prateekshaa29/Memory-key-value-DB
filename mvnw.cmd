@echo off
rem Maven Wrapper script for ConcurrentKVStore on Windows

setlocal

rem Check if mvn is directly in PATH
where mvn >nul 2>&1
if %ERRORLEVEL% equ 0 (
    mvn %*
    exit /b %ERRORLEVEL%
)

rem Check user .m2 wrapper distribution
set M2_WRAPPER_BIN=C:\Users\work\.m2\wrapper\dists\apache-maven-3.9.9-bin\4nf9hui3q3djbarqar9g711ggc\apache-maven-3.9.9\bin\mvn.cmd
if exist "%M2_WRAPPER_BIN%" (
    call "%M2_WRAPPER_BIN%" %*
    exit /b %ERRORLEVEL%
)

echo [ERROR] Maven not found on PATH or in .m2 wrapper. Please install Maven or add it to PATH.
exit /b 1
