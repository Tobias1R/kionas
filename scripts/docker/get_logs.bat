@echo off
REM Restart all containers except builder and devcontainer

REM Define the services you want restarted
set SERVICES=kionas-metastore kionas-warehouse kionas-flight-proxy kionas-worker1 kionas-worker2 kionas-worker3 kionas-worker4 kionas-ui-backend
set LOGS_OUTPUT_DIR=logs

echo clearing old logs in %LOGS_OUTPUT_DIR%...
if exist %LOGS_OUTPUT_DIR% (
    del /Q %LOGS_OUTPUT_DIR%\*.log
) else (
    mkdir %LOGS_OUTPUT_DIR%
)

echo Restarting services: %SERVICES%
for %%s in (%SERVICES%) do (
    echo Restarting %%s...
    docker logs %%s > %LOGS_OUTPUT_DIR%\%%s.log 2>&1
)

echo Done!