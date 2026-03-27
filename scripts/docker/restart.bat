@echo off
REM Restart all containers except builder and devcontainer

REM Define the services you want restarted
set SERVICES=warehouse worker1 worker2 worker3 worker4

echo Restarting services: %SERVICES%
for %%s in (%SERVICES%) do (
    echo Restarting %%s...
    docker-compose -f docker\kionas.docker-compose.yaml restart %%s
)

echo Done!
