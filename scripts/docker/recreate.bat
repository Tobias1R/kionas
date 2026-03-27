
@echo off
REM Rebuild and restart all containers except builder and devcontainer

set SERVICES=metastore warehouse flight-proxy worker1 worker2 worker3 worker4 ui-backend

echo Rebuilding and restarting services: %SERVICES%
for %%s in (%SERVICES%) do (
    echo Rebuilding and restarting %%s...
    docker-compose -f docker\kionas.docker-compose.yaml up --build -d %%s
)

echo Done!