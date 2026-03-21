@echo off
setlocal

echo [qol] cleaning local Rust build artifacts...
if exist target rmdir /s /q target
if exist docker-target rmdir /s /q docker-target

echo [qol] pruning docker builder cache...
docker builder prune -f

echo [qol] done.
endlocal
