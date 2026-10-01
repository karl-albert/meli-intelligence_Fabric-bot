@echo off
title Testar Joca Meli Bot na Nuvem
chcp 65001 > nul

powershell -ExecutionPolicy Bypass -File "%~dp0Testar_Joca_Nuvem.ps1"

echo.
pause
