@echo off
title Sincronizar Base BigQuery para Local e Fabric
chcp 65001 > nul

echo ======================================================================
echo   SINCRONIZANDO DADOS DO GOOGLE BIGQUERY PARA O COMPUTADOR LOCAL
echo ======================================================================
echo.

python "%~dp0Sincronizar_Base_BigQuery.py"

echo.
pause
