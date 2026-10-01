@echo off
title Atualizar Base do Joca e Enviar para a Nuvem
chcp 65001 > nul

echo ======================================================================
echo   ATUALIZADOR AUTOMATICO DO JOCA (MERCADO LIVRE - POWER BI - NUVEM)
echo ======================================================================
echo.
echo Certifique-se de que o Power BI Desktop está aberto e atualizado.
echo.

powershell -ExecutionPolicy Bypass -File "%~dp0Atualizar_Base_Joca_Nuvem.ps1"

echo.
pause
