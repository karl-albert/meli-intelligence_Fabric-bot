@echo off
chcp 65001 > nul
title Atualizador Mercado Livre — Microsoft Fabric (Direct Lake)
color 0E

echo ================================================================================
echo   MERCADO LIVRE ANALYTICS — ATUALIZADOR AUTOMATICO MICROSOFT FABRIC
echo   Workspace KAC · Lakehouse LH_MercadoLivre · Modelo SM_MercadoLivre
echo ================================================================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0Atualizar_Mercado_Livre_Fabric.ps1"

echo.
echo ================================================================================
echo   Processo finalizado. Pressione qualquer tecla para sair...
echo ================================================================================
pause > nul
