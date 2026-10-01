@echo off
title Meli Intelligence Bot - Telegram (Joca)
chcp 65001 > nul
cd /d "%~dp0"

echo ======================================================================
echo   INICIALIZANDO O BOT DE VENDAS DO MERCADO LIVRE (@Joca_Meli_bot)
echo ======================================================================
echo.
echo Pressione Ctrl+C para encerrar o bot a qualquer momento.
echo.

python -u bot_joca_meli.py

pause
