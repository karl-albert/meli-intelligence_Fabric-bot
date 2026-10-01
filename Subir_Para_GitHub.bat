@echo off
title Subir Bot Fabric para o GitHub
chcp 65001 > nul

echo ======================================================================
echo   ENVIANDO MELI INTELLIGENCE FABRIC BOT PARA O SEU GITHUB
echo   Repositorio: https://github.com/karl-albert/meli-intelligence_Fabric-bot
echo ======================================================================
echo.

git remote remove origin 2>nul
git remote add origin https://github.com/karl-albert/meli-intelligence_Fabric-bot.git
git branch -M main
git add .
git commit -m  feat: atualizacao da rotina Joca Meli Fabric 2>nul
git push -u origin main

echo.
if %ERRORLEVEL% EQU 0 (
    echo ======================================================================
    echo   [SUCESSO] Codigo enviado com sucesso para o seu GitHub!
    echo   Link: https://github.com/karl-albert/meli-intelligence_Fabric-bot
    echo ======================================================================
) else (
    echo [!] Se pediu login, autentique com seu token ou senha do GitHub.
)
echo.
pause
