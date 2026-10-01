Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "       TESTADOR DE STATUS DO JOCA BOT NA NUVEM (RENDER 24/7)          " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

$botUrl = "https://meli-intelligence-bot.onrender.com"
$token = "8916733671:AAH1htvd6VqDKsngdyYsFOaXdvNgUQ0RjyM"

# 1. Teste de Conexão com o Render
Write-Host "[1/4] Verificando se o servidor Render está acordado e ativo..." -ForegroundColor Yellow
try {
    $status = Invoke-RestMethod -Uri "$botUrl/status" -TimeoutSec 45
    Write-Host " -> Servidor Render: ONLINE 🟢" -ForegroundColor Green
    Write-Host " -> Registros carregados: $($status.total_registros)" -ForegroundColor White
    Write-Host " -> Data mais recente na base: $($status.data_recente)" -ForegroundColor White
} catch {
    Write-Host " -> [ERRO] Servidor demorou para responder ou está offline: $_" -ForegroundColor Red
}

# 2. Teste do Webhook do Telegram
Write-Host "`n[2/4] Verificando conexão do Webhook com o Telegram..." -ForegroundColor Yellow
try {
    $wh = Invoke-RestMethod -Uri "https://api.telegram.org/bot$token/getWebhookInfo" -TimeoutSec 10
    if ($wh.ok -and $wh.result.url -match "meli-intelligence-bot") {
        Write-Host " -> Webhook Telegram: CONECTADO 🟢" -ForegroundColor Green
        Write-Host " -> URL Ativa: $($wh.result.url)" -ForegroundColor White
        Write-Host " -> Mensagens pendentes: $($wh.result.pending_update_count)" -ForegroundColor White
    } else {
        Write-Host " -> [ALERTA] Webhook não está apontando para o Render: $($wh.result.url)" -ForegroundColor Red
    }
} catch {
    Write-Host " -> [ERRO] Falha ao consultar API do Telegram: $_" -ForegroundColor Red
}

# 3. Teste de Consulta da IA (Saudação)
Write-Host "`n[3/4] Testando comando /start..." -ForegroundColor Yellow
try {
    $testStart = Invoke-RestMethod -Uri "$botUrl/test_ai?q=/start" -TimeoutSec 20
    Write-Host " -> Resposta do Joca para /start:" -ForegroundColor Cyan
    Write-Host $testStart.resposta -ForegroundColor Gray
} catch {
    Write-Host " -> [ERRO] Falha ao testar /start: $_" -ForegroundColor Red
}

# 4. Teste de Pergunta Analítica (Gemini + DuckDB)
Write-Host "`n[4/4] Testando pergunta real: 'Qual o faturamento de hoje?'..." -ForegroundColor Yellow
try {
    $testFat = Invoke-RestMethod -Uri "$botUrl/test_ai?q=qual+o+faturamento+de+hoje" -TimeoutSec 30
    Write-Host " -> Resposta do Joca para 'qual o faturamento de hoje':" -ForegroundColor Cyan
    Write-Host $testFat.resposta -ForegroundColor Gray
} catch {
    Write-Host " -> [ERRO] Falha ao testar pergunta: $_" -ForegroundColor Red
}

Write-Host "`n======================================================================" -ForegroundColor Cyan
Write-Host "              TESTE CONCLUÍDO COM SUCESSO!                             " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
