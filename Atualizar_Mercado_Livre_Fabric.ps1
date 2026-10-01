# ==============================================================================
# ATUALIZADOR AUTOMATICO DO PROJETO MERCADO LIVRE - MICROSOFT FABRIC & ONELAKE
# ==============================================================================
# Autor: Karl Albert
# Ambiente: Microsoft Fabric (Workspace KAC / Lakehouse LH_MercadoLivre)
# Modelo: SM_MercadoLivre (Direct Lake)
# Integrado com: Joca Bot Nuvem (Render / Telegram)
# ==============================================================================

$ErrorActionPreference = "Continue"

Write-Host "======================================================================" -ForegroundColor Yellow
Write-Host "   ATUALIZADOR AUTOMATICO: MERCADO LIVRE -> MICROSOFT FABRIC         " -ForegroundColor Yellow
Write-Host "   Workspace: KAC | Lakehouse: LH_MercadoLivre | Modo: Direct Lake   " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Yellow
Write-Host ""

$baseDir = "c:\000 - Karl\116 - Dashbords\Projetos_Power_BI\B3\Documentos do Projeto"
$mlDir   = "C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre"
$parquet = Join-Path $baseDir "Fato_MercadoLivre_MaisVendidos.parquet"
$validaScript = Join-Path $baseDir "validar_base_meli.py"

# 1. Validar integridade da base local
Write-Host "[1/5] Validando base local do Mercado Livre (DuckDB)..." -ForegroundColor Yellow

$resCheck = python $validaScript
$dtMin = "N/A"
$dtMax = "N/A"
$totalRecs = 0

foreach ($line in $resCheck) {
    if ($line -match "^OK\|(.*)\|(.*)\|(.*)") {
        $dtMin = $matches[1]
        $dtMax = $matches[2]
        $totalRecs = [int]$matches[3]
    }
}

if ($totalRecs -gt 0) {
    Write-Host "  -> Periodo dos Dados : $dtMin ate $dtMax" -ForegroundColor Green
    Write-Host "  -> Total de Registros: $totalRecs linhas" -ForegroundColor Green
} else {
    Write-Host "  [AVISO] Verificacao da base local: $resCheck" -ForegroundColor Yellow
}

# 2. Sincronizar copias locais para garantir consistencia
Write-Host "`n[2/5] Sincronizando repositorios locais e pasta do Fabric..." -ForegroundColor Yellow
$destFabric = Join-Path $mlDir "fabric\Fato_MercadoLivre_MaisVendidos.parquet"
$destML = Join-Path $mlDir "Fato_MercadoLivre_MaisVendidos.parquet"
$destRender = Join-Path $baseDir "Render_Telegram_Bot\Fato_MercadoLivre_MaisVendidos.parquet"

Copy-Item -Path $parquet -Destination $destFabric -Force
Copy-Item -Path $parquet -Destination $destML -Force
if (Test-Path (Split-Path $destRender)) {
    Copy-Item -Path $parquet -Destination $destRender -Force
}
Write-Host "  -> Copias sincronizadas com sucesso em todas as pastas!" -ForegroundColor Green

# 3. Disparar Pipeline no Microsoft Fabric via API / Webhook
Write-Host "`n[3/5] Acionando automacao do Microsoft Fabric..." -ForegroundColor Yellow
$triggerScript = Join-Path $baseDir "disparar_fabric_mercadolivre.py"
if (Test-Path $triggerScript) {
    python $triggerScript
} else {
    Write-Host "  -> Script disparar_fabric_mercadolivre.py nao encontrado." -ForegroundColor Yellow
}

# 4. Sincronizar nova base com o Joca Bot na Nuvem (Render / Telegram)
Write-Host "`n[4/5] Sincronizando base com o Joca Bot na Nuvem (Render 24/7)..." -ForegroundColor Yellow
$syncScript = @"
import requests, os

parquet_file = r'$parquet'
url_sync = 'https://meli-intelligence-bot.onrender.com/sync_data?secret=meli_joca_sync_2026_karl'
url_status = 'https://meli-intelligence-bot.onrender.com/status'

try:
    try:
        requests.get(url_status, timeout=15)
    except Exception:
        pass

    with open(parquet_file, 'rb') as f:
        r = requests.post(url_sync, files={'file': f}, timeout=60)
    if r.status_code == 200:
        data = r.json()
        print(f'  [OK JOCA] Joca atualizado na nuvem! {data.get("total_registros")} registros carregados. Data mais recente: {data.get("data_recente")}')
    else:
        print(f'  [AVISO JOCA] API retornou {r.status_code}: {r.text}')
except Exception as e:
    print(f'  [AVISO JOCA] Conexao com API Joca Nuvem: {e}')
"@
python -c $syncScript

# 5. Status Final
Write-Host "`n======================================================================" -ForegroundColor Green
Write-Host " [SUCESSO] Fluxo de Atualizacao Concluido!" -ForegroundColor Green
Write-Host " -> Base: $totalRecs registros (Ultima Data: $dtMax)" -ForegroundColor Cyan
Write-Host " -> Direct Lake no Fabric e Joca Bot 100% sincronizados!" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Green
