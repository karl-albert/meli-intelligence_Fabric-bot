param(
    [switch]$SkipPush = $false
)

$ErrorActionPreference = "Stop"

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "   ATUALIZADOR AUTOMATICO DO JOCA (MERCADO LIVRE -> RENDER NUVEM)    " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Detectar Power BI Desktop
Write-Host "[1/4] Procurando processo do Power BI Desktop aberto..." -ForegroundColor Yellow
$proc = Get-Process -Name msmdsrv -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $proc) {
    Write-Host "[ERRO] O Power BI Desktop com o painel do Mercado Livre não está aberto!" -ForegroundColor Red
    Write-Host "Abra o seu relatório no Power BI Desktop, atualize os dados e execute novamente." -ForegroundColor Yellow
    exit 1
}

$port = (Get-NetTCPConnection -OwningProcess $proc.Id -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1).LocalPort
if (-not $port) {
    Write-Host "[ERRO] Não foi possível identificar a porta do Power BI." -ForegroundColor Red
    exit 1
}
Write-Host " -> Power BI detectado na porta: $port" -ForegroundColor Green

# 2. Conectar e exportar dados via DAX
Write-Host "`n[2/4] Extraindo tabela 'Fato_MercadoLivre_MaisVendidos' do Power BI..." -ForegroundColor Yellow
$baseDir = "c:\000 - Karl\116 - Dashbords\Projetos_Power_BI\B3\Documentos do Projeto"
$csvPath = Join-Path $baseDir "Fato_MercadoLivre_MaisVendidos.csv"

$amo = [System.Reflection.Assembly]::LoadWithPartialName('Microsoft.AnalysisServices.AdomdClient')
$conn = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection("Data Source=localhost:$port;")
$conn.Open()

$cmd = $conn.CreateCommand()
$cmd.CommandText = "EVALUATE Fato_MercadoLivre_MaisVendidos"
$rdr = $cmd.ExecuteReader()

$sw = New-Object System.IO.StreamWriter($csvPath, $false, [System.Text.Encoding]::UTF8)

$fieldCount = $rdr.FieldCount
$headers = @()
for ($i = 0; $i -lt $fieldCount; $i++) {
    $colName = $rdr.GetName($i)
    if ($colName -match '\[(.*)\]') {
        $headers += $matches[1]
    } else {
        $headers += $colName
    }
}
$sw.WriteLine(($headers -join ";"))

$count = 0
$batch = New-Object System.Text.StringBuilder
while ($rdr.Read()) {
    $vals = @()
    for ($i = 0; $i -lt $fieldCount; $i++) {
        $val = $rdr.GetValue($i)
        if ($val -eq [System.DBNull]::Value -or $val -eq $null) {
            $vals += ""
        } elseif ($val -is [System.DateTime]) {
            $vals += $val.ToString("yyyy-MM-dd")
        } else {
            $str = $val.ToString().Replace(";", ",").Replace("`r", " ").Replace("`n", " ")
            $vals += $str
        }
    }
    $batch.AppendLine(($vals -join ";")) | Out-Null
    $count++
    
    if ($count % 25000 -eq 0) {
        $sw.Write($batch.ToString())
        $batch.Clear() | Out-Null
        Write-Host " -> $count registros extraídos..." -ForegroundColor Gray
    }
}

if ($batch.Length -gt 0) {
    $sw.Write($batch.ToString())
    $batch.Clear() | Out-Null
}

$sw.Close()
$rdr.Close()
$conn.Close()

Write-Host " -> Extração concluída: $count registros gravados em CSV!" -ForegroundColor Green

# 3. Converter CSV para Parquet comprimido (DuckDB)
Write-Host "`n[3/4] Compactando para Parquet de alta performance com DuckDB..." -ForegroundColor Yellow

$pyScript = @"
import duckdb, shutil, os

base_dir = r'$baseDir'.replace('\\', '/')
csv_file = f'{base_dir}/Fato_MercadoLivre_MaisVendidos.csv'
parquet_file = f'{base_dir}/Fato_MercadoLivre_MaisVendidos.parquet'
render_parquet = f'{base_dir}/Render_Telegram_Bot/Fato_MercadoLivre_MaisVendidos.parquet'
ml_parquet = r'C:/000 - Karl/116 - Dashbords/Projetos_Power_BI/Mercado_Livre/Fato_MercadoLivre_MaisVendidos.parquet'
ml_render_parquet = r'C:/000 - Karl/116 - Dashbords/Projetos_Power_BI/Mercado_Livre/Render_Telegram_Bot/Fato_MercadoLivre_MaisVendidos.parquet'

con = duckdb.connect()
con.execute(f\"\"\"
    CREATE TABLE fato AS SELECT * FROM read_csv_auto('{csv_file}', delim=';', header=True, ignore_errors=True);
\"\"\")

res = con.execute(\"\"\"
    SELECT MIN(data), MAX(data), COUNT(*) FROM fato
\"\"\").fetchone()

print(f'DATA_MAX:{res[1]}')
print(f'TOTAL_RECS:{res[2]}')

con.execute(f\"\"\"
    COPY fato TO '{parquet_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
\"\"\")

shutil.copyfile(parquet_file, render_parquet)
if os.path.exists(os.path.dirname(ml_parquet)):
    shutil.copyfile(parquet_file, ml_parquet)
if os.path.exists(os.path.dirname(ml_render_parquet)):
    shutil.copyfile(parquet_file, ml_render_parquet)
"@

$pyOut = python -c $pyScript 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERRO CRÍTICO] Falha ao converter CSV para Parquet no DuckDB:" -ForegroundColor Red
    $pyOut | ForEach-Object { Write-Host $_ -ForegroundColor Red }
    exit 1
}

$maxDate = ""
$totalRecs = ""
foreach ($line in $pyOut) {
    if ($line -match "DATA_MAX:(.*)") { $maxDate = $matches[1] }
    if ($line -match "TOTAL_RECS:(.*)") { $totalRecs = $matches[1] }
}

Write-Host " -> Parquet gerado com sucesso!" -ForegroundColor Green
Write-Host " -> Data mais recente na base: $maxDate" -ForegroundColor Cyan
Write-Host " -> Total de registros: $totalRecs" -ForegroundColor Cyan

# 4. Enviar atualização instantânea para a API do Joca (Render)
Write-Host "`n[4/4] Enviando nova base em tempo real para o Joca Bot no Render..." -ForegroundColor Yellow
$parquetPath = Join-Path $baseDir "Fato_MercadoLivre_MaisVendidos.parquet"
$syncScript = @"
import requests, os

parquet_file = r'$parquetPath'
url_sync = 'https://meli-intelligence-bot.onrender.com/sync_data?secret=meli_joca_sync_2026_karl'

try:
    with open(parquet_file, 'rb') as f:
        r = requests.post(url_sync, files={'file': f}, timeout=60)
    if r.status_code == 200:
        data = r.json()
        print(f'SUCESSO_SYNC: Base do Joca atualizada instantaneamente! {data.get(\"total_registros\")} registros carregados. Data mais recente: {data.get(\"data_recente\")}')
    else:
        print(f'AVISO_SYNC: API retornou {r.status_code} - {r.text}')
except Exception as e:
    print(f'AVISO_SYNC: Conexao com API: {e}')
"@
python -c $syncScript

# 5. Salvar cópia no repositório Git como backup histórico
if (-not $SkipPush) {
    Write-Host "`n[5/5] Sincronizando repositório Git de backup..." -ForegroundColor Yellow
    $renderDir = Join-Path $baseDir "Render_Telegram_Bot"
    Push-Location $renderDir
    try {
        git add Fato_MercadoLivre_MaisVendidos.parquet
        $commitMsg = "Atualizacao automatica da base ate $maxDate ($totalRecs registros)"
        git commit -m $commitMsg
        git push origin main
        Write-Host " -> Repositório Git sincronizado com sucesso!" -ForegroundColor Green
    } catch {
        Write-Host "[AVISO] Git commit/push retornou: $_" -ForegroundColor Yellow
    } finally {
        Pop-Location
    }
}

Write-Host "`n======================================================================" -ForegroundColor Green
Write-Host " [SUCESSO TOTAL] O Joca já está com os dados mais recentes na nuvem!" -ForegroundColor Green
Write-Host " -> Data máxima: $maxDate" -ForegroundColor Green
Write-Host " -> Total de registros: $totalRecs" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green

