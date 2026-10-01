# =============================================================================
# NOTEBOOK 01 – CAMADA BRONZE: Ingestao Raw (Zero-Touch Automatica)
# Projeto: Mercado Livre – Mais Vendidos (Microsoft Fabric)
# Autor: Karl Albert
# Data: 29/09/2026 (Atualizado: Sincronizacao 100% Automatica via Nuvem)
# =============================================================================
# INSTRUCAO: Copie este codigo para o Notebook no Fabric chamado
#            "01_bronze_ingestao" e vincule ao Lakehouse LH_MercadoLivre.
# =============================================================================

# %% [markdown]
# # 📥 Camada BRONZE – Ingestao dos Dados Brutos (Autonomia Total)
# 
# **Objetivo:** Ingerir a base mais recente do Mercado Livre e gravar
# na tabela Delta `bronze_mercadolivre_mais_vendidos` no Lakehouse.
# 
# **Destaque:** Baixa automaticamente a base atualizada da nuvem (GitHub/BigQuery),
# gravando no OneLake sem necessidade de upload manual!

# %% Celula 1 – Configuracao e Imports
import os
import sys
import urllib.request
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

print("=" * 75)
print("  NOTEBOOK 01 – BRONZE: Ingestao Automatizada do Mercado Livre")
print("  Lakehouse: LH_MercadoLivre | Workspace: KAC")
print("=" * 75)

PARQUET_PATH = "Files/Fato_MercadoLivre_MaisVendidos.parquet"
LOCAL_ONELAKE_PATH = "/lakehouse/default/Files/Fato_MercadoLivre_MaisVendidos.parquet"
GITHUB_RAW_URL = "https://raw.githubusercontent.com/karl-albert/meli-intelligence-bot/main/Fato_MercadoLivre_MaisVendidos.parquet"
TABELA_BRONZE = "bronze_mercadolivre_mais_vendidos"

# Colunas oficiais alinhadas com o BigQuery
COLUNAS_BIGQUERY = [
    "data",
    "ano",
    "mes",
    "ano_mes",
    "posicao_ranking",
    "categoria",
    "subcategoria",
    "id_anuncio",
    "titulo_produto",
    "marca",
    "preco_atual",
    "preco_original",
    "desconto_pct",
    "parcelamento",
    "qtd_vendas_estimadas_dia",
    "faturamento_estimado_dia",
    "avaliacao_nota",
    "qtd_avaliacoes",
    "is_full",
    "frete_gratis",
    "loja_oficial",
    "reputacao_vendedor",
    "url_imagem",
    "url_produto",
    "tipo_dado"
]

# %% Celula 2 – Sincronizacao em Nuvem e Leitura dos Dados
print("[*] Verificando sincronizacao automatica com a nuvem...")
try:
    # Garante que a pasta Files existe
    os.makedirs("/lakehouse/default/Files", exist_ok=True)
    print("[*] Baixando versao mais recente direto do repositorio GitHub...")
    urllib.request.urlretrieve(GITHUB_RAW_URL, LOCAL_ONELAKE_PATH)
    tamanho_mb = os.path.getsize(LOCAL_ONELAKE_PATH) / (1024 * 1024)
    print(f"[OK] Base sincronizada no OneLake com sucesso! ({tamanho_mb:.2f} MB)")
    origem_dados = "GitHub_Cloud_Sync"
except Exception as e_down:
    print(f"[Aviso] Nao foi possivel baixar da nuvem ({e_down}). Tentando ler arquivo existente...")
    origem_dados = "OneLake_Local_Files"

try:
    df_raw = spark.read.parquet(PARQUET_PATH)
    total_lido = df_raw.count()
    print(f"[OK] Sucesso ao ler Parquet: {total_lido:,} registros carregados.")
except Exception as e:
    print(f"[Aviso] Falha ao ler Parquet ({e}). Verificando fallback Delta...")
    if spark.catalog.tableExists(TABELA_BRONZE):
        print(f"[*] Utilizando tabela Delta existente '{TABELA_BRONZE}' como base.")
        df_raw = spark.table(TABELA_BRONZE)
        origem_dados = "Delta_Snapshot"
    else:
        raise Exception("Nenhum dado encontrado para carga Bronze no Lakehouse.")

# %% Celula 3 – Selecionar Colunas e Adicionar Metadados de Auditoria
cols_presentes = [c for c in COLUNAS_BIGQUERY if c in df_raw.columns]
df_bronze = df_raw.select(cols_presentes)

df_bronze = df_bronze \
    .withColumn("_data_ingestao", F.current_timestamp()) \
    .withColumn("_arquivo_origem", F.lit(origem_dados))

# %% Celula 4 – Gravacao na Tabela Delta Bronze (Modo Idempotente)
print(f"\n[*] Gravando tabela Delta '{TABELA_BRONZE}' no Lakehouse...")

df_bronze.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABELA_BRONZE)

print(f"[OK] Tabela Delta '{TABELA_BRONZE}' atualizada com sucesso!")

# %% Celula 5 – Validacao e Auditoria Pos-Carga
df_check = spark.table(TABELA_BRONZE)
total_final = df_check.count()
datas = df_check.agg(
    F.min("data").alias("data_min"),
    F.max("data").alias("data_max")
).collect()[0]

print("\n" + "=" * 75)
print("  AUDITORIA DE INGESTAO (CAMADA BRONZE)")
print("=" * 75)
print(f"  Total de Registros : {total_final:,}")
print(f"  Periodo Coberto    : {datas['data_min']} ate {datas['data_max']}")
print(f"  Status             : PRONTO PARA CAMADA PRATA (02_prata_limpeza)")
print("=" * 75)
