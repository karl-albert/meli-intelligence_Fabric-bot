# =============================================================================
# NOTEBOOK 01 — CAMADA BRONZE: Ingestão Raw
# Projeto: Mercado Livre — Mais Vendidos (Microsoft Fabric)
# Autor: Karl Albert
# Data: 27/09/2026
# =============================================================================
# INSTRUÇÃO: Copie este código para um Notebook no Fabric chamado
#            "01_bronze_ingestao" e vincule ao Lakehouse LH_MercadoLivre.
# =============================================================================

# %% [markdown]
# # 🟫 Camada BRONZE — Ingestão dos Dados Brutos
# 
# **Objetivo:** Ler o arquivo Parquet da pasta `Files/` do Lakehouse e salvar
# como tabela Delta sem nenhuma transformação nos dados originais.
# 
# **Fonte:** `Files/Fato_MercadoLivre_MaisVendidos.parquet`  
# **Destino:** Tabela Delta `bronze_mercadolivre_mais_vendidos`

# %% Célula 1 — Configuração e leitura do Parquet
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

print("=" * 70)
print("  NOTEBOOK 01 — BRONZE: Ingestão Raw do Mercado Livre")
print("=" * 70)

# Caminho do arquivo Parquet na pasta Files do Lakehouse
# No Fabric, o Lakehouse padrão é montado automaticamente em:
#   Files/ -> abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/
PARQUET_PATH = "Files/Fato_MercadoLivre_MaisVendidos.parquet"

# Ler o Parquet bruto
df_raw = spark.read.parquet(PARQUET_PATH)

print(f"Registros lidos do Parquet: {df_raw.count():,}")
print(f"Colunas no arquivo: {len(df_raw.columns)}")
print(f"Colunas: {df_raw.columns}")

# %% Célula 2 — Selecionar as 25 colunas (alinhadas com BigQuery)
# A coluna "produto" existe só no Parquet (é titulo_produto truncado)
# e NÃO existe no BigQuery, então a ignoramos.

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

# Selecionar apenas as 25 colunas do BigQuery
df_bronze = df_raw.select(COLUNAS_BIGQUERY)

# %% Célula 3 — Adicionar colunas de controle de ingestão
df_bronze = df_bronze \
    .withColumn("_data_ingestao", F.current_timestamp()) \
    .withColumn("_arquivo_origem", F.lit("Fato_MercadoLivre_MaisVendidos.parquet"))

print(f"\nColunas na Bronze (25 dados + 2 controle): {len(df_bronze.columns)}")
print("Schema da tabela Bronze:")
df_bronze.printSchema()

# %% Célula 4 — Salvar como tabela Delta (overwrite na carga inicial)
TABELA_BRONZE = "bronze_mercadolivre_mais_vendidos"

df_bronze.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(TABELA_BRONZE)

print(f"\nTabela '{TABELA_BRONZE}' salva com sucesso!")

# %% Célula 5 — Validação pós-ingestão
print("\n" + "=" * 70)
print("  VALIDACAO POS-INGESTAO")
print("=" * 70)

df_check = spark.table(TABELA_BRONZE)

total = df_check.count()
print(f"\nTotal de registros na Bronze: {total:,}")

# Período de datas
datas = df_check.agg(
    F.min("data").alias("data_min"),
    F.max("data").alias("data_max")
).collect()[0]
print(f"Periodo: {datas['data_min']} ate {datas['data_max']}")

# Distribuição por categoria
print("\nRegistros por categoria:")
df_check.groupBy("categoria") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(truncate=False)

# Verificação de integridade
print(f"Numero de colunas: {len(df_check.columns)}")
print(f"Colunas: {df_check.columns}")

# Amostra
print("\nAmostra de 3 registros:")
df_check.show(3, truncate=30)

print("\nBRONZE CONCLUIDA COM SUCESSO!")
