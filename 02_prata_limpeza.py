# =============================================================================
# NOTEBOOK 02 — CAMADA PRATA: Limpeza e Tipagem
# Projeto: Mercado Livre — Mais Vendidos (Microsoft Fabric)
# Autor: Karl Albert
# Data: 27/09/2026
# =============================================================================
# INSTRUÇÃO: Copie este código para um Notebook no Fabric chamado
#            "02_prata_limpeza" e vincule ao Lakehouse LH_MercadoLivre.
#            Execute APÓS o notebook 01_bronze_ingestao.
# =============================================================================

# %% [markdown]
# # ⬜ Camada PRATA — Dados Limpos e Tipados
# 
# **Objetivo:** Aplicar limpeza, tipagem e padronização nos dados brutos
# da Bronze, alinhando com as mesmas regras do BigQuery.
# 
# **Fonte:** Tabela Delta `bronze_mercadolivre_mais_vendidos`  
# **Destino:** Tabela Delta `prata_mercadolivre_mais_vendidos`

# %% Célula 1 — Leitura da Bronze
from pyspark.sql import functions as F
from pyspark.sql import Window

print("=" * 70)
print("  NOTEBOOK 02 — PRATA: Limpeza e Tipagem do Mercado Livre")
print("=" * 70)

TABELA_BRONZE = "bronze_mercadolivre_mais_vendidos"
df = spark.table(TABELA_BRONZE)

total_bronze = df.count()
print(f"\nRegistros lidos da Bronze: {total_bronze:,}")

# %% Célula 2 — Conversão monetária BR (formato "1.500,00" → 1500.00)
# No Parquet, preco_atual, preco_original e faturamento_estimado_dia
# estão como VARCHAR com formato brasileiro (ponto=milhar, virgula=decimal)

def converter_monetario_br(col_name):
    """Converte string monetária BR para DOUBLE.
    Exemplos: '29,9' → 29.9 | '1.500,00' → 1500.0 | '2093' → 2093.0
    """
    return F.regexp_replace(
        F.regexp_replace(F.trim(F.col(col_name)), r'\.', ''),  # Remove ponto de milhar
        r',', '.'                                                # Troca vírgula por ponto
    ).cast("double")

df = df \
    .withColumn("preco_atual", converter_monetario_br("preco_atual")) \
    .withColumn("preco_original", converter_monetario_br("preco_original")) \
    .withColumn("faturamento_estimado_dia", converter_monetario_br("faturamento_estimado_dia"))

print("Conversao monetaria BR aplicada: preco_atual, preco_original, faturamento_estimado_dia")

# %% Célula 3 — Conversão da nota de avaliação ("4,8" → 4.8)
df = df.withColumn(
    "avaliacao_nota",
    F.regexp_replace(F.trim(F.col("avaliacao_nota")), r',', '.').cast("double")
)

print("Conversao de avaliacao_nota aplicada")

# %% Célula 4 — Conversão de booleanos (BIGINT 0/1 → BOOLEAN)
# No BigQuery esses campos são BOOL, no Parquet são BIGINT (0 ou 1)
df = df \
    .withColumn("is_full", F.col("is_full").cast("boolean")) \
    .withColumn("frete_gratis", F.col("frete_gratis").cast("boolean"))

print("Conversao de booleanos aplicada: is_full, frete_gratis")

# %% Célula 5 — Tipagem explícita dos campos numéricos
df = df \
    .withColumn("ano", F.col("ano").cast("int")) \
    .withColumn("mes", F.col("mes").cast("int")) \
    .withColumn("posicao_ranking", F.col("posicao_ranking").cast("int")) \
    .withColumn("desconto_pct", F.col("desconto_pct").cast("int")) \
    .withColumn("qtd_vendas_estimadas_dia", F.col("qtd_vendas_estimadas_dia").cast("int")) \
    .withColumn("qtd_avaliacoes", F.col("qtd_avaliacoes").cast("int"))

print("Tipagem numerica aplicada: ano, mes, posicao_ranking, desconto_pct, qtd_vendas, qtd_avaliacoes")

# %% Célula 6 — TRIM em todos os campos texto
campos_texto = [
    "ano_mes", "categoria", "subcategoria", "id_anuncio",
    "titulo_produto", "marca", "parcelamento",
    "loja_oficial", "reputacao_vendedor",
    "url_imagem", "url_produto", "tipo_dado"
]

for campo in campos_texto:
    df = df.withColumn(campo, F.trim(F.col(campo)))

print(f"TRIM aplicado em {len(campos_texto)} campos texto")

# %% Célula 7 — Deduplicação por (data, id_anuncio)
# Mantém o registro com menor posicao_ranking (melhor posição)
total_antes_dedup = df.count()

window_dedup = Window.partitionBy("data", "id_anuncio").orderBy("posicao_ranking")
df = df \
    .withColumn("_rn", F.row_number().over(window_dedup)) \
    .filter(F.col("_rn") == 1) \
    .drop("_rn")

total_apos_dedup = df.count()
duplicatas_removidas = total_antes_dedup - total_apos_dedup

print(f"\nDeduplicacao: {total_antes_dedup:,} → {total_apos_dedup:,} ({duplicatas_removidas:,} duplicatas removidas)")

# %% Célula 8 — Tratamento de nulos em campos críticos
df = df \
    .withColumn("categoria", F.coalesce(F.col("categoria"), F.lit("Sem Categoria"))) \
    .withColumn("subcategoria", F.coalesce(F.col("subcategoria"), F.lit("Sem Subcategoria"))) \
    .withColumn("marca", F.coalesce(F.col("marca"), F.lit("Sem Marca"))) \
    .withColumn("qtd_vendas_estimadas_dia", F.coalesce(F.col("qtd_vendas_estimadas_dia"), F.lit(0))) \
    .withColumn("faturamento_estimado_dia", F.coalesce(F.col("faturamento_estimado_dia"), F.lit(0.0))) \
    .withColumn("preco_atual", F.coalesce(F.col("preco_atual"), F.lit(0.0)))

print("Tratamento de nulos aplicado nos campos criticos")

# %% Célula 9 — Remover colunas de controle da Bronze (não propagam para Prata)
df_prata = df.drop("_data_ingestao", "_arquivo_origem")

# Adicionar colunas de controle da Prata
df_prata = df_prata \
    .withColumn("_data_processamento", F.current_timestamp())

print(f"\nSchema final da Prata ({len(df_prata.columns)} colunas):")
df_prata.printSchema()

# %% Célula 10 — Salvar como tabela Delta
TABELA_PRATA = "prata_mercadolivre_mais_vendidos"

df_prata.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABELA_PRATA)

print(f"\nTabela '{TABELA_PRATA}' salva com sucesso!")

# %% Célula 11 — Validação pós-processamento
print("\n" + "=" * 70)
print("  VALIDACAO POS-PROCESSAMENTO")
print("=" * 70)

df_check = spark.table(TABELA_PRATA)

total = df_check.count()
print(f"\nTotal de registros na Prata: {total:,}")

# Verificar tipos
print("\nTipos de dados:")
for campo in df_check.dtypes:
    print(f"  {campo[0]:35s} {campo[1]}")

# Verificar conversão monetária (amostra)
print("\nAmostra de valores convertidos:")
df_check.select(
    "titulo_produto", "preco_atual", "preco_original",
    "faturamento_estimado_dia", "avaliacao_nota",
    "is_full", "frete_gratis"
).show(5, truncate=30)

# Verificar nulos em campos críticos
print("Contagem de nulos em campos criticos:")
for campo in ["categoria", "subcategoria", "marca", "qtd_vendas_estimadas_dia", "faturamento_estimado_dia"]:
    nulos = df_check.filter(F.col(campo).isNull()).count()
    print(f"  {campo}: {nulos} nulos")

# Totais para validação cruzada com BigQuery
print("\n--- VALORES PARA VALIDACAO CRUZADA COM BIGQUERY ---")
totais = df_check.agg(
    F.count("*").alias("total_registros"),
    F.sum("faturamento_estimado_dia").alias("faturamento_total"),
    F.sum("qtd_vendas_estimadas_dia").alias("vendas_total"),
    F.min("data").alias("data_min"),
    F.max("data").alias("data_max"),
    F.countDistinct("categoria").alias("qtd_categorias")
).collect()[0]

print(f"  Total registros:  {totais['total_registros']:,}")
print(f"  Faturamento total: R$ {totais['faturamento_total']:,.2f}")
print(f"  Vendas total:     {totais['vendas_total']:,}")
print(f"  Periodo:          {totais['data_min']} ate {totais['data_max']}")
print(f"  Categorias:       {totais['qtd_categorias']}")

print("\nPRATA CONCLUIDA COM SUCESSO!")
