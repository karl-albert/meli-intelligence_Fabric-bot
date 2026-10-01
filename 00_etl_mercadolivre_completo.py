# =============================================================================
# PIPELINE COMPLETO DE DADOS — MERCADO LIVRE (MICROSOFT FABRIC)
# Camadas: BRONZE -> PRATA -> OURO (Execução End-to-End em 1 Única Sessão Spark)
# Autor: Karl Albert
# Ambiente: Microsoft Fabric (Workspace KAC / Lakehouse LH_MercadoLivre)
# Vantagem: Zero erro de concorrência Spark (HTTP 430), 10x mais rápido!
# =============================================================================

import os
import sys
import urllib.request
from pyspark.sql import functions as F, Window
from pyspark.sql.types import TimestampType

print("=" * 80)
print("  🚀 INICIANDO ETL END-TO-END DO MERCADO LIVRE NO MICROSOFT FABRIC")
print("  Fluxo: Download -> Bronze -> Prata (Limpeza) -> Ouro (Dimensional)")
print("=" * 80)

# =============================================================================
# 1. ETAPA BRONZE — INGESTÃO RAW
# =============================================================================
print("\n" + "=" * 50)
print("  [1/3] EXECUTANDO CAMADA BRONZE")
print("=" * 50)

TABELA_BRONZE = "bronze_mercadolivre_mais_vendidos"
PARQUET_PATH = "Files/Fato_MercadoLivre_MaisVendidos.parquet"
LOCAL_ONELAKE_PATH = "/lakehouse/default/Files/Fato_MercadoLivre_MaisVendidos.parquet"
GITHUB_RAW_URL = "https://raw.githubusercontent.com/karl-albert/meli-intelligence-bot/main/Fato_MercadoLivre_MaisVendidos.parquet"

origem_dados = "OneLake_Local_Files"
try:
    os.makedirs("/lakehouse/default/Files", exist_ok=True)
    print("[*] Baixando base mais recente diretamente do GitHub...")
    urllib.request.urlretrieve(GITHUB_RAW_URL, LOCAL_ONELAKE_PATH)
    tamanho_mb = os.path.getsize(LOCAL_ONELAKE_PATH) / (1024 * 1024)
    print(f"✅ Download concluído! Arquivo: {tamanho_mb:.2f} MB")
    origem_dados = "GitHub_Cloud_Sync"
except Exception as e_down:
    print(f"⚠️ Aviso no download ({e_down}), usando arquivo existente...")

try:
    df_raw = spark.read.parquet(PARQUET_PATH)
    print(f"✅ Registros lidos do Parquet: {df_raw.count():,}")
except Exception as e_read:
    if spark.catalog.tableExists(TABELA_BRONZE):
        df_raw = spark.table(TABELA_BRONZE)
        origem_dados = "Delta_Snapshot"
    else:
        raise Exception(f"Erro crítico na leitura Bronze: {e_read}")

COLUNAS_BIGQUERY = [
    "data", "ano", "mes", "ano_mes", "posicao_ranking", "categoria", "subcategoria",
    "id_anuncio", "titulo_produto", "marca", "preco_atual", "preco_original",
    "desconto_pct", "parcelamento", "qtd_vendas_estimadas_dia", "faturamento_estimado_dia",
    "avaliacao_nota", "qtd_avaliacoes", "is_full", "frete_gratis", "loja_oficial",
    "reputacao_vendedor", "url_imagem", "url_produto", "tipo_dado"
]

cols_presentes = [c for c in COLUNAS_BIGQUERY if c in df_raw.columns]
df_bronze = df_raw.select(cols_presentes) \
    .withColumn("_data_ingestao", F.current_timestamp()) \
    .withColumn("_arquivo_origem", F.lit(origem_dados))

df_bronze.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABELA_BRONZE)

print(f"✅ Camada Bronze concluída: Tabela '{TABELA_BRONZE}' salva com sucesso!")

# =============================================================================
# 2. ETAPA PRATA — LIMPEZA, TIPAGEM E PADRONIZAÇÃO
# =============================================================================
print("\n" + "=" * 50)
print("  [2/3] EXECUTANDO CAMADA PRATA")
print("=" * 50)

TABELA_PRATA = "prata_mercadolivre_mais_vendidos"
df = spark.table(TABELA_BRONZE)
print(f"[*] Registros lidos da Bronze: {df.count():,}")

def converter_monetario_br(col_name):
    return F.regexp_replace(
        F.regexp_replace(F.trim(F.col(col_name)), r'\.', ''),
        r',', '.'
    ).cast("double")

# Tratamento monetário, avaliação e booleanos
df = df \
    .withColumn("preco_atual", converter_monetario_br("preco_atual")) \
    .withColumn("preco_original", converter_monetario_br("preco_original")) \
    .withColumn("faturamento_estimado_dia", converter_monetario_br("faturamento_estimado_dia")) \
    .withColumn("avaliacao_nota", F.regexp_replace(F.trim(F.col("avaliacao_nota")), r',', '.').cast("double")) \
    .withColumn("is_full", F.col("is_full").cast("boolean")) \
    .withColumn("frete_gratis", F.col("frete_gratis").cast("boolean"))

# Tipagem numérica
df = df \
    .withColumn("ano", F.col("ano").cast("int")) \
    .withColumn("mes", F.col("mes").cast("int")) \
    .withColumn("posicao_ranking", F.col("posicao_ranking").cast("int")) \
    .withColumn("desconto_pct", F.col("desconto_pct").cast("int")) \
    .withColumn("qtd_vendas_estimadas_dia", F.col("qtd_vendas_estimadas_dia").cast("int")) \
    .withColumn("qtd_avaliacoes", F.col("qtd_avaliacoes").cast("int"))

# Trim em campos de texto
campos_texto = [
    "ano_mes", "categoria", "subcategoria", "id_anuncio",
    "titulo_produto", "marca", "parcelamento",
    "loja_oficial", "reputacao_vendedor",
    "url_imagem", "url_produto", "tipo_dado"
]
for campo in campos_texto:
    df = df.withColumn(campo, F.trim(F.col(campo)))

# Deduplicação por (data, id_anuncio)
window_dedup = Window.partitionBy("data", "id_anuncio").orderBy("posicao_ranking")
df = df \
    .withColumn("_rn", F.row_number().over(window_dedup)) \
    .filter(F.col("_rn") == 1) \
    .drop("_rn")

# Tratamento de nulos
df = df \
    .withColumn("categoria", F.coalesce(F.col("categoria"), F.lit("Sem Categoria"))) \
    .withColumn("subcategoria", F.coalesce(F.col("subcategoria"), F.lit("Sem Subcategoria"))) \
    .withColumn("marca", F.coalesce(F.col("marca"), F.lit("Sem Marca"))) \
    .withColumn("qtd_vendas_estimadas_dia", F.coalesce(F.col("qtd_vendas_estimadas_dia"), F.lit(0))) \
    .withColumn("faturamento_estimado_dia", F.coalesce(F.col("faturamento_estimado_dia"), F.lit(0.0))) \
    .withColumn("preco_atual", F.coalesce(F.col("preco_atual"), F.lit(0.0)))

df_prata = df.drop("_data_ingestao", "_arquivo_origem") \
    .withColumn("_data_processamento", F.current_timestamp())

df_prata.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABELA_PRATA)

print(f"✅ Camada Prata concluída: Tabela '{TABELA_PRATA}' salva com sucesso!")

# =============================================================================
# 3. ETAPA OURO — MODELAGEM DIMENSIONAL (STAR SCHEMA DIRECT LAKE)
# =============================================================================
print("\n" + "=" * 50)
print("  [3/3] EXECUTANDO CAMADA OURO")
print("=" * 50)

# 3.1 Dimensão Calendário
df_datas = df_prata.select("data").distinct()

meses_pt = {
    1: "Janeiro", 2: "Fevereiro", 3: "Marco", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}
dias_semana_pt = {
    1: "Domingo", 2: "Segunda", 3: "Terca",
    4: "Quarta", 5: "Quinta", 6: "Sexta", 7: "Sabado"
}
mes_case = F.create_map([F.lit(x) for item in meses_pt.items() for x in item])
dia_case = F.create_map([F.lit(x) for item in dias_semana_pt.items() for x in item])

df_dim_calendario = df_datas \
    .withColumn("ano", F.year("data")) \
    .withColumn("mes", F.month("data")) \
    .withColumn("dia", F.dayofmonth("data")) \
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM")) \
    .withColumn("nome_mes", mes_case[F.month("data")]) \
    .withColumn("trimestre", F.quarter("data")) \
    .withColumn("dia_semana", dia_case[F.dayofweek("data")]) \
    .withColumn("dia_semana_num", F.dayofweek("data")) \
    .withColumn("is_fim_semana", F.when(F.dayofweek("data").isin(1, 7), True).otherwise(False)) \
    .withColumn("semana_do_ano", F.weekofyear("data")) \
    .orderBy("data")

df_dim_calendario.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ouro_dim_calendario")
print(f"  -> Dimensão 'ouro_dim_calendario' gerada.")

# 3.2 Dimensão Categoria
df_dim_categoria = df_prata \
    .select("categoria", "subcategoria") \
    .distinct() \
    .withColumn("sk_categoria", F.monotonically_increasing_id() + 1) \
    .select("sk_categoria", "categoria", "subcategoria") \
    .orderBy("categoria", "subcategoria")

df_dim_categoria.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ouro_dim_categoria")
print(f"  -> Dimensão 'ouro_dim_categoria' gerada.")

# 3.3 Dimensão Produto
window_produto = Window.partitionBy("id_anuncio").orderBy(F.desc("data"))
df_dim_produto = df_prata \
    .withColumn("_rn", F.row_number().over(window_produto)) \
    .filter(F.col("_rn") == 1) \
    .select(
        "id_anuncio", "titulo_produto", "marca", "avaliacao_nota", "qtd_avaliacoes",
        "is_full", "frete_gratis", "loja_oficial", "reputacao_vendedor", "url_imagem", "url_produto"
    ) \
    .withColumn("sk_produto", F.monotonically_increasing_id() + 1) \
    .select("sk_produto", "id_anuncio", "titulo_produto", "marca",
            "avaliacao_nota", "qtd_avaliacoes", "is_full", "frete_gratis",
            "loja_oficial", "reputacao_vendedor", "url_imagem", "url_produto")

df_dim_produto.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ouro_dim_produto")
print(f"  -> Dimensão 'ouro_dim_produto' gerada.")

# 3.4 Tabela Fato Vendas
dim_cat = spark.table("ouro_dim_categoria").select("sk_categoria", "categoria", "subcategoria")
dim_prod = spark.table("ouro_dim_produto").select("sk_produto", "id_anuncio")

df_fato = df_prata \
    .join(dim_cat, on=["categoria", "subcategoria"], how="left") \
    .join(dim_prod, on=["id_anuncio"], how="left") \
    .select(
        "data", "sk_produto", "sk_categoria", "posicao_ranking",
        "qtd_vendas_estimadas_dia", "faturamento_estimado_dia",
        "preco_atual", "preco_original", "desconto_pct", "parcelamento",
        "tipo_dado", "ano_mes"
    ) \
    .withColumnRenamed("qtd_vendas_estimadas_dia", "qtd_vendas") \
    .withColumnRenamed("faturamento_estimado_dia", "faturamento")

df_fato.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ouro_fato_vendas")
print(f"  -> Tabela Fato 'ouro_fato_vendas' gerada.")

# =============================================================================
# 4. AUDITORIA FINAL E SUCESSO
# =============================================================================
totais_ouro = spark.table("ouro_fato_vendas").agg(
    F.count("*").alias("registros"),
    F.sum("faturamento").alias("faturamento"),
    F.sum("qtd_vendas").alias("vendas"),
    F.min("data").alias("data_min"),
    F.max("data").alias("data_max")
).collect()[0]

print("\n" + "=" * 80)
print("  🎉 PIPELINE EXECUTADO COM SUCESSO ABSOLUTO!")
print("=" * 80)
print(f"  Total de Registros na Fato : {totais_ouro['registros']:,}")
print(f"  Período Coberto            : {totais_ouro['data_min']} até {totais_ouro['data_max']}")
print(f"  Faturamento Consolidado    : R$ {totais_ouro['faturamento']:,.2f}")
print(f"  Vendas Totais              : {totais_ouro['vendas']:,}")
print("  Status Direct Lake         : PRONTO PARA ATUALIZAR O SM_MERCADOLIVRE!")
print("=" * 80)
