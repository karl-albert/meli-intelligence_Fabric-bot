# =============================================================================
# NOTEBOOK 03 — CAMADA OURO: Modelo Dimensional (Star Schema)
# Projeto: Mercado Livre — Mais Vendidos (Microsoft Fabric)
# Autor: Karl Albert
# Data: 27/09/2026
# =============================================================================
# INSTRUÇÃO: Copie este código para um Notebook no Fabric chamado
#            "03_ouro_modelo_dimensional" e vincule ao Lakehouse LH_MercadoLivre.
#            Execute APÓS o notebook 02_prata_limpeza.
# =============================================================================

# %% [markdown]
# # 🟡 Camada OURO — Modelo Dimensional Star Schema
# 
# **Objetivo:** Criar modelo dimensional otimizado para Direct Lake no Power BI.
# 
# **Fonte:** Tabela Delta `prata_mercadolivre_mais_vendidos`  
# **Destino:** 4 tabelas Delta:
# - `ouro_fato_vendas` (Fato)
# - `ouro_dim_calendario` (Dimensão)
# - `ouro_dim_categoria` (Dimensão)
# - `ouro_dim_produto` (Dimensão)

# %% Célula 1 — Leitura da Prata
from pyspark.sql import functions as F
from pyspark.sql import Window

print("=" * 70)
print("  NOTEBOOK 03 — OURO: Modelo Dimensional Star Schema")
print("=" * 70)

TABELA_PRATA = "prata_mercadolivre_mais_vendidos"
df_prata = spark.table(TABELA_PRATA)

total_prata = df_prata.count()
print(f"\nRegistros lidos da Prata: {total_prata:,}")

# =====================================================================
# DIMENSÃO 1: CALENDÁRIO
# =====================================================================

# %% Célula 2 — Criar Dimensão Calendário
print("\n--- Criando ouro_dim_calendario ---")

# Extrair todas as datas únicas
df_datas = df_prata.select("data").distinct()

# Nomes dos meses em português
meses_pt = {
    1: "Janeiro", 2: "Fevereiro", 3: "Marco", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

# Nomes dos dias da semana em português (1=Domingo no Spark)
dias_semana_pt = {
    1: "Domingo", 2: "Segunda", 3: "Terca",
    4: "Quarta", 5: "Quinta", 6: "Sexta", 7: "Sabado"
}

# Criar mapeamento como expressão CASE WHEN
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
    .withColumn("is_fim_semana", 
        F.when(F.dayofweek("data").isin(1, 7), True).otherwise(False)) \
    .withColumn("semana_do_ano", F.weekofyear("data")) \
    .orderBy("data")

# Salvar
df_dim_calendario.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ouro_dim_calendario")

total_cal = df_dim_calendario.count()
print(f"  ouro_dim_calendario: {total_cal:,} datas unicas")

# =====================================================================
# DIMENSÃO 2: CATEGORIA
# =====================================================================

# %% Célula 3 — Criar Dimensão Categoria
print("\n--- Criando ouro_dim_categoria ---")

df_dim_categoria = df_prata \
    .select("categoria", "subcategoria") \
    .distinct() \
    .withColumn("sk_categoria", F.monotonically_increasing_id() + 1) \
    .select("sk_categoria", "categoria", "subcategoria") \
    .orderBy("categoria", "subcategoria")

# Salvar
df_dim_categoria.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ouro_dim_categoria")

total_cat = df_dim_categoria.count()
print(f"  ouro_dim_categoria: {total_cat:,} pares categoria/subcategoria unicos")
df_dim_categoria.show(10, truncate=False)

# =====================================================================
# DIMENSÃO 3: PRODUTO
# =====================================================================

# %% Célula 4 — Criar Dimensão Produto
print("\n--- Criando ouro_dim_produto ---")

# Para cada id_anuncio, pegar a versão mais recente (dados mais atualizados)
window_produto = Window.partitionBy("id_anuncio").orderBy(F.desc("data"))

df_dim_produto = df_prata \
    .withColumn("_rn", F.row_number().over(window_produto)) \
    .filter(F.col("_rn") == 1) \
    .select(
        "id_anuncio",
        "titulo_produto",
        "marca",
        "avaliacao_nota",
        "qtd_avaliacoes",
        "is_full",
        "frete_gratis",
        "loja_oficial",
        "reputacao_vendedor",
        "url_imagem",
        "url_produto"
    ) \
    .withColumn("sk_produto", F.monotonically_increasing_id() + 1) \
    .select("sk_produto", "id_anuncio", "titulo_produto", "marca",
            "avaliacao_nota", "qtd_avaliacoes", "is_full", "frete_gratis",
            "loja_oficial", "reputacao_vendedor", "url_imagem", "url_produto")

# Salvar
df_dim_produto.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ouro_dim_produto")

total_prod = df_dim_produto.count()
print(f"  ouro_dim_produto: {total_prod:,} produtos unicos (por id_anuncio)")

# =====================================================================
# TABELA FATO: VENDAS
# =====================================================================

# %% Célula 5 — Criar Tabela Fato Vendas
print("\n--- Criando ouro_fato_vendas ---")

# Carregar dimensões para fazer o JOIN e obter as surrogate keys
dim_cat = spark.table("ouro_dim_categoria").select("sk_categoria", "categoria", "subcategoria")
dim_prod = spark.table("ouro_dim_produto").select("sk_produto", "id_anuncio")

# Fazer JOIN da Prata com as dimensões para obter as SKs
df_fato = df_prata \
    .join(dim_cat, on=["categoria", "subcategoria"], how="left") \
    .join(dim_prod, on=["id_anuncio"], how="left") \
    .select(
        # Chaves
        "data",
        "sk_produto",
        "sk_categoria",
        # Métricas da venda
        "posicao_ranking",
        "qtd_vendas_estimadas_dia",
        "faturamento_estimado_dia",
        "preco_atual",
        "preco_original",
        "desconto_pct",
        "parcelamento",
        # Campos de contexto
        "tipo_dado",
        "ano_mes"
    ) \
    .withColumnRenamed("qtd_vendas_estimadas_dia", "qtd_vendas") \
    .withColumnRenamed("faturamento_estimado_dia", "faturamento")

# Salvar
df_fato.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ouro_fato_vendas")

total_fato = df_fato.count()
print(f"  ouro_fato_vendas: {total_fato:,} registros")

# =====================================================================
# VALIDAÇÃO CRUZADA
# =====================================================================

# %% Célula 6 — Validação Cruzada (Prata vs Ouro)
print("\n" + "=" * 70)
print("  VALIDACAO CRUZADA: PRATA vs OURO")
print("=" * 70)

# Totais da Prata
totais_prata = df_prata.agg(
    F.count("*").alias("registros"),
    F.sum("faturamento_estimado_dia").alias("faturamento"),
    F.sum("qtd_vendas_estimadas_dia").alias("vendas")
).collect()[0]

# Totais da Fato (Ouro)
df_fato_check = spark.table("ouro_fato_vendas")
totais_ouro = df_fato_check.agg(
    F.count("*").alias("registros"),
    F.sum("faturamento").alias("faturamento"),
    F.sum("qtd_vendas").alias("vendas")
).collect()[0]

print(f"\n{'Metrica':<25s} {'Prata':>20s} {'Ouro (Fato)':>20s} {'Match':>8s}")
print("-" * 75)

checks = [
    ("Registros", totais_prata['registros'], totais_ouro['registros']),
    ("Faturamento Total", totais_prata['faturamento'], totais_ouro['faturamento']),
    ("Vendas Total", totais_prata['vendas'], totais_ouro['vendas']),
]

all_ok = True
for nome, val_p, val_o in checks:
    match = "OK" if val_p == val_o else "ERRO!"
    if match == "ERRO!":
        all_ok = False
    print(f"  {nome:<25s} {str(val_p):>20s} {str(val_o):>20s} {match:>8s}")

# =====================================================================
# RESUMO FINAL
# =====================================================================

# %% Célula 7 — Resumo Final
print("\n" + "=" * 70)
print("  RESUMO FINAL — CAMADA OURO")
print("=" * 70)

print(f"""
  Tabelas criadas:
    1. ouro_dim_calendario:  {total_cal:>8,} registros (datas unicas)
    2. ouro_dim_categoria:   {total_cat:>8,} registros (pares cat/subcat)
    3. ouro_dim_produto:     {total_prod:>8,} registros (produtos unicos)
    4. ouro_fato_vendas:     {total_fato:>8,} registros (vendas diarias)

  Validacao Prata vs Ouro: {'TODAS OK' if all_ok else 'ATENCAO: DIVERGENCIAS ENCONTRADAS!'}
""")

print("--- VALORES PARA COMPARAR COM BIGQUERY ---")
print(f"  SELECT COUNT(*) => {totais_ouro['registros']:,}")
print(f"  SUM(faturamento) => {totais_ouro['faturamento']:,.2f}")
print(f"  SUM(qtd_vendas)  => {totais_ouro['vendas']:,}")

print(f"""
  Para validar no BigQuery, execute:
  
  SELECT 
    COUNT(*) as total_registros,
    SUM(CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS FLOAT64)) as faturamento_total,
    SUM(qtd_vendas_estimadas_dia) as vendas_total
  FROM `mercado-livre-mais-vendidos.Mercado_Livre.Fato_MercadoLivre_MaisVendidos`;
""")

print("OURO CONCLUIDA COM SUCESSO — MODELO DIMENSIONAL PRONTO!")
