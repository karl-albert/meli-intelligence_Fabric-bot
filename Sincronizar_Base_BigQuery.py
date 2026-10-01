import os
import shutil
from google.cloud import bigquery
from google.oauth2 import service_account

print("=" * 70)
print("  SINCRONIZADOR BIGQUERY -> PARQUET LOCAL & FABRIC")
print("=" * 70)

key_path = r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\JSON\mercado-livre-mais-vendidos-143cf1ecac17.json"
if not os.path.exists(key_path):
    print(f"[ERRO] Chave GCP nao encontrada em: {key_path}")
    exit(1)

credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(project="mercado-livre-mais-vendidos", credentials=credentials)

query = "SELECT * FROM `mercado-livre-mais-vendidos.Mercado_Livre.Fato_MercadoLivre_MaisVendidos` ORDER BY data ASC, posicao_ranking ASC"
print("Baixando base mais recente do BigQuery...")
df = client.query(query).to_dataframe()

dt_min = df["data"].min()
dt_max = df["data"].max()
total_linhas = len(df)
print(f" -> Sucesso: {total_linhas:,} linhas baixadas. Periodo: {dt_min} ate {dt_max}")

destinos = [
    r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\fabric\Fato_MercadoLivre_MaisVendidos.parquet",
    r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\Fato_MercadoLivre_MaisVendidos.parquet",
    r"c:\000 - Karl\116 - Dashbords\Projetos_Power_BI\B3\Documentos do Projeto\Fato_MercadoLivre_MaisVendidos.parquet",
    r"c:\000 - Karl\116 - Dashbords\Projetos_Power_BI\B3\Documentos do Projeto\Render_Telegram_Bot\Fato_MercadoLivre_MaisVendidos.parquet"
]

temp_pq = r"c:\000 - Karl\116 - Dashbords\Projetos_Power_BI\B3\Documentos do Projeto\temp_bq.parquet"
df.to_parquet(temp_pq, index=False, compression="zstd")

for dst in destinos:
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copyfile(temp_pq, dst)
    print(f" -> Arquivo atualizado: {dst}")

if os.path.exists(temp_pq):
    os.remove(temp_pq)

print(f"\n[OK] Todas as pastas locais estao atualizadas ate {dt_max} ({total_linhas:,} registros)!")
