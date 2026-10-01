#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================================================
BOT DE TELEGRAM: Meli Intelligence Fabric (@Joca_Meli_Fabric_bot)
CANAL CONVERSACIONAL COM INTELIGENCIA ARTIFICIAL GENERATIVA (GOOGLE GEMINI FLASH)
INTEGRADO COM: DuckDB + Microsoft Fabric (Fato_MercadoLivre_MaisVendidos.parquet)
========================================================================================
"""

import os
import sys
import time
import requests
import duckdb
import google.generativeai as genai

# Configuracao de encoding UTF-8 no Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

# CREDENCIAIS DO BOT (Base64 + Variaveis de Ambiente)
import base64

_B64_TOK = "ODk1ODUyNTM2MzpBQUgwUkQwbDhlWHZyZTFZeTJYVE90VkxuT0FCOGx1UWRoRQ=="
_B64_GEM = "QVEuQWI4Uk42S3RWbzR3RkhZTVA4a3FiMXplWXo2dmRTLVRrakd3ZG1yY18xbzY4MURuUUE="

FALLBACK_KEY = base64.b64decode(_B64_GEM).decode("utf-8").strip()
FALLBACK_TOKEN = base64.b64decode(_B64_TOK).decode("utf-8").strip()

TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip() or FALLBACK_TOKEN
GEMINI_KEY = os.environ.get("GEMINI_KEY", "").strip() or FALLBACK_KEY
BOT_USERNAME = "@Joca_Meli_Fabric_bot"
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

# Localizacao dinamica da base Fato_MercadoLivre_MaisVendidos.parquet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_FILE = os.path.join(BASE_DIR, "Fato_MercadoLivre_MaisVendidos.parquet").replace("\\", "/")

if not os.path.exists(PARQUET_FILE):
    candidatos = [
        r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\fabric\Fato_MercadoLivre_MaisVendidos.parquet",
        r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\Fato_MercadoLivre_MaisVendidos.parquet",
        r"c:\000 - Karl\116 - Dashbords\Projetos_Power_BI\B3\Documentos do Projeto\Fato_MercadoLivre_MaisVendidos.parquet"
    ]
    for c in candidatos:
        if os.path.exists(c):
            PARQUET_FILE = c.replace("\\", "/")
            break

print("=" * 70)
print(f"  [*] INICIALIZANDO Meli Intelligence Fabric ({BOT_USERNAME})")
print("  [*] Origem dos Dados (Microsoft Fabric): " + PARQUET_FILE)
print("=" * 70)

# Configurar chave Gemini
genai.configure(api_key=GEMINI_KEY)

# Modelos Gemini com fallback para contornar limites de taxa (Free Tier)
AVAILABLE_MODELS = ["gemini-3.1-flash-lite", "gemini-3.8-flash", "gemini-3.5-flash-lite"]

def chamar_gemini(prompt):
    for model_name in AVAILABLE_MODELS:
        try:
            m = genai.GenerativeModel(model_name)
            resp = m.generate_content(prompt)
            if resp and resp.text:
                return resp.text.strip()
        except Exception as e:
            err_msg = str(e)
            if "429" in err_msg or "ResourceExhausted" in err_msg:
                print(f"  [Aviso] Cota atingida em {model_name}, alternando para proximo modelo...")
                continue
            elif "404" in err_msg or "NotFound" in err_msg:
                continue
            else:
                print(f"  [Erro Gemini {model_name}]: {e}")
                continue
    return None

# Inicializar DuckDB em memoria
print(f"Carregando {PARQUET_FILE} no DuckDB...")
con = duckdb.connect()
con.execute(f"""
    CREATE TABLE fato_ml AS 
    SELECT 
        data, 
        ano, 
        mes, 
        ano_mes,
        posicao_ranking,
        categoria, 
        subcategoria, 
        titulo_produto, 
        marca, 
        TRY_CAST(qtd_vendas_estimadas_dia AS INT) as qtd_vendas_num, 
        TRY_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS DOUBLE) as fat_num, 
        TRY_CAST(REPLACE(REPLACE(preco_atual, '.', ''), ',', '.') AS DOUBLE) as preco_num, 
        is_full, 
        frete_gratis 
    FROM '{PARQUET_FILE}'
""")

# Obter metadados basicos
data_recente = con.execute("SELECT MAX(data) FROM fato_ml").fetchone()[0]
total_registros = con.execute("SELECT COUNT(*) FROM fato_ml").fetchone()[0]
data_inicio = con.execute("SELECT MIN(data) FROM fato_ml").fetchone()[0]

try:
    partes_dt = str(data_recente).split("-")
    data_recente_fmt = f"{partes_dt[2]}/{partes_dt[1]}/{partes_dt[0]}"
except Exception:
    data_recente_fmt = str(data_recente)

print(f"[OK] Base DuckDB pronta: {total_registros:,} registros. Periodo: {data_inicio} ate {data_recente}")
print(f"[OK] Bot {BOT_USERNAME} pronto e aguardando mensagens no Telegram!\n")

def enviar_mensagem(chat_id, texto):
    try:
        url = f"{BASE_URL}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": texto,
            "parse_mode": "Markdown"
        }
        res = requests.post(url, json=payload, timeout=15)
        if not res.json().get("ok"):
            payload.pop("parse_mode")
            res = requests.post(url, json=payload, timeout=15)
        return res.json()
    except Exception as e:
        print(f"Erro ao enviar mensagem: {e}")
        return None

def formatar_resultado_python(df_res, user_name, pergunta_usuario):
    """Fallback quando o Gemini atinge cota de formatacao: formata diretamente em Python"""
    if df_res.empty:
        return f"Fala {user_name}! Consultei a base oficial, mas nao encontrei registros correspondentes a sua pergunta."
    
    linhas = [
        f"📊 *Fala {user_name}! Segue o resultado da sua consulta (Microsoft Fabric):*",
        f"🔍 _\"{pergunta_usuario}\"_\n"
    ]
    
    if len(df_res) == 1 and len(df_res.columns) == 1:
        col = df_res.columns[0]
        val = df_res.iloc[0, 0]
        if isinstance(val, (int, float)):
            if "fat" in col.lower() or "preco" in col.lower() or "ticket" in col.lower():
                val_fmt = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            else:
                val_fmt = f"{val:,.0f}".replace(",", ".")
        else:
            val_fmt = str(val)
        col_nome = col.replace("_", " ").title()
        linhas.append(f"📦 *{col_nome}:* `{val_fmt}`\n")
    else:
        for idx, row in df_res.head(8).iterrows():
            itens = []
            for col, val in row.items():
                if isinstance(val, float):
                    if "fat" in col.lower() or "preco" in col.lower():
                        v_str = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    else:
                        v_str = f"{val:,.0f}".replace(",", ".")
                elif isinstance(val, int):
                    v_str = f"{val:,.0f}".replace(",", ".")
                else:
                    v_str = str(val)
                itens.append(f"*{col.replace('_', ' ').title()}:* {v_str}")
            linhas.append("• " + " | ".join(itens))
        
        if len(df_res) > 8:
            linhas.append(f"\n_... e mais {len(df_res) - 8} registros encontrados._")

    linhas.append(f"\n📌 _Dados oficiais Microsoft Fabric · Atualizado ate {data_recente_fmt}_")
    return "\n".join(linhas)

def consultar_via_ia(pergunta_usuario, user_name):
    """
    Usa o Gemini para converter a pergunta em SQL DuckDB,
    executa na base real e formata a resposta executiva.
    """
    prompt_sql = f"""
Voce e o motor analitico SQL DuckDB do Mercado Livre Brasil (Microsoft Fabric).
A tabela DuckDB chama-se 'fato_ml'.
Schema da tabela:
- data (DATE, formato YYYY-MM-DD)
- ano (INT, ex: 2025, 2026)
- mes (INT, 1 a 12)
- categoria (VARCHAR, ex: 'Celulares e Telefones', 'Informatica', 'Eletrodomesticos', 'Casa, Moveis e Decoracao', 'Ferramentas e Construcao')
- subcategoria (VARCHAR, ex: 'Smartphones', 'Notebooks', 'Games')
- titulo_produto (VARCHAR, nome do anuncio)
- marca (VARCHAR, ex: 'Apple', 'Samsung', 'Xiaomi', 'Sony', 'Dell', etc.)
- qtd_vendas_num (INT, quantidade de vendas estimadas)
- fat_num (DOUBLE, faturamento estimado em reais)
- preco_num (DOUBLE, preco atual de venda)
- is_full (BOOLEAN)
- frete_gratis (BOOLEAN)

Contexto de negocio:
- Data mais recente na base (considerada 'hoje' ou 'data atual'): '{data_recente}'.
- MTD (Month to Date / acumulado do mes): ano = 2026 AND mes = 9 AND data <= '{data_recente}'.
- YTD (Year to Date / acumulado do ano): ano = 2026 AND data <= '{data_recente}'.
- Quando pedir marcas ou produtos, use ILIKE para evitar problemas de maiusculas/minusculas.

Pergunta do usuario: \"{pergunta_usuario}\"

Gere uma unica query SQL SELECT DuckDB para extrair o dado exato que responda a pergunta.
Se a pergunta nao for analitica sobre dados (ex: 'oi', 'quem e voce'), retorne 'NAO_SQL'.
Responda APENAS com a query SQL dentro de ```sql ... ``` ou com a palavra NAO_SQL.
"""
    try:
        resp_sql = chamar_gemini(prompt_sql)
        if not resp_sql or "NAO_SQL" in resp_sql:
            return None
            
        clean_sql = resp_sql.replace("```sql", "").replace("```", "").strip()
        print(f"  [SQL Gemini]: {clean_sql}")
        
        df_res = con.execute(clean_sql).df()
        print(f"  [Resultado DuckDB]: {len(df_res)} linhas retornadas")
        
        prompt_formatacao = f"""
Voce e o assistente virtual executivo 'Meli Intelligence Fabric' ({BOT_USERNAME}) do time comercial do Mercado Livre (Microsoft Fabric).
O representante de vendas '{user_name}' perguntou: \"{pergunta_usuario}\"
A data de referencia mais recente na base e '{data_recente}' ({data_recente_fmt}).

O resultado obtido no banco de dados oficial foi:
{df_res.to_string()}

Formate a resposta para o Telegram:
- Comece com uma saudacao amigavel: "Fala {user_name}!..."
- Use emojis comerciais (📊, 💰, 📦, 🏷️, 🏆, 🚀)
- Formate valores monetarios em R$ (ex: R$ 1.500.000,00) e quantidades com separador de milhar.
- Seja objetivo, direto e executivo.
- Adicione uma nota de rodape breve: "📌 _Dados oficiais da base do Mercado Livre (Microsoft Fabric) · Atualizado ate {data_recente_fmt}_"
"""
        resp_final = chamar_gemini(prompt_formatacao)
        if resp_final:
            return resp_final
        else:
            return formatar_resultado_python(df_res, user_name, pergunta_usuario)
        
    except Exception as e:
        print(f"  [Erro IA/SQL]: {e}")
        return None

def processar_pergunta(texto_msg, user_name):
    t_lower = texto_msg.lower().strip()
    
    # 1. Comando /start ou /ajuda
    if t_lower in ['/start', '/ajuda', 'oi', 'ola', 'olá', 'start']:
        total_fmt = f"{total_registros:,}".replace(",", ".")
        return (
            f"👋 *Fala {user_name}! Eu sou o Meli Intelligence Fabric ({BOT_USERNAME}).*\n\n"
            f"Estou conectado a base oficial de Mais Vendidos (Microsoft Fabric) com a IA do **Google Gemini**.\n"
            f"📅 *Base atualizada ate:* `{data_recente_fmt}` ({total_fmt} registros sincronizados).\n\n"
            f"💡 *Voce pode me perguntar qualquer coisa em linguagem natural:*\n"
            f"• _\"Qual a quantidade de vendas total ate agora no MTD?\"_\n"
            f"• _\"Quantos produtos no total da Apple venderam no dia de hoje ate agora?\"_\n"
            f"• _\"Qual o faturamento total de hoje?\"_\n"
            f"• _\"Qual o produto mais vendido de informatica?\"_\n"
            f"• _\"Qual a data mais recente da base?\"_\n\n"
            f"Pode mandar do jeito que voce preferir!"
        )
    
    # 2. Exemplo historico do Slide 5
    if 'slide' in t_lower and ('exemplo' in t_lower or 'caso' in t_lower or 'apresentação' in t_lower or 'apresentacao' in t_lower):
        return (
            f"🤖 *Meli Intelligence Fabric* · _Caso de Uso do Slide 5_\n"
            f"Na apresentacao executiva (Slide 5), usamos o exemplo do dia 18/09:\n\n"
            f"📦 *Volume Vendido Apple (18/09):* 40 unidades\n"
            f"💰 *Faturamento Estimado:* R$ 535.680\n"
            f"🏷️ *Preco Medio:* R$ 13.392,01  ·  *% FULL:* 151%\n"
            f"🏆 *Top 1 Anuncio:* iPhone 18 PRO MAX 512GB (10 unidades)\n\n"
            f"📌 _Base atualizada com dados em tempo real ate {data_recente_fmt}!_"
        )
    
    # 3. Processamento dinamico via IA + DuckDB
    resposta_ia = consultar_via_ia(texto_msg, user_name)
    if resposta_ia:
        return resposta_ia

    # 4. Fallback caso a IA nao retorne
    return (
        f"🤖 *Meli Intelligence Fabric*\n"
        f"Nao consegui processar a consulta para: _\"{texto_msg}\"_\n\n"
        f"Tente reformular, por exemplo:\n"
        f"• *\"Total de vendas no MTD\"*\n"
        f"• *\"Faturamento de hoje por categoria\"*\n"
        f"• *\"Qual a data mais recente da base?\"*"
    )

def main():
    offset = None
    url_cloud_wh = "https://meli-intelligence-bot.onrender.com/webhook"

    if "8916733671" in TOKEN:
        try:
            wh_info = requests.get(f"{BASE_URL}/getWebhookInfo", timeout=10).json()
            if wh_info.get("result", {}).get("url"):
                print(f"  [*] Webhook em nuvem detectado: {wh_info['result']['url']}")
                print("  [*] Liberando Telegram para execucao local...")
                requests.get(f"{BASE_URL}/deleteWebhook", timeout=10)
        except Exception as e:
            print(f"  [Aviso Webhook]: {e}")

    print(f"[*] Conectado ao Telegram ({BOT_USERNAME})! Polling com IA ativo...")
    try:
        while True:
            try:
                url = f"{BASE_URL}/getUpdates"
                params = {"timeout": 25}
                if offset:
                    params["offset"] = offset
                    
                res = requests.get(url, params=params, timeout=30)
                data = res.json()
                
                if data.get("ok"):
                    for item in data.get("result", []):
                        offset = item["update_id"] + 1
                        msg = item.get("message")
                        if not msg or "text" not in msg:
                            continue
                            
                        chat_id = msg["chat"]["id"]
                        user_name = msg["from"].get("first_name", "Parceiro")
                        texto = msg["text"]
                        
                        print(f"\n[{time.strftime('%H:%M:%S')}] Pergunta de {user_name}: {texto}")
                        
                        resposta = processar_pergunta(texto, user_name)
                        enviar_mensagem(chat_id, resposta)
                        print(f"[{time.strftime('%H:%M:%S')}] Resposta enviada com sucesso!")
                        
                time.sleep(0.5)
            except requests.exceptions.RequestException:
                time.sleep(2)
            except Exception as e:
                print(f"Erro no loop principal: {e}")
                time.sleep(2)
    except KeyboardInterrupt:
        print("\n[!] Encerrando bot local...")
    finally:
        if "8916733671" in TOKEN:
            try:
                print("  [*] Reativando Webhook em nuvem no Render (24/7)...")
                requests.get(f"{BASE_URL}/setWebhook?url={url_cloud_wh}", timeout=10)
            except Exception:
                pass

if __name__ == '__main__':
    main()
