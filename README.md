# 🚀 Meli Intelligence Fabric Bot (@Joca_Meli_Fabric_bot)

> **Assistente Executivo Inteligente para Mercado Livre integrado ao Microsoft Fabric (Arquitetura Medallion), DuckDB e Google Gemini Flash.**

---

## 📌 Visão Geral

O **Meli Intelligence Fabric Bot** é uma solução corporativa de ponta a ponta que une:
1. **Pipeline de Dados Medallion no Microsoft Fabric** (Bronze ➔ Prata ➔ Ouro) gerando modelo dimensional colunar (`Fato_MercadoLivre_MaisVendidos.parquet`).
2. **Motor Analítico In-Memory de Alta Performance (DuckDB)** capaz de filtrar e agregar mais de 159.500 registros em milissegundos (< 15ms).
3. **Inteligência Artificial Generativa (Google Gemini Flash)** para tradução de linguagem natural (texto e voz) em consultas SQL precisas e formatação de respostas executivas estruturadas.
4. **Interface Conversacional via Telegram** com suporte a mensagens de texto e notas de voz (Speech-to-Text e Text-to-Speech com voz executiva masculina).

---

## 🏛️ Arquitetura do Projeto

```text
Microsoft Fabric (Medallion)                  Bot Meli Intelligence
----------------------------                  ---------------------
[API Mercado Livre]                           [Usuário Telegram]
         │                                             │
         ▼ (01_bronze_ingestao.py)                     ▼ (Texto / Áudio)
[Camada Bronze (JSON Bruto)]                  [Telegram @Joca_Meli_Fabric_bot]
         │                                             │
         ▼ (02_prata_limpeza.py)                       ▼ (Webhook / Polling)
[Camada Prata (Limpeza/Tipos)]                [app.py / bot_joca_meli.py]
         │                                        │         │
         ▼ (03_ouro_modelo_dimensional.py)        │         │ (Prompt + Schema)
[Fato_MercadoLivre_MaisVendidos.parquet]          │         ▼
         │                                        │   [Google Gemini Flash AI]
         └─────────────► [DuckDB Engine] ◄────────┘   (Gera SQL analítico)
                               │
                               ▼
                       [Resultado < 15ms]
                               │
                               ▼
                      [Resposta Formatada]
                               │
                               ▼
                      [Telegram Texto + Voz]
```

---

## 📂 Estrutura de Arquivos

| Arquivo / Pasta | Descrição |
|---|---|
| `app.py` | Web Service Flask para deploy 24/7 no Render.com (Webhook, suporte a áudio/voz, DuckDB e Gemini) |
| `bot_joca_meli.py` | Bot em modo Polling contínuo para execução local no computador |
| `00_etl_mercadolivre_completo.py` | Orquestrador completo do Pipeline Medallion Fabric (Bronze ➔ Prata ➔ Ouro) |
| `01_bronze_ingestao.py` | Coleta e ingestão dos dados brutos do Mercado Livre na camada Bronze |
| `02_prata_limpeza.py` | Limpeza, tratamento, deduplicação e padronização dos dados na camada Prata |
| `03_ouro_modelo_dimensional.py` | Criação do modelo dimensional Star Schema e geração da Fato em Parquet |
| `Fato_MercadoLivre_MaisVendidos.parquet` | Base analítica colunar compactada com 159.500 registros oficiais |
| `dicionario_dados_ml.json` | Dicionário semântico com categorias, subcategorias e marcas para a IA |
| `requirements.txt` | Dependências Python do projeto (Flask, DuckDB, Gemini, Edge-TTS, etc.) |
| `Procfile` | Configuração de inicialização de processo Web para Render/Heroku (`web: gunicorn app:app`) |
| `Subir_Para_GitHub.bat` | Script batch com 1 clique para sincronizar e enviar alterações para o GitHub |
| `Iniciar_Bot_Telegram.bat` | Script batch para iniciar o bot localmente com 1 clique |
| `Atualizar_Mercado_Livre_Fabric.ps1/.bat` | Rotina automatizada para atualizar os dados do Fabric |
| `Sincronizar_Base_BigQuery.py/.bat` | Rotina para espelhamento e sincronização com BigQuery |

---

## ⚙️ Variáveis de Ambiente (Opcionais no Render ou `.env`)

O bot possui tokens e chaves de fallback pré-configurados, mas para ambientes produtivos você pode definir:

| Variável | Descrição | Padrão / Fallback |
|---|---|---|
| `TELEGRAM_TOKEN` | Token do bot `@Joca_Meli_Fabric_bot` via BotFather | Token oficial embutido |
| `GEMINI_KEY` | Chave de API do Google Gemini | Chave oficial embutida |
| `SYNC_SECRET` | Chave secreta para autorizar o endpoint `/sync_data` | `meli_joca_sync_2026_karl` |
| `PORT` | Porta HTTP utilizada pelo Flask | `5000` (definida automaticamente pelo Render) |

---

## 🚀 Como Executar

### Opção 1: Execução Local (Modo Polling)
Ideal para desenvolvimento, testes e atualizações pontuais:
```bash
# 1. Instale as dependências
pip install -r requirements.txt

# 2. Inicie o bot
python bot_joca_meli.py
# Ou clique duas vezes em: Iniciar_Bot_Telegram.bat
```

### Opção 2: Deploy 24/7 na Nuvem Gratuita (Render.com)
Para manter o bot online 24 horas por dia sem precisar do seu computador ligado:
1. Acesse o [Render Dashboard](https://dashboard.render.com/) e clique em **New +** ➔ **Web Service**.
2. Conecte seu repositório: `karl-albert/meli-intelligence_Fabric-bot`.
3. Configure os campos:
   - **Environment:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn app:app`
   - **Plan:** `Free`
4. Após o deploy concluir, copie o link público gerado pelo Render (ex: `https://meli-intelligence-fabric-bot.onrender.com`).
5. Acesse no navegador o link: `https://SEU-LINK.onrender.com/set_webhook` para ativar o webhook automaticamente no Telegram!

---

## 🎙️ Funcionalidades do Bot (@Joca_Meli_Fabric_bot)

- **Consultas em Linguagem Natural:** Pergunte qualquer métrica sobre faturamento, quantidade de vendas, preço médio, Full, frete grátis, rankings, marcas e categorias.
- **Suporte Multimodal de Voz:** Mande uma mensagem de áudio pelo Telegram e o Joca responderá em texto e em áudio falado por voz executiva (`pt-BR-AntonioNeural`).
- **Resiliência Multi-Model:** Sistema com auto-recovery que alterna dinamicamente entre múltiplos modelos Gemini Flash caso haja limitação de taxa (429).
- **Fallback Formatter:** Se a IA atingir cota momentânea de geração de texto, um formatador em Python nativo gera o resumo executivo tabular instantaneamente.

---

## 👨‍💻 Autor

- **Karl Albert**
- Repositório Oficial: [github.com/karl-albert/meli-intelligence_Fabric-bot](https://github.com/karl-albert/meli-intelligence_Fabric-bot)
