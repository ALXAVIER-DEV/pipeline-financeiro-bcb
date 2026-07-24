# 📊 Pipeline Financeiro BCB

> Data lakehouse pipeline que ingere séries macroeconômicas do **Banco Central do Brasil** (Selic, IPCA, câmbio USD/BRL), processa em arquitetura medallion (Bronze → Silver → Gold) com **Spark + Apache Iceberg**, orquestra com **Dagster**, agrega com **dbt** e expõe os indicadores em um dashboard **Streamlit**.

Projeto pessoal de portfólio em engenharia de dados — construído para demonstrar um pipeline batch de ponta a ponta com ferramentas do mundo real (não apenas notebooks).

---

## 🗺️ Arquitetura

```
BCB SGS API  ──►  Bronze (Iceberg, append-only)  ──►  Silver (Iceberg, overwrite por partição)  ──►  Gold (dbt)  ──►  Dashboard (Streamlit)
                         ▲                                      ▲
                         └──────────── orquestrado por Dagster (job diário) ─┘

Armazenamento: Apache Iceberg sobre S3 (LocalStack) via Spark
```

| Camada | Tecnologia | O que faz |
|---|---|---|
| **Bronze** | Spark + Iceberg | Ingestão bruta da API do BCB, append-only, particionado por ano, com metadados de auditoria (`_ingested_at`, `_source`, `_serie`) |
| **Silver** | Spark + Iceberg | Limpeza, enriquecimento e cálculo de indicadores derivados (médias móveis, variações, classificações), sobrescrita idempotente por partição |
| **Gold** | dbt (SQL) | Agregação mensal dos três indicadores em uma tabela única para consumo analítico |
| **Orquestração** | Dagster | Assets declarativos + job agendado (dias úteis, 08:00 UTC) |
| **Visualização** | Streamlit | Dashboard com métricas atuais e evolução do juro real estimado |

### Séries do BCB (SGS)

| Série | Código SGS | Frequência | Status |
|---|---|---|---|
| Selic | 11 | Diária | ✅ Bronze → Silver → Gold |
| IPCA | 433 | Mensal | ✅ Bronze → ⚠️ Silver não conectada ao Dagster ainda |
| Dólar (USD/BRL) | 1 | Diária | ✅ Bronze → Silver → Gold |
| PIB | 4380 | — | 🔜 Mapeada no client, ainda não ingerida |
| Inadimplência | 21082 | — | 🔜 Mapeada no client, ainda não ingerida |

---

## 🧱 Stack técnica

- **Ingestão**: Python, `requests`, `pandas`, `loguru`
- **Processamento**: Apache Spark 3.5 + Apache Iceberg 1.6 (tabelas ACID, time travel, particionamento)
- **Armazenamento**: S3 (emulado localmente com LocalStack)
- **Transformação analítica**: dbt-core
- **Orquestração**: Dagster (Software-Defined Assets + schedule)
- **Dashboard**: Streamlit
- **Qualidade**: pytest, ruff, mypy, CI no GitHub Actions

---

## 📂 Estrutura do projeto

```
src/
├── ingestion/           # Client da API do BCB + escrita na camada Bronze
│   ├── bcb_client.py       # fetch_bcb_data() — consome a API SGS
│   ├── bronze_writer.py    # writer_bronze() — grava Iceberg + control table
│   └── ingest_bronze.py    # CLI: python -m src.ingestion.ingest_bronze
├── transformation/       # Processadores da camada Silver
│   ├── base_processor.py     # BaseProcessor (template method: transform → create → write)
│   ├── processors/
│   │   ├── selic_processor.py    # variação, taxa anualizada, médias móveis, nível da taxa
│   │   ├── ipca_processor.py     # acumulado 12m, pressão inflacionária
│   │   └── dollar_processor.py   # variação, médias móveis, tendência
│   └── process_silver.py     # CLI: python -m src.transformation.process_silver [serie]
├── orchestration/
│   └── pipeline.py         # Assets e schedule do Dagster
├── dashboard/
│   └── app.py               # Dashboard Streamlit (lê a tabela gold)
└── utils/
    └── spark_session.py     # SparkSession configurada para Iceberg + S3A

dbt/models/gold/
└── indicadores_macroeconomicos.sql   # Agregação mensal (join Selic + IPCA + Dólar)

scripts/
├── check_bronze.py         # Sanity check da camada Bronze
└── check_silver.py         # Sanity check da camada Silver

tests/unit/                 # Testes com pytest + mocks de Spark/requests
infra/                       # docker-compose (LocalStack + Spark), jars do Iceberg, Terraform (scaffold)
```

---

## 🚀 Como rodar localmente

**Pré-requisitos**: Docker, Python 3.11+, Java (para o Spark local)

```bash
# 1. Instalar dependências
make install

# 2. Subir infraestrutura (LocalStack + Spark)
make docker-up
# ou apenas o LocalStack + bucket:
make localstack-up

# 3. Ingerir dados brutos do BCB (Bronze)
python -m src.ingestion.ingest_bronze 30      # últimos 30 dias

# 4. Processar camada Silver
python -m src.transformation.process_silver    # todas as séries
python -m src.transformation.process_silver selic   # uma série específica

# 5. Orquestrar com Dagster (alternativa aos passos 3-4)
dagster dev -f src/orchestration/pipeline.py
# UI em http://localhost:3000

# 6. Rodar o dashboard
streamlit run src/dashboard/app.py
```

### Testes e qualidade

```bash
make lint    # ruff + mypy
make test    # pytest com cobertura
```

CI configurado em [.github/workflows/ci.yml](.github/workflows/ci.yml): lint (ruff/mypy) + testes a cada push/PR para `main`/`develop`.

---

## 🔍 Destaques técnicos

- **Iceberg em vez de Parquet puro**: tabelas ACID com schema evolution e particionamento por ano (Bronze) — escolhido para simular um lakehouse real, não apenas arquivos estáticos.
- **Append vs. overwrite intencional**: Bronze é *append-only* (preserva histórico bruto de cada ingestão), Silver usa `overwritePartitions()` (idempotente, resultado sempre reflete o estado atual da lógica de transformação).
- **Indicadores derivados na Silver**: taxa Selic anualizada por capitalização composta (252 dias úteis), IPCA acumulado em 12 meses (inflação móvel), médias e volatilidade móveis de 30 dias, classificações de regime (nível da Selic, pressão inflacionária, tendência cambial) via window functions do Spark.
- **Camada Gold via dbt**: cálculo de um juro real estimado (Selic mensal − IPCA acumulado), pensado para consumo direto por BI/dashboard.
- **Control table de auditoria**: cada execução de ingestão registra linhas processadas, duração e status em `data/control/control_table.jsonl`.

---

## ⚠️ Estado atual / roadmap

Este é um projeto em evolução ativa — alguns pontos conhecidos, mantidos aqui de forma transparente:

- [ ] Asset `silver_ipca` ainda não está conectado ao pipeline do Dagster (apenas `selic` e `dollar` estão wired).
- [ ] Projeto dbt está parcialmente configurado (falta `dbt_project.yml`/`profiles.yml`/`sources.yml`); o model `gold` existe mas ainda não roda de forma independente.
- [ ] Divergência de nome entre a tabela Silver `dollar` e o source dbt `dolar` — a corrigir.
- [ ] Séries de PIB e inadimplência mapeadas no client mas ainda não ingeridas.
- [ ] Cobertura de testes crescendo — módulos de ingestão e transformação com testes unitários; suíte de integração planejada.

---

## 📖 Sobre a fonte de dados

Os dados são obtidos da **API SGS (Sistema Gerenciador de Séries Temporais)** do Banco Central do Brasil, pública e sem necessidade de autenticação: `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados`.

---

## 📄 Licença

Projeto pessoal para fins de estudo e portfólio.

---
---

# 📊 Pipeline Financeiro BCB (English)

> Data lakehouse pipeline that ingests macroeconomic time series from the **Central Bank of Brazil** (Selic policy rate, IPCA inflation, USD/BRL exchange rate), processes them through a medallion architecture (Bronze → Silver → Gold) with **Spark + Apache Iceberg**, orchestrates with **Dagster**, aggregates with **dbt**, and surfaces the indicators in a **Streamlit** dashboard.

Personal data engineering portfolio project — built to demonstrate an end-to-end batch pipeline using real-world tooling rather than notebooks alone.

## Architecture

```
BCB SGS API  ──►  Bronze (Iceberg, append-only)  ──►  Silver (Iceberg, partition overwrite)  ──►  Gold (dbt)  ──►  Dashboard (Streamlit)
                         ▲                                      ▲
                         └──────────── orchestrated by Dagster (daily job) ─┘

Storage: Apache Iceberg on S3 (LocalStack) via Spark
```

| Layer | Tech | What it does |
|---|---|---|
| **Bronze** | Spark + Iceberg | Raw ingestion from the BCB API, append-only, year-partitioned, with audit metadata (`_ingested_at`, `_source`, `_serie`) |
| **Silver** | Spark + Iceberg | Cleaning, enrichment, and derived indicators (rolling averages, variations, classifications), idempotent partition-overwrite |
| **Gold** | dbt (SQL) | Monthly aggregation of the three indicators into a single analytics-ready table |
| **Orchestration** | Dagster | Declarative software-defined assets + scheduled job (weekdays, 08:00 UTC) |
| **Visualization** | Streamlit | Dashboard with current metrics and estimated real interest rate over time |

### BCB series (SGS)

| Series | SGS code | Frequency | Status |
|---|---|---|---|
| Selic (policy rate) | 11 | Daily | ✅ Bronze → Silver → Gold |
| IPCA (inflation) | 433 | Monthly | ✅ Bronze → ⚠️ Silver not yet wired into Dagster |
| USD/BRL exchange rate | 1 | Daily | ✅ Bronze → Silver → Gold |
| GDP | 4380 | — | 🔜 Mapped in the client, not yet ingested |
| Credit default rate | 21082 | — | 🔜 Mapped in the client, not yet ingested |

## Tech stack

- **Ingestion**: Python, `requests`, `pandas`, `loguru`
- **Processing**: Apache Spark 3.5 + Apache Iceberg 1.6 (ACID tables, time travel, partitioning)
- **Storage**: S3 (emulated locally with LocalStack)
- **Analytics transformation**: dbt-core
- **Orchestration**: Dagster (software-defined assets + schedule)
- **Dashboard**: Streamlit
- **Quality**: pytest, ruff, mypy, GitHub Actions CI

## Getting started

**Prerequisites**: Docker, Python 3.11+, Java (for local Spark)

```bash
# 1. Install dependencies
make install

# 2. Bring up infrastructure (LocalStack + Spark)
make docker-up
# or just LocalStack + bucket:
make localstack-up

# 3. Ingest raw BCB data (Bronze)
python -m src.ingestion.ingest_bronze 30      # last 30 days

# 4. Process the Silver layer
python -m src.transformation.process_silver          # all series
python -m src.transformation.process_silver selic    # a single series

# 5. Orchestrate with Dagster (alternative to steps 3-4)
dagster dev -f src/orchestration/pipeline.py
# UI at http://localhost:3000

# 6. Run the dashboard
streamlit run src/dashboard/app.py
```

### Tests and quality

```bash
make lint    # ruff + mypy
make test    # pytest with coverage
```

CI configured in [.github/workflows/ci.yml](.github/workflows/ci.yml): lint (ruff/mypy) + tests on every push/PR to `main`/`develop`.

## Technical highlights

- **Iceberg instead of plain Parquet**: ACID tables with schema evolution and year partitioning (Bronze) — chosen to simulate a real lakehouse rather than static files.
- **Intentional append vs. overwrite**: Bronze is append-only (preserves the raw history of every ingestion run), Silver uses `overwritePartitions()` (idempotent — the result always reflects the current transformation logic).
- **Derived indicators in Silver**: annualized Selic rate via compound capitalization (252 business days), 12-month rolling IPCA (trailing inflation), 30-day rolling averages/volatility, regime classifications (Selic level, inflationary pressure, FX trend) via Spark window functions.
- **Gold layer via dbt**: computes an estimated real interest rate (monthly Selic − accumulated IPCA), designed for direct BI/dashboard consumption.
- **Audit control table**: every ingestion run logs rows processed, duration, and status to `data/control/control_table.jsonl`.

## Current state / roadmap

This project is under active development — known gaps, kept here transparently:

- [ ] `silver_ipca` asset not yet wired into the Dagster pipeline (only `selic` and `dollar` are connected).
- [ ] dbt project is partially scaffolded (missing `dbt_project.yml`/`profiles.yml`/`sources.yml`); the `gold` model exists but doesn't run standalone yet.
- [ ] Naming mismatch between the Silver table `dollar` and the dbt source `dolar` — to be fixed.
- [ ] GDP and credit-default series mapped in the client but not yet ingested.
- [ ] Test coverage growing — ingestion and transformation modules have unit tests; integration suite planned.

## About the data source

Data comes from the **SGS (Time Series Management System) API** of the Central Bank of Brazil, public and requiring no authentication: `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados`.

## License

Personal project for study and portfolio purposes.
