# Roadmap — Pipeline Financeiro BCB

Este documento acompanha a evolução do projeto para uma execução totalmente
local e reproduzível com Docker, sem dependência de provedor de nuvem.

## Progresso

- [x] Criar imagem compartilhada com Python, Spark, aplicação e JARs.
- [x] Usar a imagem compartilhada no Spark Master e Worker.
- [x] Conectar a aplicação ao cluster Spark.
- [ ] Executar o pipeline completo dentro do Docker.
- [x] Executar Dagster dentro do Docker.
- [x] Completar a camada Gold com dbt.
- [x] Executar Streamlit dentro do Docker.
- [ ] Criar testes end-to-end.
- [ ] Validar imagens e serviços no CI.
- [ ] Finalizar a documentação de execução local.

## Item 1 — Imagem compartilhada

Status: concluído.

Imagem:

```text
pipeline-financeiro-bcb:spark-local
```

Componentes validados:

- Python 3.11.15;
- Spark/PySpark 3.5.9;
- Scala 2.12.18;
- Java 21;
- Iceberg Spark Runtime;
- Hadoop AWS;
- AWS Java SDK Bundle;
- código e dependências da aplicação.

## Item 2 — Spark Master e Worker

Status: concluído.

Serviços:

```text
spark-master
spark-worker
```

Configuração validada:

- Master saudável na porta `7077`;
- interface do Master em `http://localhost:8080`;
- interface do Worker em `http://localhost:8081`;
- Worker registrado com 2 núcleos e 2 GiB;
- Master e Worker executando Spark 3.5.9;
- healthchecks ativos.

## Item 3 — Conectar a aplicação ao cluster Spark

Status: concluído.

### 3.1 Tornar o endereço do Master configurável

No início de `get_spark()`, em `src/utils/spark_session.py`, adicionar:

```python
master_url = os.getenv("SPARK_MASTER_URL", "local[4]")
driver_host = os.getenv("SPARK_DRIVER_HOST", "127.0.0.1")
driver_bind_address = os.getenv(
    "SPARK_DRIVER_BIND_ADDRESS",
    "127.0.0.1",
)
driver_port = os.getenv("SPARK_DRIVER_PORT", "40440")
block_manager_port = os.getenv(
    "SPARK_BLOCK_MANAGER_PORT",
    "40441",
)
```

Substituir a criação fixa da sessão:

```python
SparkSession.builder
.appName(app_name)
.master("local[4]")
.config("spark.driver.host", "127.0.0.1")
.config("spark.driver.bindAddress", "127.0.0.1")
```

por:

```python
builder = (
    SparkSession.builder
    .appName(app_name)
    .master(master_url)
)
```

Aplicar as configurações atuais sobre `builder`:

```python
builder = (
    builder
    .config("spark.driver.extraClassPath", str(HADOOP_CONF_DIR))
    .config("spark.executor.extraClassPath", str(HADOOP_CONF_DIR))
    .config("spark.local.dir", str(SPARK_LOCAL_DIR))
    # Manter aqui as demais configurações Iceberg, S3A e performance.
)
```

Quando estiver usando o cluster, configurar um driver acessível pelo Worker:

```python
if not master_url.startswith("local"):
    builder = (
        builder
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", driver_bind_address)
        .config("spark.driver.port", driver_port)
        .config("spark.blockManager.port", block_manager_port)
    )
```

Finalizar com:

```python
return builder.getOrCreate()
```

### 3.2 Evitar configuração vazia de JARs

Substituir:

```python
jars = ",".join(str(j) for j in JARS_DIR.glob("*.jar"))
```

por:

```python
jar_files = list(JARS_DIR.glob("*.jar"))
```

Remover a configuração incondicional:

```python
.config("spark.jars", jars)
```

Antes de criar a sessão, adicionar:

```python
if jar_files:
    builder = builder.config(
        "spark.jars",
        ",".join(str(jar) for jar in jar_files),
    )
```

No Docker, os JARs presentes em `$SPARK_HOME/jars` serão carregados
automaticamente. Na execução pelo Windows, os arquivos locais em `jars/`
continuarão sendo usados.

### 3.3 Adicionar o serviço `pipeline`

Adicionar a `docker-compose.yml`:

```yaml
  pipeline:
    image: pipeline-financeiro-bcb:spark-local
    build:
      context: .
      dockerfile: Dockerfile
    command:
      - sleep
      - infinity
    environment:
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_DEFAULT_REGION: us-east-1
      AWS_ENDPOINT_URL: http://localstack:4566
      S3_BUCKET: financial-lakehouse
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_DRIVER_HOST: pipeline
      SPARK_DRIVER_BIND_ADDRESS: 0.0.0.0
      SPARK_DRIVER_PORT: "40440"
      SPARK_BLOCK_MANAGER_PORT: "40441"
    depends_on:
      spark-master:
        condition: service_healthy
      spark-worker:
        condition: service_healthy
      localstack:
        condition: service_started
    restart: unless-stopped
```

### 3.4 Validar o Compose

```powershell
docker compose config
docker compose config --services
```

Serviços esperados:

```text
localstack
create-buckets
spark-master
spark-worker
pipeline
```

### 3.5 Reconstruir os serviços

```powershell
docker compose build spark-master spark-worker pipeline
```

### 3.6 Subir o ambiente

```powershell
docker compose up -d `
  localstack `
  create-buckets `
  spark-master `
  spark-worker `
  pipeline
```

Validar:

```powershell
docker compose ps
```

### 3.7 Executar o smoke test

```powershell
docker compose exec pipeline `
  python -c "from src.utils.spark_session import get_spark; spark = get_spark('smoke-test'); print('MASTER:', spark.sparkContext.master); print('COUNT:', spark.range(10).count()); spark.stop()"
```

Resultado esperado:

```text
MASTER: spark://spark-master:7077
COUNT: 10
```

### 3.8 Validar o catálogo Iceberg

```powershell
docker compose exec pipeline `
  python -c "from src.utils.spark_session import get_spark; spark = get_spark('iceberg-test'); spark.sql('SHOW NAMESPACES IN local').show(); spark.stop()"
```

Namespaces esperados:

```text
bronze
gold
silver
```

### Critério de conclusão do item 3

- [x] `pipeline` permanece ativo;
- [x] a sessão usa `spark://spark-master:7077`;
- [x] o smoke test retorna `COUNT: 10`;
- [x] o job é enviado ao cluster Spark;
- [x] os namespaces Iceberg `bronze`, `silver` e `gold` são listados;
- [x] os logs não mostram erro de DNS ou conexão com o driver.

Validação executada:

```text
MASTER: spark://spark-master:7077
COUNT: 10

+---------+
|namespace|
+---------+
|bronze   |
|gold     |
|silver   |
+---------+
```

Para alinhar o catálogo Iceberg com a estrutura criada no LocalStack, o
warehouse da aplicação foi definido como
`s3a://financial-lakehouse/warehouse`. O bootstrap do bucket também passou a
criar os prefixos `warehouse/bronze`, `warehouse/silver` e `warehouse/gold`.

## Item 4 — Pipeline completo no Docker

Status: concluído.

1. [x] executar ingestão Bronze pelo container `pipeline`;
2. [x] executar os três processadores Silver;
3. [x] validar contagens e schemas;
4. [x] criar um comando único em `scripts/run-pipeline.sh`;
5. [x] garantir execução idempotente.

Validação executada:

- pipeline Bronze → Silver concluído com sucesso;
- processadores Selic, dólar e IPCA concluídos;
- a leitura Bronze mantém a ingestão mais recente de cada data;
- Selic, dólar e IPCA não apresentam datas duplicadas na Silver.

## Item 5 — Dagster no Docker

Status: concluído.

1. [x] adicionar `dagster-webserver` às dependências;
2. [x] preparar `/app/data/dagster` na imagem;
3. [x] adicionar o serviço `dagster` ao Compose;
4. [x] usar a imagem compartilhada;
5. [x] expor a porta `3000`;
6. [x] configurar acesso ao Spark e LocalStack;
7. [x] persistir metadados no volume `dagster_data`;
8. [x] materializar os assets Bronze e Silver;
9. [x] validar a persistência do histórico.

### 5.1 Validar o Compose

```powershell
docker compose config
docker compose config --services
```

Serviços esperados:

```text
localstack
create-buckets
spark-master
spark-worker
pipeline
dagster
```

### 5.2 Construir o serviço

```powershell
docker compose build dagster
```

### 5.3 Subir e acompanhar o Dagster

```powershell
docker compose up -d dagster
docker compose ps
docker compose logs -f dagster
```

Interface:

```text
http://localhost:3000
```

Use `Ctrl+C` para sair dos logs sem encerrar o container.

### 5.4 Executar o job

Pela interface, abrir `financial_pipeline` e selecionar **Launch Run**.

Pelo terminal:

```powershell
docker compose exec dagster dagster job execute `
  -f src/orchestration/pipeline.py `
  -j financial_pipeline
```

### 5.5 Investigar falhas

```powershell
docker compose logs --tail 200 dagster
docker compose logs --tail 100 spark-master
docker compose logs --tail 100 spark-worker
```

### 5.6 Validar persistência

```powershell
docker compose restart dagster
docker compose ps dagster
docker compose logs --tail 100 dagster
```

Depois do restart, o histórico das execuções deve continuar disponível em
`http://localhost:3000`.

### Critério de conclusão do item 5

- serviço `dagster` saudável;
- seis assets visíveis;
- job `financial_pipeline` concluído;
- jobs Spark recebidos pelo Worker;
- tabelas Bronze e Silver atualizadas;
- histórico preservado após reiniciar o serviço.

## Item 6 — dbt e camada Gold

Status: concluído.

1. [x] adicionar `dbt-spark[PyHive]`;
2. [x] adicionar o Spark Thrift Server ao Compose;
3. [x] criar `dbt_project.yml`;
4. [x] criar `profiles.yml`;
5. [x] declarar as sources Silver;
6. [x] corrigir o modelo `indicadores_macroeconomicos`;
7. [x] validar a conexão com `dbt debug`;
8. [x] executar `dbt run`;
9. [x] executar `dbt test`;
10. [x] validar `local.gold.indicadores_macroeconomicos`.

### Comandos de validação

```powershell
docker compose config
docker compose build spark-thrift dbt
docker compose up -d spark-thrift
docker compose ps spark-thrift
docker compose logs --tail 100 spark-thrift
```

```powershell
docker compose run --rm dbt debug
docker compose run --rm dbt run
docker compose run --rm dbt test
```

Consulta final:

```powershell
docker compose exec pipeline python -c "from src.utils.spark_session import get_spark; s=get_spark('validate-gold'); s.sql('SELECT * FROM local.gold.indicadores_macroeconomicos ORDER BY mes_ref DESC').show(20, False); s.stop()"
```

### Critério de conclusão do item 6

- Spark Thrift Server saudável;
- `dbt debug` conecta ao Spark;
- sources `selic`, `ipca` e `dollar` resolvidas no catálogo `local.silver`;
- `dbt run` cria a tabela Iceberg Gold;
- `dbt test` aprovado;
- tabela `local.gold.indicadores_macroeconomicos` consultável pelo pipeline.

Validação executada:

- `dbt debug`: conexão aprovada;
- `dbt run`: modelo Gold criado com sucesso;
- `dbt test`: três testes aprovados;
- tabela Gold consultada pelo container `pipeline`, com 14 meses retornados;
- Spark Thrift limitado a 1 núcleo para permitir jobs concorrentes no Worker.

## Item 7 — Streamlit no Docker

Status: concluído.

1. [x] adicionar serviço `dashboard`;
2. [x] expor a porta `8501`;
3. [x] conectar ao catálogo Gold;
4. [x] tratar tabela inexistente e DataFrame vazio;
5. [x] validar o dashboard em `http://localhost:8501`.

Validação executada:

- serviço `dashboard` saudável;
- endpoint `/_stcore/health` retornando `ok`;
- aplicação executada sem exceções pelo framework de testes do Streamlit;
- métricas de Selic, IPCA e dólar renderizadas com dados da tabela Gold;
- sessão Spark encerrada após a carga para liberar recursos do Worker.

## Item 8 — CI e testes end-to-end

1. validar Ruff e mypy;
2. executar pytest com cobertura;
3. validar `docker compose config`;
4. construir a imagem;
5. subir a infraestrutura;
6. executar um smoke test;
7. mostrar logs em caso de falha;
8. encerrar os serviços com `docker compose down`.

## Definição de pronto

O projeto estará completo quando uma pessoa conseguir executar:

```bash
git clone <URL_DO_REPOSITORIO>
cd pipeline-financeiro-bcb
cp .env.example .env
docker compose up --build -d
```

E acessar:

- Spark Master: `http://localhost:8080`;
- Spark Worker: `http://localhost:8081`;
- Dagster: `http://localhost:3000`;
- Streamlit: `http://localhost:8501`.
