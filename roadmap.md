# Roadmap — Pipeline Financeiro BCB

Este documento acompanha a evolução do projeto para uma execução totalmente
local e reproduzível com Docker, sem dependência de provedor de nuvem.

## Progresso

- [x] Criar imagem compartilhada com Python, Spark, aplicação e JARs.
- [x] Usar a imagem compartilhada no Spark Master e Worker.
- [x] Conectar a aplicação ao cluster Spark.
- [ ] Executar o pipeline completo dentro do Docker.
- [ ] Executar Dagster dentro do Docker.
- [ ] Completar a camada Gold com dbt.
- [ ] Executar Streamlit dentro do Docker.
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

Depois do item 3:

1. executar ingestão Bronze pelo container `pipeline`;
2. executar os três processadores Silver;
3. validar contagens e schemas;
4. [x] criar um comando único em `scripts/run-pipeline.sh`;
5. garantir execução idempotente.

## Item 5 — Dagster no Docker

1. adicionar serviço `dagster`;
2. usar a mesma imagem compartilhada;
3. expor a porta `3000`;
4. configurar acesso ao Spark e LocalStack;
5. persistir metadados do Dagster;
6. materializar os assets Bronze e Silver pela interface.

## Item 6 — dbt e camada Gold

1. completar `dbt_project.yml`;
2. adicionar o adapter necessário;
3. configurar `profiles.yml`;
4. declarar sources Silver;
5. executar `dbt run`;
6. executar `dbt test`;
7. validar `local.gold.indicadores_macroeconomicos`.

## Item 7 — Streamlit no Docker

1. adicionar serviço `dashboard`;
2. expor a porta `8501`;
3. conectar ao catálogo Gold;
4. tratar tabela inexistente e DataFrame vazio;
5. validar o dashboard em `http://localhost:8501`.

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
