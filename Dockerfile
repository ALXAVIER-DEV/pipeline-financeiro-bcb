# syntax=docker/dockerfile:1

FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=/opt/venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python
ENV PIP_DEFAULT_TIMEOUT=1000
ENV PIP_RETRIES=10

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        default-jre-headless \
        git \
        procps \
    && python -m venv "${VIRTUAL_ENV}" \
    && groupadd --system spark \
    && useradd --system --gid spark --create-home --home-dir /home/spark spark \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

RUN curl --fail --location \
      --retry 10 --retry-all-errors --retry-delay 5 \
      --connect-timeout 30 --max-time 1800 --continue-at - \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      --output /tmp/iceberg-spark-runtime.jar

RUN curl --fail --location \
      --retry 10 --retry-all-errors --retry-delay 5 \
      --connect-timeout 30 --max-time 600 --continue-at - \
      "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      --output /tmp/hadoop-aws.jar

RUN curl --fail --location \
      --retry 10 --retry-all-errors --retry-delay 5 \
      --connect-timeout 30 --max-time 3600 --continue-at - \
      "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      --output /tmp/aws-java-sdk-bundle.jar

COPY pyproject.toml README.md ./
COPY src ./src
COPY dbt ./dbt
COPY conf ./conf
COPY scripts ./scripts

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip install --prefer-binary .

ENV SPARK_HOME=/opt/venv/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN cp /tmp/iceberg-spark-runtime.jar "${SPARK_HOME}/jars/" \
    && cp /tmp/hadoop-aws.jar "${SPARK_HOME}/jars/" \
    && cp /tmp/aws-java-sdk-bundle.jar "${SPARK_HOME}/jars/" \
    && rm -f /tmp/*.jar \
    && mkdir -p \
        /app/data/tmp/spark \
        /app/data/tmp/hadoop \
        /app/data/tmp/s3a \
        /app/data/dagster \
    && chown -R spark:spark /app /opt/venv

USER spark

CMD ["python", "-m", "src.ingestion.ingest_bronze"]
