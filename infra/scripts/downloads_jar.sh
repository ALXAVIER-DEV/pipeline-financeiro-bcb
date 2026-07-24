#!/bin/bash

JAR_DIR = "infra/jars"
mkdir -p $JAR_DIR

BASE = "https://repo1.maven.org/maven2"

declare -A JARS(
    ["iceberg-spark"]="$BASE/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
    ["hadoop"]="$BASE/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    ["aws-java-sdk"]="$BASE/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
)

for name in "${!JARS[@]}"; do
    url="${JARS[$name]}"
    filename=$(basename "$url")    
    if [ ! -f "$JAR_DIR/$filename" ]; then
        echo "Baixando $name..."
        curl -L -o "$JAR_DIR/$filename" "$url"
    else
        echo "$name já existe, pulando download."
    fi
done

echo "Todos os JARs foram baixados com sucesso em $JAR_DIR/"

chmod +x scripts/downloads_jar.sh
./scripts/downloads_jar.sh
