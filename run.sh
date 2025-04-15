#!/bin/bash

set -e 
set -o pipefail

log() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
    exit 1
}

log "Перенос датасета в контейнер namenode"
docker cp data_simple.csv namenode:/ || error "Ошибка копирования"

log "Перенос датасета в HDFS"
docker exec namenode hdfs dfs -mkdir -p /data || error "Ошибка создания директории"
docker exec namenode hdfs dfs -put -f data_simple.csv /data || error "Ошибка копирования"

log "Установка зависимостей в spark-worker-1"
docker exec spark-worker-1 apk add --no-cache make automake gcc g++ python3-dev linux-headers || error "Ошибка установки зависимостей"

log "Перенос приложения в spark-master"
docker cp spark_app.py spark-master:/tmp/spark_app.py || error "Ошибка копирования"

log "Установка зависимостей в spark-master"
docker exec spark-master apk add --no-cache make automake gcc g++ python3-dev linux-headers py3-pip && \
docker exec spark-master python3 -m pip install --upgrade pip setuptools wheel && \
pip3 install psutil || error "Ошибка установки зависимостей"

log "Запуск приложения"
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/spark_app.py True && \
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/spark_app.py False || error "Ошибка spark-submit"

log "Выполнение завершено"
