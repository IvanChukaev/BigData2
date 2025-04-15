docker cp .\data_simple.csv namenode:/
docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put -f /data_simple.csv /data

docker exec spark-worker-1 apk add --no-cache make automake gcc g++ python3-dev linux-headers

docker cp .\spark_app.py spark-master:/tmp/spark_app.py

docker exec spark-master apk add --no-cache make automake gcc g++ python3-dev linux-headers py3-pip
docker exec spark-master python3 -m pip install --upgrade pip setuptools wheel
docker exec spark-master pip3 install numpy psutil

#Без оптимизации
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/spark_app.py False

#С оптимизацией
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/spark_app.py True
