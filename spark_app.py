# coding: utf-8
import time
import logging
import psutil
import sys
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import min, max, col
from pyspark.ml.regression import RandomForestRegressor 
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder

OPTIMIZED = True if sys.argv[1] == "True" else False

# --- Настройка логгера ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# --- Создание SparkSession с минимальными логами ---
conf = SparkConf()
conf.set("spark.ui.showConsoleProgress", "false")
conf.set("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
conf.set("spark.executor.memory", "1g")
conf.set("spark.driver.memory", "2g")

# --- SparkSession ---
spark = (
    SparkSession.builder.appName("SparkPerformanceApp")
    .master("spark://spark-master:7077")
    .config(conf=conf)
    .getOrCreate()
)

# Уровень логирования
spark.sparkContext.setLogLevel("ERROR")  # Убираем WARN, FATAL и прочее

if OPTIMIZED:
    logger.info("Запущена оптимизированная версия")
else:
    logger.info("Запущена версия без оптимизации")

# --- Замер времени ---
start_time = time.time()
logger.info("Загрузка данных началась.")

# --- Чтение данных ---
df = spark.read.csv(
    "hdfs:///data/data_simple.csv", header=True, inferSchema=True
)
logger.info("Данные загружены: {} строк.".format(df.count()))

# --- Обработка данных ---
logger.info("Кодирование признаков.")

for col in ["airline", "flight", "source_city", "departure_time", "stops", "arrival_time", "destination_city"]: 
    indexer = StringIndexer(inputCol=col, outputCol= col + "_index") 
    df = indexer.fit(df).transform(df) 
    df = df.drop(col) 
    df = df.withColumnRenamed(col + "_index", col) 

one_hot_encoder = OneHotEncoder(inputCol='stops', outputCol='stops_one_hot') 
one_hot_encoder = one_hot_encoder.fit(df) 
df = one_hot_encoder.transform(df) 
df = df.drop('stops') 

numeric_cols = ["duration", "days_left", "price"] 
assembler = VectorAssembler(inputCols=numeric_cols, outputCol='vectorized_data') 
df = assembler.transform(df) 

data_train, data_test = df.randomSplit([0.7, 0.3]) 
if OPTIMIZED:
    data_train.cache()
    data_train = data_train.repartition(4)
    data_test.cache()
    data_test = data_test.repartition(4)

logger.info("Обучение модели.")

model = RandomForestRegressor(featuresCol="vectorized_data", labelCol="price", maxBins=700) 
model = model.fit(data_train) 

pred_test = model.transform(data_test)

# if OPTIMIZED:
#     df = df.repartition(5).cache()

# --- Замер времени и RAM ---
end_time = time.time()
elapsed = end_time - start_time
spark.stop()
logger.info("Время выполнения Spark-пайплайна: {:.2f} секунд".format(elapsed))

process = psutil.Process(os.getpid())
ram_usage_mb = process.memory_info().rss / (1024 * 1024)
logger.info("Использование памяти драйвера: {:.2f} MB".format(ram_usage_mb))

# --- Завершение ---
logger.info("Завершение приложения.")
