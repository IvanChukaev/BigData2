# BigData2
Датасет - https://www.kaggle.com/datasets/shubhambathwal/flight-price-prediction файл data.csv
Перед запуском спарк выполнение preprocessing.ipynb преобразует data.csv в data_simple.csv - файл уменьшенного размера с измененным признаком "class"

Для запуска приложения:
  docker-compose up
  ./run_app.ps1
При одном запуске выполняются обе версии - с оптимизацией и без
Узлы:
  spark-master http://localhost:4040/
  spark-worker1 http://localhost:8081/
  namenode http://localhost:9870/
Результаты запуска:
1 node:	Оптимизированный - 48.93 секунд 44.13 MB, Без оптимизации - 81.19 секунд 44.14 MB; 3 nodes Оптимизированный - 22.07 секунд 44.18 MB, Без оптимизации - 23.08 секунд 44.14 MB
