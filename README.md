# de_infra
Infrastructure for running DE pipelines locally

## Components
1. AirFlow - pipelines orchestrator
2. Ozone - S3 object storage
3. Jupyter Lab - code development and testing environment
4. Spark (localy or cluster mode) - ETL, data processing
5. Iceberg + Nessie catalog - table metadata
~~5.5. Hive Metastore (unused)~~

## Инструкция
Первый запуск на Windows:
1. Ставим и запускаем Docker Desktop
2. Обновляем WSL2 (cmd: wsl --update)
3. Запускаем из папки проекта (cmd: docker compose up --profile spark). Для частичного запуска указываем профиль (docker compose --profile airflow up). Профили: core, airflow, spark, ozone, hive
4. Опционально, установить Make: winget install GnuWin32.Make

При успешном запуске:
1. UI AirFlow открывается по адресу: http://localhost:8080 (login: airflow, password: airflow)
2. Ozone Recon доступен по адресу: http://localhost:9888
3. JupyterLab доступен по адресу: http://localhost:8888. Доступ по токену, можно взять в логах контейнера
4. Создаем ozone bucket: docker exec _om-container-id_ ozone sh bucket create s3v/${OZONE_BUCKET_NAME}
5. В кластерном режиме Spark, UI spark master-а доступен по ссылке: http://localhost:8088

Тестирование:
1. Добавляем тестовый ДАГ в AirFlow (airflow_test), скопировав его в папку ./docker-volumes/airflow/dags
2. Выводим Ozone SCM из безопасного режима руками: docker exec _scm-container-id_ ozone admin safemode exit
3. Записываем данные в S3 через Spark (тетрадка pyspark-localy, копируем в папку ./docker-volumes/jupyter_notebooks)
4. Записываем данные в Iceberg-таблицу через Spark (pyspark-cluster)
5. Запускаем тоже самое через AirFlow (dag - airflow/dags/spark-dag.py; job - airflow/dags/spark-jobs/spark-job-test.py)

## Заметки
1. Java 21 (openjdk-21-jdk) не устанавливается (у меня)? Не удается найти пакет. Только 17.
2. Используем Nessie catalog для Iceberg. Hive Metastore (>4) не работают с Iceberg? Open issue: https://github.com/apache/iceberg/issues/12878 
