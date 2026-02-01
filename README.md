# de_infra
Infrastructure for running DE pipelines locally

## Components
1. AirFlow - pipelines orchestrator
2. Ozone - S3 object storage
3. Jupyter Lab - code development and testing environment
4. Hive Metastore - каталог для таблиц

## Инструкция
Первый запуск на Windows:
1. Ставим и запускаем Docker Desktop
2. Обновляем WSL2 (cmd: wsl --update)
3. Запускаем из папки проекта (cmd: docker compose up -d). Для частичного запуска указываем профиль (docker compose --profile airflow up). Профили: core, airflow, spark, ozone

При успешном запуске:
1. UI AirFlow открывается по адресу: http://localhost:8080 (login: airflow, password: airflow)
2. В конфиге .env указан путь до хранилища AirFlow (и также Postgres). Добавить ДАГ можно (для примера: examples/airflow_test.py), скопировав его в эту папку
3. Ozone Recon доступен по адресу: http://localhost:9888
4. JupyterLab доступен по адресу: http://localhost:8888. Доступ по токену, можно взять в логах контейнера
5. Создаем ozone bucket: ozone sh bucket create s3v/${OZONE_BUCKET_NAME}

## Заметки
Java 21 (openjdk-21-jdk) не устанавливается (у меня)? Не удается найти пакет. Только 17.
