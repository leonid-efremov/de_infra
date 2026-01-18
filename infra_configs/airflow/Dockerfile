FROM apache/airflow:3.1.5-python3.12

COPY requirements.txt /requirements.txt

USER airflow
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

USER airflow
