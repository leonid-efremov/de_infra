FROM de_infra-spark-jars-base:spark3.5-iceberg1.10 AS jars-provider

FROM apache/airflow:3.1.5-python3.10

USER root

ENV SPARK_HOME=/opt/spark
ARG SPARK_JARS_DIR="$SPARK_HOME/jars"
COPY --from=jars-provider /tmp/jars/*.jar "${SPARK_JARS_DIR}/"
RUN chmod -R 755 "${SPARK_JARS_DIR}/"
COPY --from=jars-provider "${SPARK_HOME}/conf/spark-defaults.conf" "${SPARK_HOME}/conf/spark-defaults.conf"
RUN chmod -R 755 "${SPARK_HOME}/conf/"

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

COPY requirements.txt /requirements.txt

USER airflow
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt
