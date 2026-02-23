FROM de_infra-spark-jars-base:spark3.5-iceberg1.10 AS jars-provider

FROM jupyter/base-notebook:python-3.10

USER root

ARG SPARK_JARS_DIR="/opt/spark/jars"
COPY --from=jars-provider /tmp/jars/*.jar "${SPARK_JARS_DIR}/"
RUN chmod -R 755 "${SPARK_JARS_DIR}/"

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

COPY requirements.txt /requirements.txt

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/workspace

USER $NB_UID
