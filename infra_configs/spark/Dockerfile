FROM de_infra-spark-jars-base:spark3.5-iceberg1.10 AS jars-provider

FROM spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

ENV SPARK_HOME=/opt/spark
ARG SPARK_JARS_DIR="$SPARK_HOME/jars"
COPY --from=jars-provider /tmp/jars/*.jar "${SPARK_JARS_DIR}/"
RUN chmod -R 755 "${SPARK_JARS_DIR}/"

COPY requirements.txt /requirements.txt

RUN ln -sf /usr/bin/python3 /usr/bin/python
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER $NB_UID
