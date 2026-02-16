FROM spark:3.5.7-scala2.12-java17-python3-ubuntu

FROM spark-jars-base:latest AS jars-provider

USER root

ENV SPARK_HOME = /opt/spark
ARG SPARK_JARS_DIR="$SPARK_HOME/jars"

COPY requirements.txt /requirements.txt

USER $NB_UID
RUN pip install --no-cache-dir -r /requirements.txt

USER $NB_UID
