FROM jupyter/pyspark-notebook:spark-4.1.1

COPY requirements.txt /requirements.txt

USER $NB_UID
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt update && \
    apt install -y openjdk-21-jdk
	
ENV SPARK_HOME=/opt/spark

WORKDIR /opt/workspace

USER $NB_UID
