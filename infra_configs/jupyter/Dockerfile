FROM jupyter/base-notebook

COPY requirements.txt /requirements.txt

USER $NB_UID
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk
	
ENV SPARK_HOME=/opt/spark

WORKDIR /opt/workspace

USER $NB_UID
