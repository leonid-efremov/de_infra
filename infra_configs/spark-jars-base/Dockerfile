FROM ubuntu:22.04

ENV SPARK_HOME = /opt/spark

USER root

RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /tmp/jars && \
    curl -o /tmp/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /tmp/jars/aws-java-sdk-bundle-1.12.791.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.791/aws-java-sdk-bundle-1.12.791.jar && \
    curl -o /tmp/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar && \
    curl -o /tmp/jars/iceberg-aws-bundle-1.10.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar && \

RUN chmod -R 755 /tmp/jars/

ONBUILD ARG SPARK_JARS_DIR
ONBUILD RUN if [ -z "$SPARK_JARS_DIR" ]; then \
        echo "SPARK_JARS_DIR не задан, используется ${SPARK_HOME}/jars"; \
        SPARK_JARS_DIR="${SPARK_HOME}/jars"; \
    fi && \
    mkdir -p "$SPARK_JARS_DIR" && \
    cp /tmp/jars/*.jar "$SPARK_JARS_DIR/" && \
    chmod -R 755 "$SPARK_JARS_DIR/"
