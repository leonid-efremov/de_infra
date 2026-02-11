FROM spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

RUN mkdir -p "${SPARK_HOME}/jars"
RUN mkdir /tmp/jars && \
    curl -o /tmp/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /tmp/jars/aws-java-sdk-bundle-1.12.791.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.791/aws-java-sdk-bundle-1.12.791.jar && \
    curl -o /tmp/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar && \
    curl -o /tmp/jars/iceberg-aws-bundle-1.10.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar && \
    cp /tmp/jars/*.jar "${SPARK_HOME}/jars/" && \
    rm -rf /tmp/jars

RUN chmod -R 755 "${SPARK_HOME}/jars/"

COPY requirements.txt /requirements.txt

USER $NB_UID
RUN pip install --no-cache-dir -r /requirements.txt

USER $NB_UID
