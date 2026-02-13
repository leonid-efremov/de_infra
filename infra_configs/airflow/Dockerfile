FROM apache/airflow:3.1.5-python3.10

COPY requirements.txt /requirements.txt

USER airflow
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

RUN mkdir -p /opt/spark/jars 
RUN mkdir -p /tmp/jars && \
    curl -o /tmp/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /tmp/jars/aws-java-sdk-bundle-1.12.791.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.791/aws-java-sdk-bundle-1.12.791.jar && \
    curl -o /tmp/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar && \
    curl -o /tmp/jars/iceberg-aws-bundle-1.10.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar && \
    cp /tmp/jars/*.jar /opt/spark/jars/ && \
    rm -rf /tmp/jars

RUN chmod -R 755 /opt/spark/jars/

USER airflow
