FROM jupyter/base-notebook

USER root
RUN apt-get update && \
    apt-get install -y curl openjdk-17-jdk

RUN mkdir -p /opt/spark/jars 
RUN mkdir -p /tmp/jars && \
    curl -o /tmp/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /tmp/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    cp /tmp/jars/*.jar /opt/spark/jars/ && \
    rm -rf /tmp/jars

RUN chmod -R 755 /opt/spark/jars/

COPY requirements.txt /requirements.txt

USER $NB_UID
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/workspace

USER $NB_UID
