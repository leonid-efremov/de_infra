FROM apache/hive:standalone-metastore-4.2.0

USER root

RUN mkdir -p /tmp/jars && \
    curl -o /tmp/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /tmp/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    cp /tmp/jars/*.jar /opt/hive/lib/ && \
    cp /tmp/jars/*.jar /opt/hadoop/share/hadoop/common/lib/ && \
    rm -rf /tmp/jars

RUN chmod -R 755 /opt/hive/lib/
RUN chmod -R 755 /opt/hadoop/share/hadoop/common/lib/

USER $NB_UID
