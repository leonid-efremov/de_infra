from pyspark.sql import SparkSession


spark = SparkSession \
	.builder \
	.config('spark.driver.cores', '1')\
    .config('spark.driver.memory', '1G')\
    .config('spark.executor.instances', '1')\
    .config('spark.executor.cores', '1')\
    .config('spark.executor.memory', '512m')\
    .config("spark.ui.port", "4045")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.791.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "nessie") \
    .config("spark.sql.catalog.iceberg.uri", "http://nessie-catalog:19120/api/v2") \
    .config("spark.sql.catalog.iceberg.ref", "main") \
    .config("spark.sql.catalog.iceberg.warehouse", "warehouse") \
    .config("spark.sql.catalog.iceberg.authentication.type", "NONE") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://ozone-s3g:9878") \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "ozone-access-key") \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "ozone-secret-key") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.defaultFS", "s3a://ozone-om") \
    .config("spark.hadoop.fs.s3a.access.key", "ozone-access-key") \
    .config("spark.hadoop.fs.s3a.secret.key", "ozone-secret-key") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ozone-s3g:9878") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.committer.name", "directory") \
    .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging") \
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
    .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "50") \
    .config("spark.hadoop.fs.s3a.retry.limit", "50") \
    .config("spark.hadoop.fs.s3a.retry.interval", "5s") \
	.getOrCreate()

df = spark.createDataFrame([(1115, "spark job test", "2025-12-31")], ["id", "descr", "report_dt"])

df.write \
    .mode("append") \
    .insertInto("iceberg.test_iceberg_db_2.test_iceberg_table")

spark.stop()
