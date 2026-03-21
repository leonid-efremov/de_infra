import os
os.environ['SPARK_CONF_DIR'] = '/opt/spark/conf'
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from ETL.utils import PySparkTableLoader


TABLE_NAME = os.environ['TABLE_NAME']
TARGET_FILE_SIZE = 8 * 1024 * 1024
SNAPSHOTS_NUM_TO_STORE = 2


spark = SparkSession \
    .builder \
    .appName("iceberg_maintenance") \
    .config("spark.ui.port", "4041")\
    .getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '10')

spark.sql(f"""
    CALL iceberg.system.expire_snapshots(
        table => '{TABLE_NAME}',
        retain_last => {SNAPSHOTS_NUM_TO_STORE}
    )
""")

spark.sql(f"""
    CALL iceberg.system.rewrite_data_files(
        table => '{TABLE_NAME}',
        options => map('target-file-size-bytes', '{TARGET_FILE_SIZE}')
    )
""")

spark.sql(f"""
    CALL iceberg.system.remove_orphan_files(
        table => '{TABLE_NAME}'
    )
""")

spark.stop()
