import os
os.environ['SPARK_CONF_DIR'] = '/opt/spark/conf'
from pyspark.sql import SparkSession

from ETL.utils import PySparkTableLoader


spark = SparkSession \
    .builder \
    .appName("union_cryptoasset_data") \
    .config('spark.driver.cores', '1')\
    .config('spark.driver.memory', '1G')\
    .config('spark.executor.instances', '1')\
    .config('spark.executor.cores', '1')\
    .config('spark.executor.memory', '1G')\
    .getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '10')


DDS_MAX_DATE = spark.sql("SELECT COALESCE(MAX(dt), '2020-01-01') FROM iceberg.cryptocurrencies_project_dds.trade_data").collect()[0][0]

# dedub by business keys: asset name + business date
DDS_QUERY = f"""
    SELECT asset
        , dt
        , open_price
        , high_price
        , low_price
        , close_price
        , trading_volume
        , src_processed_dttm
        , processed_dttm
    FROM (
        SELECT *
            , ROW_NUMBER() OVER(PARTITION BY asset, dt ORDER BY src_processed_dttm DESC) AS rn
        FROM (
            SELECT 'BTC' AS asset
                    , dt
                    , open_price
                    , high_price
                    , low_price
                    , close_price
                    , trading_volume
                    , processed_dttm AS src_processed_dttm
                    , CURRENT_TIMESTAMP AS processed_dttm
            FROM iceberg.cryptocurrencies_project_raw.btc
            WHERE dt >= '{DDS_MAX_DATE}'
            UNION ALL
            SELECT 'ETH' AS asset
                    , dt
                    , open_price
                    , high_price
                    , low_price
                    , close_price
                    , trading_volume
                    , processed_dttm AS src_processed_dttm
                    , CURRENT_TIMESTAMP AS processed_dttm
            FROM iceberg.cryptocurrencies_project_raw.eth
            WHERE dt >= '{DDS_MAX_DATE}'
        ) AS united_data
    ) AS dedubled_data
    WHERE rn = 1
    ;
"""


dds_table_loader = PySparkTableLoader(
    table_name='iceberg.cryptocurrencies_project_dds.trade_data',
    stg_table_name='iceberg.cryptocurrencies_project_dds_stg.trade_data',
    partition_by=['asset', 'dt'],
    spark_session=spark,
)

dds_table_loader\
    .calc_stg(DDS_QUERY)\
    .load_trg_scd1(overwrite_trg=False)


spark.stop()
