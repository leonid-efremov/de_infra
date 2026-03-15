import os
os.environ['SPARK_CONF_DIR'] = '/opt/spark/conf'
from pyspark.sql import SparkSession

from ETL.utils import PySparkTableLoader


spark = SparkSession \
    .builder \
    .appName("calc_analytic_indicators") \
    .config("spark.ui.port", "4041")\
    .getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '10')


REPORT_DT = str(os.environ['REPORT_DT'])
REWRITE_TRG = bool(os.environ['REWRITE_TRG'])

# not the best calculation methodology)
DM_QUERY = f"""
    SELECT asset
        , {REPORT_DT} AS report_dt
        , current_price
        , ROUND(100 * (2 - (high_price_exactly_7d + low_price_exactly_7d) / current_price) , 2) AS div_percent_7d
        , ROUND(100 * (2 - (high_price_exactly_30d + low_price_exactly_30d) / current_price) , 2) AS div_percent_30d
        , ROUND(100 * (2 - (high_price_exactly_90d + low_price_exactly_90d) / current_price) , 2) AS div_percent_90d
        , ROUND(100 * (2 - (high_price_exactly_1y + low_price_exactly_1y) / current_price) , 2) AS div_percent_1y
        , ROUND(100 * (high_price_7d / low_price_7d), 2) AS volatility_percent_7d
        , ROUND(100 * (high_price_30d / low_price_30d), 2) AS volatility_percent_30d
        , ROUND(100 * (high_price_90d / low_price_90d), 2) AS volatility_percent_90d
        , ROUND(100 * (high_price_1y / low_price_1y), 2) AS volatility_percent_1y
        , high_price_7d
        , high_price_30d
        , high_price_90d
        , high_price_1y   
        , low_price_7d
        , low_price_30d
        , low_price_90d
        , low_price_1y
        , med_price_7d
        , med_price_30d
        , med_price_90d
        , med_price_1y
        , src_processed_dttm
        , CURRENT_TIMESTAMP AS processed_dttm
    FROM (
        SELECT hist_data.asset
            , MAX(current_data.current_price) AS current_price
            , MAX(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 7 days AND {REPORT_DT}
                    THEN hist_data.high_price
                    ELSE 0
            END) AS high_price_7d
            , MAX(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 30 days AND {REPORT_DT}
                    THEN hist_data.high_price
                    ELSE 0
            END) AS high_price_30d
            , MAX(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 90 days AND {REPORT_DT}
                    THEN hist_data.high_price
                    ELSE 0
            END) AS high_price_90d
            , MAX(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 1 year AND {REPORT_DT}
                    THEN hist_data.high_price
                    ELSE 0
            END) AS high_price_1y
            , MIN(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 7 days AND {REPORT_DT}
                    THEN hist_data.low_price
                    ELSE 0
            END) AS low_price_7d
            , MIN(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 30 days AND {REPORT_DT}
                    THEN hist_data.low_price
                    ELSE 0
            END) AS low_price_30d
            , MIN(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 90 days AND {REPORT_DT}
                    THEN hist_data.low_price
                    ELSE 0
            END) AS low_price_90d
            , MIN(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 1 year AND {REPORT_DT}
                    THEN hist_data.low_price
                    ELSE 0
            END) AS low_price_1y
            , AVG(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 7 days AND {REPORT_DT}
                    THEN (hist_data.high_price + hist_data.low_price) / 2
                    ELSE 0
            END) AS med_price_7d
            , AVG(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 30 days AND {REPORT_DT}
                    THEN (hist_data.high_price + hist_data.low_price) / 2
                    ELSE 0
            END) AS med_price_30d
            , AVG(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 90 days AND {REPORT_DT}
                    THEN (hist_data.high_price + hist_data.low_price) / 2
                    ELSE 0
            END) AS med_price_90d
            , AVG(CASE
                WHEN hist_data.dt BETWEEN {REPORT_DT} - interval 1 year AND {REPORT_DT}
                    THEN (hist_data.high_price + hist_data.low_price) / 2
                    ELSE 0
            END) AS med_price_1y
            , MAX(CASE
                WHEN hist_data.dt = {REPORT_DT} - interval 7 days
                    THEN hist_data.high_price
                    ELSE 0
            END) AS high_price_exactly_7d
            , MAX(CASE
                WHEN hist_data.dt = {REPORT_DT} - interval 30 days
                    THEN hist_data.high_price
                    ELSE 0
            END) AS high_price_exactly_30d
            , MAX(CASE
                WHEN hist_data.dt = {REPORT_DT} - interval 90 days
                    THEN hist_data.high_price
                    ELSE 0
            END) AS high_price_exactly_90d
            , MAX(CASE
                WHEN hist_data.dt = {REPORT_DT} - interval 1 year
                    THEN hist_data.high_price
                    ELSE 0
            END) AS high_price_exactly_1y
            , MIN(CASE
                WHEN hist_data.dt = {REPORT_DT} - interval 7 days
                    THEN hist_data.low_price
                    ELSE 0
            END) AS low_price_exactly_7d
            , MIN(CASE
                WHEN hist_data.dt = {REPORT_DT} - interval 30 days
                    THEN hist_data.low_price
                    ELSE 0
            END) AS low_price_exactly_30d
            , MIN(CASE
                WHEN hist_data.dt = {REPORT_DT} - interval 90 days
                    THEN hist_data.low_price
                    ELSE 0
            END) AS low_price_exactly_90d
            , MIN(CASE
                WHEN hist_data.dt = {REPORT_DT} - interval 1 year
                    THEN hist_data.low_price
                    ELSE 0
            END) AS low_price_exactly_1y
            , MAX(hist_data.src_processed_dttm) AS src_processed_dttm
            , CURRENT_TIMESTAMP AS processed_dttm
        FROM iceberg.cryptocurrencies_project_dds.trade_data AS hist_data
        INNER JOIN (
            SELECT asset, MAX(low_price - high_price) / 2 AS current_price
            FROM iceberg.cryptocurrencies_project_dds.trade_data
            WHERE dt = {REPORT_DT}
            GROUP BY asset
        ) AS current_data
            ON hist_data.asset = current_data.asset
        WHERE hist_data.dt BETWEEN {REPORT_DT} - interval 1 year AND {REPORT_DT}
        GROUP BY hist_data.asset
    ) AS prep
    ;
"""

dm_table_loader = PySparkTableLoader(
    table_name='iceberg.cryptocurrencies_project_dm.analytic_indicators',
    stg_table_name='iceberg.cryptocurrencies_project_dm_stg.analytic_indicators',
    partition_by=['asset', 'report_dt'],
    spark_session=spark,
)

dm_table_loader \
    .calc_stg(DM_QUERY)\
    .load_trg_scd1(overwrite_trg=REWRITE_TRG)


spark.stop()
