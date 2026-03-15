from airflow.datasets import Dataset


RAW_BTC_DATASET = Dataset(
    uri="iceberg://iceberg.cryptocurrencies_project_raw.btc",
    extra={"table_name": "iceberg.cryptocurrencies_project_raw.btc", "layer": "raw"},
)
RAW_ETH_DATASET = Dataset(
    uri="iceberg://iceberg.cryptocurrencies_project_raw.eth",
    extra={"table_name": "iceberg.cryptocurrencies_project_raw.eth", "layer": "raw"},
)
DDS_TRADE_DATA_DATASET = Dataset(
    uri="iceberg://iceberg.cryptocurrencies_project_dds.trade_data",
    extra={"table_name": "iceberg.cryptocurrencies_project_dds.trade_data", "layer": "dds"},
)
DM_ANALYTIC_INDICATORS_DATASET = Dataset(
    uri="iceberg://iceberg.cryptocurrencies_project_dm.analytic_indicators",
    extra={"table_name": "iceberg.cryptocurrencies_project_dm.analytic_indicators", "layer": "dm"},
)

ALL_ENTITIES = [RAW_BTC_DATASET, RAW_ETH_DATASET, DDS_TRADE_DATA_DATASET, DM_ANALYTIC_INDICATORS_DATASET]
