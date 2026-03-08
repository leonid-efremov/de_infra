from airflow.datasets import Dataset


RAW_BTC_DATASET = Dataset(uri="iceberg://iceberg.cryptocurrencies_project_raw.btc")
RAW_ETH_DATASET = Dataset(uri="iceberg://iceberg.cryptocurrencies_project_raw.eth")
