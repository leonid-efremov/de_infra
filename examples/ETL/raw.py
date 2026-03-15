import pendulum
from time import sleep

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from ETL.src.data_collector import CryptoDataCollector
from ETL.src.dwh_entities import RAW_BTC_DATASET, RAW_ETH_DATASET


ASSETS_LIST = ['BTC', 'ETH']
STG_SCHEMA = 'iceberg.cryptocurrencies_project_raw'
TRG_SCHEMA = 'iceberg.cryptocurrencies_project_raw_stg'
TRG_ENTITIES = [RAW_BTC_DATASET, RAW_ETH_DATASET]


def load_cryptoasset_data(asset):
    sleep(3)

    crypto_data_collector = CryptoDataCollector(
        asset,
        f"{TRG_SCHEMA}.{asset.lower()}",
        f"{STG_SCHEMA}.{asset.lower()}",
    )

    result_row_cnt = crypto_data_collector.process()
    return result_row_cnt


default_args = {
    'owner': 'MegaSuper_DE',
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'retries': 1,
}


dag = DAG(
    dag_id="cryptocurrencies_project_raw_loading",
    default_args=default_args,
    schedule=None,
    description="Cryptocurrencies data loading from source API",
    catchup=False,
    tags=['DE', 'cryptocurrencies_project', 'raw', 'duckdb', 'iceberg'],
)

start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

load_task = PythonOperator.partial(
        task_id='load_cryptoasset_data',
        python_callable=load_cryptoasset_data,
	max_active_tis_per_dag=1,
    )\
    .expand(
        op_kwargs=[{'asset': i} for i in ASSETS_LIST],
    )

entities_update_task = EmptyOperator(task_id='entities_update', dag=dag, outlets=TRG_ENTITIES)

start_task >> load_task >> entities_update_task >> end_task
