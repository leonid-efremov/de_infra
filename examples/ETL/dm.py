import pendulum

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from ETL.src.dwh_entities import DM_ANALYTIC_INDICATORS_DATASET, DDS_TRADE_DATA_DATASET


SRC_ENTITIES = [DDS_TRADE_DATA_DATASET]
TRG_ENTITIES = [DM_ANALYTIC_INDICATORS_DATASET]


default_args = {
    'owner': 'MegaSuper_DE',
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'retries': 1
}

parameters = dict(
    spark_app="/opt/airflow/dags/ETL/src/spark-job-dm.py",
    rewrite_trg="",
    report_dt="current_date", # "'2026-01-01'"
)


dag = DAG(
    dag_id="cryptocurrencies_project_dm_loading",
    default_args=default_args,
    schedule=SRC_ENTITIES,
    description="Cryptocurrencies data analytics",
    catchup=False,
    tags=['DE', 'cryptocurrencies_project', 'dm', 'spark', 'iceberg'],
    params=parameters,
)

start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

load_task = SparkSubmitOperator(
    task_id="calc_analytic_indicators",
    conn_id="spark_default",
    application=dag.params.get("spark_app"),
    deploy_mode="client",
    driver_memory="1G",
    num_executors=1,
    executor_cores=1,
    executor_memory="512m",
    spark_binary="/home/airflow/.local/bin/spark-submit",
    properties_file='/opt/spark/conf/spark-defaults.conf',
    env_vars={
        'SPARK_HOME': "/home/airflow/.local/lib/python3.10/site-packages/pyspark",
	'PYTHONPATH': '/opt/airflow/dags',
        'REWRITE_TRG': dag.params.get("rewrite_trg"),
        'REPORT_DT': dag.params.get("report_dt"),
    },
    jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.791.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
    outlets=TRG_ENTITIES,
)

start_task >> load_task >> end_task
