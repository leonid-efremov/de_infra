import pendulum

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from ETL.src.dwh_entities import ALL_ENTITIES


TABLE_NAMES_LIST = [ds.extra["table_name"] for ds in ALL_ENTITIES]


default_args = {
    'owner': 'MegaSuper_DE',
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'retries': 1
}

parameters = dict(
    spark_app="/opt/airflow/dags/ETL/src/spark-job-maintenance.py",
)

def make_env_4table(table_name):
    return {
        'SPARK_HOME': "/home/airflow/.local/lib/python3.10/site-packages/pyspark",
	'PYTHONPATH': '/opt/airflow/dags',
	'TABLE_NAME': table_name,
    }


dag = DAG(
    dag_id="iceberg_maintenance",
    default_args=default_args,
    schedule=None,
    description="Iceberg tables maintenance",
    catchup=False,
    tags=['DE', 'cryptocurrencies_project', 'maintenance', 'spark', 'iceberg'],
    params=parameters,
)

start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

maintenance_task = SparkSubmitOperator.partial(
    task_id="do_maintenance",
    conn_id="spark_default",
    application=dag.params.get("spark_app"),
    deploy_mode="client",
    driver_memory="1G",
    num_executors=1,
    executor_cores=1,
    executor_memory="1G",
    spark_binary="/home/airflow/.local/bin/spark-submit",
    properties_file='/opt/spark/conf/spark-defaults.conf',
    jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.791.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
).expand(
    env_vars=[make_env_4table(i) for i in TABLE_NAMES_LIST],
)

start_task >> maintenance_task >> end_task


