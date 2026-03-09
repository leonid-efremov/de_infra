import pendulum

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    'owner': 'MegaSuper_DE',
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'retries': 1
}


dag = DAG(
    dag_id="cryptocurrencies_project_dds_loading",
    default_args=default_args,
    schedule=None,
    description="Cryptocurrencies data union to one table",
    catchup=False,
    tags=['DE', 'cryptocurrencies_project', 'dds', 'spark', 'iceberg'],
)

start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

load_task = SparkSubmitOperator(
    task_id="union_cryptoasset_data",
    conn_id="spark_default",
    application="/opt/airflow/dags/ETL/spark-jobs/dds.py",
    deploy_mode="client",
    driver_memory="1G",
    num_executors=1,
    executor_cores=1,
    executor_memory="512m",
    spark_binary="/home/airflow/.local/bin/spark-submit",
    env_vars={
        'SPARK_HOME': "/home/airflow/.local/lib/python3.10/site-packages/pyspark"
    },
    jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.791.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
)


start_task >> load_task >> end_task
