import pendulum

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator


default_args = {
    'owner': 'MegaSuper_DE',
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'retries': 1
}

dag = DAG(
    dag_id="test_dag",
    default_args=default_args,
    schedule=None,
    description="Simple Test Dag",
    catchup=False,
    tags=['DE', 'test']
)

test_task = EmptyOperator(task_id='test_task', dag=dag)

test_task
