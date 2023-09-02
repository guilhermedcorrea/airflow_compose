

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'databricks_example',
    default_args=default_args,
    description='Example DAG to run a Databricks job',
    schedule_interval=None,
    catchup=False,
    tags=['example']
)

databricks_operator = DatabricksSubmitRunOperator(
    task_id='run_databricks_job',
    databricks_conn_id='databricks_default', 
    job_id='12345',  
    new_cluster={
        'spark_version': '7.3.x-scala2.12',
        'node_type_id': 'Standard_D3_v2',
        'num_workers': 1,
    },
    notebook_task={
        'notebook_path': '/path/to/your/notebook',  
    },
    dag=dag,
)

databricks_operator
