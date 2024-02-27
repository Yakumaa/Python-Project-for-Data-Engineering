# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Shrish Maharjan',
    'start_date': days_ago(0),
    'email': ['maharjanshrish8@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

# define the tasks
extract_tranform_load = BashOperator(
    task_id="extract_transform_load",
    bash_command="/home/shrish/airflow/dags/ETL_Server_Access_Log_Processing.sh ",
    dag=dag,
)

# task pipeline
extract_tranform_load