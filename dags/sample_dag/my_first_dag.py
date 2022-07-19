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
    'owner': 'Pranavi Shekhar',
    'start_date': days_ago(0),
    'email': ['pranavishekhar@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='my_first_dag',
    default_args=default_args,
    description='My first ETL DAG using Bash',
    schedule_interval=timedelta(minutes=5),
)

# define the tasks

# define the first task named extract
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1 /mnt/c/dags/sample_dag/input.txt > /mnt/c/dags/sample_dag/extracted.txt',
    dag=dag,
)


# define the second task named transform and load
transform_and_load = BashOperator(
    task_id='transform_and_load',
    bash_command='tr " " "," < /mnt/c/dags/sample_dag/extracted.txt > /mnt/c/dags/sample_dag/transform_and_load.txt',
    dag=dag,
)


# task pipeline
extract >> transform_and_load