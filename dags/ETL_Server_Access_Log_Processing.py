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
    dag_id='ETL_Server_Access_Log_processing',
    default_args=default_args,
    description='DAG to understand basic ETL operations using bash',
    schedule_interval=timedelta(minutes=5),
)

# define the tasks

# define the first task named extract
download = BashOperator(
    task_id='download',
    bash_command='wget -O /mnt/c/dags/log_file.txt https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt',
    dag=dag,
)

extract = BashOperator(
    task_id='extract',
    bash_command='cut -f 1,4 -d "#" /mnt/c/dags/log_file.txt > /mnt/c/dags/extracted.txt',
    dag=dag,
)

transform = BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]" < /mnt/c/dags/extracted.txt > /mnt/c/dags/transformed.txt',
    dag=dag,
)


load = BashOperator(
    task_id='load',
    bash_command='zip /mnt/c/dags/output.zip /mnt/c/dags/transformed.txt' ,
    dag=dag,
)

# Define order of tasks (task pipeline)

download >> extract >> transform >> load