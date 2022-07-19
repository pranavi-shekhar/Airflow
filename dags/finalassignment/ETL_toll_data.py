# import the libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Pranavi Shekhar',
    'start_date': days_ago(0),
    'email': ['ps@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar zxvf /mnt/c/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -f 1-4 -d "," /mnt/c/dags/finalassignment/vehicle-data.csv > /mnt/c/dags/finalassignment/csv_data.csv',
    dag=dag,
)


extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 5-7 /mnt/c/dags/finalassignment/tollplaza-data.tsv | tr $"\t" "," | tr -d "\r" > /mnt/c/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59- /mnt/c/dags/finalassignment/payment-data.txt | tr " " "," > /mnt/c/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d, /mnt/c/dags/finalassignment/csv_data.csv /mnt/c/dags/finalassignment/tsv_data.csv /mnt/c/dags/finalassignment/fixed_width_data.csv > /mnt/c/dags/finalassignment/extracted_data.csv',
    dag=dag,
)


transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr [:lower:] [:upper:] < /mnt/c/dags/finalassignment/extracted_data.csv > /mnt/c/dags/finalassignment/transformed_data.csv',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data 
