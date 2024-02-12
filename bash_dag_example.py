# I DD DA DT TP 

from datetime import timedelta


# importer le DAG object 
from airflow import DAG 

# importer operators, pour écrire des tasks
from airflow.operators.bash_operator import BashOperatorr

# permet de mieux gérer la programmation de temps 
from airflow.utils.dates import days_ago 


# definition des arguments 

defaults_args = {
    'owner': 'Ramesh Sannareddy', 
    'start_day' : days_ago(0),
    'email': ['ramesh@somemail.com'],
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# definition du DAG

dag = DAG(
    'my-first-dag',
    defaults_args= defaults_args,
    description = 'My first DAG',
    schedule_interval=timedelta(days=1),
)

# definition des task 

# definition de la premiere task 

extract = BashOperatorr(
    task_id='extract',
    bash_command= 'cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/extracted-data.txt',
    dag=dag
)

# define second task 

transformt_and_load = BashOperatorr(
    task_id='transform',
    bash_command='tr ":" "," < /home/project/airflow/extracted-data.txt > /home/project/airflow/transformed-data.csv '
    dag=dag
)


# task pipeline 

extract >> transformt_and_load

