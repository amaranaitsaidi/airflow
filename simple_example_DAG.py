# import library 

from airflow import DAG
from airglow.operators.bash_operator import BashOperator 
import datetime as dt 
# DA DD TD TP
# specify dag arguments as a python dict (the owner of dag,  start_date of dag, number of times it should keep trying if it is failing
# and the time to wait between subsequent tries.  

default_args = {
	'owner' : 'me',
	'start_date' : dt.datetime(2024,08,2024),
	'retries': 1,
	'retry_delay' : dt.timedelta(minutes=5),
}


# instatiating the dag object  (name of the dag, description, default arguments to apply to the DAG, scheduling instructions

dag = DAG('simple_example',
	  description = 'A simple example DAG', 
	 default_args = default_args
         schedule_interval=dt.timedelta(second=5) # the DAG will run repeatedly  on a schedule interval of five seconds one it is deployed
	)

# Task definition  

task1 = BashOperator(
	task_id='print_hello',
	bash_command= 'echo \' Greetings. The date and time are \'',
	dag=dag,
)

task2 = BashOperator(
	task_id='print_date',
	bash_command ='date',
	dag=dag, # task is assigned to the DAG we instantiate in the DAG definition block 
)


task1 >> task2




