'''
 Создайте ShortCircuitOperator и измените граф таким образом чтобы 
было видно как выполняются два различных сценария в 
зависимости от результатов работы данного оператора. 
'''

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.http_sensor import HttpSensor

def visit_gb():
    return True
 
def print_hello():
    return 'Hello world from Airflow DAG!'

def skipp():
    return 99


dag = DAG(
    'shortCircuit_DAG',
    description='Hello World DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    template_searchpath='dags/scripts/'
    )

hello_operator = PythonOperator(
   task_id='hello_task', 
   python_callable=print_hello, 
   dag=dag
   )

skipp_operator = PythonOperator(
   task_id='skip_task', 
   python_callable=skipp, 
   dag=dag
   )
hello_file_operator = BashOperator(
   task_id='hello_file_task', 
   bash_command='script1.sh', 
   dag=dag
   )

task_http_sensor_check = HttpSensor(
   task_id="http_sensor_check",
   http_conn_id="gb_ru_connection",
   endpoint="",
   request_params={},
   response_check=lambda response:"httpbin" in response.text,
   poke_interval=5,
   dag=dag,
   )

visit_site = ShortCircuitOperator(
    task_id='visit_gb',
    python_callable=visit_gb,
    dag=dag)

hello_operator >> skipp_operator >> hello_file_operator >> visit_site >> task_http_sensor_check