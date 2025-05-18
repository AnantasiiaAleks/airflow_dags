'''
Создайте новый граф, добавьте в него два PythonOperator, первый должен 
выводить на экран сообщение “Hello from Airflow”, второй должен брать код 
из bash файла. Создайте bash файл который будет выводить сообщение 
“Hello from Airflow bash script processer.” Создайте последовательную связь 
между первым и вторым оператором. 
Добавьте между первым и вторым операторами еще один Operator 
который будет заканчивать свою работу в статусе skipped
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.exceptions import AirflowSkipException

default_args = {
    'owner': 'anantasiiaaleks',
    'start_date': days_ago(0),
    'depends_on_past': False,
}

def print_hello():
    print('Hello from Airflow')

def skipped():
    raise AirflowSkipException("This task is skipped intentionally")

with DAG(
    'hello_PythonOperators_DAG',
    description= 'Hello PythonOperators DAG',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    template_searchpath='dags/scripts/'
) as dag:

    task_1 = PythonOperator(
        task_id='python_from_airflow',
        python_callable=print_hello,
    )

    task_2 = PythonOperator(
        task_id='skipped_python',
        python_callable=skipped
    )

    task_3 = BashOperator(
    task_id='hello_from_file',
    bash_command='script1.sh',
    trigger_rule='all_done'
)
    
    # task_3 = BashOperator(
    #     task_id='from_python_file',
    #     bash_command='python file1.py',
    # )

task_1 >> task_2 >> task_3


