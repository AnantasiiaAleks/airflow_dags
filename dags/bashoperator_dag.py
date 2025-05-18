'''
 Создайте новый граф, добавьте в него два BashOperator, первый должен 
выводить на экран сообщение “Hello from Airflow”, второй должен брать код 
из bash файла. Создайте bash файл который будет выводить сообщение 
“Hello from Airflow bash script processer.” Создайте последовательную связь 
между первым и вторым оператором. 
Добавьте между первым и вторым операторами еще один BashOperator 
который будет заканчивать свою работу в статусе skipped
'''
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'owner': 'anantasiiaaleks',
    'start_date': days_ago(0),
    # 'start_date': datetime(2025, 5, 18)
    'depends_on_past': False,       # при запуске задним числом (backfill) дает запустить DAG в не зависимости от статуса предыдущего запуска
}

with DAG(
    'hello_BashOperators_DAG',
    description= 'Hello BashOperators DAG',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    template_searchpath='dags/scripts/'
) as dag:

    task_1 = BashOperator(
        task_id='echo_hi_from_airflow',
        bash_command='echo "Hello from Airflow"',
    )

    task_2 = BashOperator(
        task_id='skipped_task',
        bash_command='exit 99'
    )

    task_3 = BashOperator(
    task_id='hello_from_file',
    bash_command='script1.sh',
    trigger_rule='all_done'
)

    task_1 >> task_2 >> task_3


#  from datetime import datetime
#  from airflow import DAG
#  from airflow.operators import BashOperator

#  dag = DAG( 'hello_world' , description= 'Hello World DAG' ,
#  schedule_interval= '0 12 * * *' ,
#  start_date=datetime( 2023 , 1 , 1
#  ), catchup= False )
#  hello_operator = BashOperator(task_id= 'hello_task' , bash_command='echo Hello from Airflow', dag=dag)
#  skipp_operator = BashOperator(task_id= 'skip_task' , bash_command='exit 99', dag=dag)
#  hello_file_operator = BashOperator(task_id= 'hello_file_task' ,
#  bash_command='./home/airflow/airflow/dags/scripts/file1.sh', dag=dag)
#  hello_operator >> skipp_operator >> hello_file_operator
