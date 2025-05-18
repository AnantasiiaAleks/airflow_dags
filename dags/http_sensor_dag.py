'''
Добавьте в граф httpSensor который будет обращаться к сайту gb.ru.
 Отправьте в чат скриншот кода и логи работы
'''


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'anantasiiaaleks',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print('Hello from Airflow')

def skipped():
    return 99

with DAG(
    'http_sensor_gb_ru',
    default_args=default_args,
    schedule_interval='@daily',
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
)

    check_gb_ru = HttpSensor(
        task_id='check_gb_ru_site',
        http_conn_id='gb_ru_connection',
        endpoint='',
        request_params={},
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=60,
        mode='poke'
    )



task_1 >> task_2 >> task_3 >> check_gb_ru


#     task_http_sensor_check = HttpSensor(
#  task_id="http_sensor_check",
#  http_conn_id="http_default",
#  endpoint="",
#  request_params={},
#  response_check=lambda response: "httpbin" in response.text,
#  poke_interval=5,
#  dag=dag,
#  )
#  hello_operator >> skipp_operator >> hello_file_operator >> task_http_sensor_check
