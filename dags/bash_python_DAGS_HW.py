'''
1. Создайте новый граф. Добавьте в него BashOperator, 
который будет генерировать рандомное число и печатать его в
консоль.
2. Создайте PythonOperator, который генерирует рандомное число, 
возводит его в квадрат и выводит в консоль исходное число и результат.
3. Сделайте оператор, который отправляет запрос к
 https://goweather.herokuapp.com/weather/"location" (вместо location используйте ваше местоположение).
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from random import randint
import requests

default_args = {
    'owner': 'anantasiiaaleks',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def pow_random():
    n = randint(0, 32767)
    return f'{n} ^ 2 = {n ** 2}'

def get_weather():
    location = "bratislava" 
    url = f"https://goweather.herokuapp.com/weather/{location}"
    response = requests.get(url)
    if response.status_code == 200:
        weather_data = response.json()
        print(weather_data)
    else:
        print("Ошибка при запросе данных о погоде")


with DAG(
    'bash_python_dags_hw',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    template_searchpath='dags/scripts/'
) as dag:
    
    task_1 = BashOperator(
        task_id='generate_and_print_random',
        bash_command='echo $RANDOM',
    )

    task_2 = PythonOperator(
        task_id='pow_random',
        python_callable=pow_random
    )

    task_3 = PythonOperator(
        task_id='get_weather_task',
        python_callable = get_weather,
)




task_1 >> task_2 >> task_3 
