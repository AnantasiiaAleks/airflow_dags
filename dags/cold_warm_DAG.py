'''
— Зарегистрируйтесь в ОрепWeatherApi (https://openweathermap.org/api)
— Создайте ETL, который получает температуру в заданной вами локации, и
дальше делает ветвление:

• В случае, если температура больше 15 градусов цельсия — идёт на ветку, в которой есть оператор, выводящий на
экран «тепло»;
• В случае, если температура ниже 15 градусов, идёт на ветку с оператором, который выводит в консоль «холодно».

Оператор ветвления должен выводить в консоль полученную от АРI температуру.

— Приложите скриншот графа и логов работы оператора ветвленния.
'''

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
import requests

API_KEY = '28bb22fe4d94d7d3579d1d602e34d8302011'
LOCATION = 'Moscow'

def get_temperature(**context):
    """Получаем температуру"""
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Moscow,ru&APPID={API_KEY}&units=metric'
    params = {'apikey': API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    temperature_c = data['main']['temp']
    print(f"Температура в {LOCATION}: {temperature_c}°C")
    context['ti'].xcom_push(key='temperature', value=temperature_c)

def branching(**context):
    """Ветвление в зависимости от температуры"""
    ti = context['ti']
    temp = ti.xcom_pull(key='temperature', task_ids='get_temperature')
    print(f"Температура получена: {temp}°C")
    if temp > 15:
        return 'warm'
    else:
        return 'cold'

def warm():
    print("тепло")

def cold():
    print("холодно")

with DAG('temperature_etl', start_date=days_ago(1), schedule_interval=None, catchup=False) as dag:

    get_temperature = PythonOperator(
        task_id='get_temperature',
        python_callable=get_temperature,
        provide_context=True
    )

    branch = BranchPythonOperator(
        task_id='branching',
        python_callable=branching,
        provide_context=True
    )

    warm = PythonOperator(
        task_id='warm',
        python_callable=warm
    )

    cold = PythonOperator(
        task_id='cold',
        python_callable=cold
    )

    get_temperature >> branch >> [warm, cold]
