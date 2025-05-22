'''
1. Скачайте файлы boking.csv, client.csv и hotel.csv;
2. Создайте новый dag;
3. Создайте три оператора для получения данных и загрузите файлы. Передайте дата фреймы в оператор трансформации;
4. Создайте оператор который будет трансформировать данные:
— Объедините все таблицы в одну;
— Приведите даты к одному виду;
— Удалите невалидные колонки;
— Приведите все валюты к одной;
5. Создайте оператор загрузки в базу данных;
6. Запустите dag.'''

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# пути до файлов
BOOKING_PATH = '/opt/airflow/data/booking.csv'
CLIENT_PATH = '/opt/airflow/data/client.csv'
HOTEL_PATH = '/opt/airflow/data/hotel.csv'

# условные курсы валют
CURENCY_RATES = {
    'RUB': 1,
    'USD': 70,
    'EUR': 100,
}

default_args = {
    'owner': 'anantasiiaaleks',
    'start_date': datetime(2024, 1, 1),
    'depends_of_past': False,
}

dag = DAG(
    'hotel_booking_etl',
    default_args=default_args,
    schedule_interval=None
)

# функции чтения файлов
def load_booking(**context):
    try:
        df = pd.read_csv(BOOKING_PATH)
        context['ti'].xcom_push(key='booking_df', value=df.to_json())
    except Exception as e:
        raise ValueError(f"Ошибка при загрузке booking.csv: {e}")

def load_client(**context):
    try:
        df = pd.read_csv(CLIENT_PATH)
        context['ti'].xcom_push(key='client_df', value=df.to_json())
    except Exception as e:
        raise ValueError(f"Ошибка при загрузке client.csv: {e}")

def load_hotel(**context):
    try:
        df = pd.read_csv(HOTEL_PATH)
        context['ti'].xcom_push(key='hotel_df', value=df.to_json())
    except Exception as e:
        raise ValueError(f"Ошибка при загрузке hotel.csv: {e}")

# трансформация
def transform(**context):

    booking = pd.read_json(context['ti'].xcom_pull(key='booking_df', task_ids='load_booking'))
    client = pd.read_json(context['ti'].xcom_pull(key='client_df', task_ids='load_client'))
    hotel = pd.read_json(context['ti'].xcom_pull(key='hotel_df', task_ids='load_hotel'))

    # объединение таблиц
    df = booking.merge(client, on='client_id', how='left')\
                .merge(hotel, on='hotel_id', how='left')
    
    # даты к одному виду
    df['booking_date'] = pd.to_datetime(df['booking_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # удаление невалидных строк
    df = df.dropna(axis=1)

    # приведение валют к одной (руб)
    def convert_money(row):
        rate = CURENCY_RATES.get(row['currency'], 1)
        return row['booking_cost'] * rate
    
    df['booking_cost_rub'] = df.apply(convert_money, axis=1)
    df = df.drop(columns=['booking_cost'])

    context['ti'].xcom_push(key='transformed_df', value=df)

# загрузка в БД
def load_to_db(**context):
    df = context['ti'].xcom_pull(key='transformed_df', task_ids='transform')

    user = 'airflow'
    password = 'airflow'
    host = 'postgres'
    port = '5432'
    database = 'airflow'

    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

    df.to_sql('hotel_bookings', engine, if_exists='replace', index=False)

    engine.dispose()

# Airflow-операторы
load_booking_task = PythonOperator(
    task_id='load_booking',
    python_callable=load_booking,
    provide_context=True,
    dag=dag
)

load_client_task = PythonOperator(
    task_id='load_client',
    python_callable=load_client,
    provide_context=True,
    dag=dag
)

load_hotel_task = PythonOperator(
    task_id='load_hotel',
    python_callable=load_hotel,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

load_db_task = PythonOperator(
    task_id='load_to_db',
    python_callable=load_to_db,
    provide_context=True,
    dag=dag
)


# Последовательность задач
[load_booking_task, load_client_task, load_hotel_task] >> transform_task >> load_db_task
