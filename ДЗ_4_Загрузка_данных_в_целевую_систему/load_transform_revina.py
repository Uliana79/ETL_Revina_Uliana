from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from datetime import datetime
import logging

default_args = {
      'owner': 'revina_uliana',
}

dag = DAG('transform_revina',
          default_args=default_args,
          schedule_interval = timedelta(minutes=30),
          start_date = datetime(2025,1,1), 
          catchup = False, 
          tags=['postgres']        
)

def read_transform(**context):
    hook= PostgresHook(postgres_conn_id='demo_db')
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    df = pd.read_csv('/opt/airflow/data/IOT-temp.csv')
    df = df.drop_duplicates()
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    
    cursor.execute("""
            DROP TABLE IF EXISTS transform_revina.content, transform_revina.max_temp, transform_revina.min_temp, 
                   transform_revina.sort_in, transform_revina.procentili;
                                      
            CREATE TABLE transform_revina.content (
                id text PRIMARY KEY,
                room_id text,
                noted_date timestamp,
                temp_ integer,
                out_in text
            );
        
            CREATE TABLE transform_revina.max_temp (LIKE transform_revina.content INCLUDING ALL);
            CREATE TABLE transform_revina.min_temp (LIKE transform_revina.content INCLUDING ALL);
            CREATE TABLE transform_revina.sort_in (LIKE transform_revina.content INCLUDING ALL);
            CREATE TABLE transform_revina.procentili (LIKE transform_revina.content INCLUDING ALL);
        """)
    
    data_tuples = [tuple(x) for x in df.to_numpy()]
    insert_query = """
        INSERT INTO transform_revina.content (id, room_id, noted_date, temp_, out_in)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_tuples)
    

    df_temp_max = df.sort_values(by='temp',ascending=False).head(5)
    data_tuples = [tuple(x) for x in df_temp_max.to_numpy()]
    insert_query = """
        INSERT INTO transform_revina.max_temp (id, room_id, noted_date, temp_, out_in)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_tuples)


    df_temp_min = df.sort_values(by='temp',ascending=True).head(5)
    data_tuples = [tuple(x) for x in df_temp_min.to_numpy()]
    insert_query = """
        INSERT INTO transform_revina.min_temp (id, room_id, noted_date, temp_, out_in)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_tuples)


    df_sort_in = df[df['out/in']=="In"]
    data_tuples = [tuple(x) for x in df_sort_in.to_numpy()]
    insert_query = """
        INSERT INTO transform_revina.sort_in (id, room_id, noted_date, temp_, out_in)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_tuples)


    lower_bound = df['temp'].quantile(0.05)
    upper_bound = df['temp'].quantile(0.95)
    df_procentili = df[(df['temp']>= lower_bound) & (df['temp'] <= upper_bound)]
    data_tuples = [tuple(x) for x in df_procentili.to_numpy()]
    insert_query = """
        INSERT INTO transform_revina.procentili (id, room_id, noted_date, temp_, out_in)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_tuples)

    connection.commit()
    cursor.close()
    connection.close()


def load_all(**context):
    hook= PostgresHook(postgres_conn_id='demo_db')
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    df = pd.read_csv('/opt/airflow/data/IOT-temp.csv')
    df = df.drop_duplicates()
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    cursor.execute("""
        TRUNCATE TABLE transform_revina.content RESTART IDENTITY;
    """)
    data_tuples = [tuple(x) for x in df.to_numpy()]
    insert_query = """
        INSERT INTO transform_revina.content (id, room_id, noted_date, temp_, out_in)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_tuples)
    cursor.execute("SELECT COUNT(*) FROM transform_revina.content")
    logging.info(f"Вставлено записей в основную таблицу при полной загрузке: {cursor.fetchone()[0]}")
    connection.commit()
    cursor.close()
    connection.close()

def load_increment(**context):
    hook= PostgresHook(postgres_conn_id='demo_db')
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    df = pd.read_csv('/opt/airflow/data/IOT-temp.csv')
    df = df.drop_duplicates()
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    
    new_data = df[df['noted_date'] > datetime.now() - timedelta(days=5)]

    if len(new_data) == 0:
        cursor.close()
        connection.close()
        logging.info(f"Нет данных для загрузки")
        return 1
    
        
    data_tuples = [tuple(x) for x in df.to_numpy()]
    insert_query = """
        INSERT INTO transform_revina.content (id, room_id, noted_date, temp_, out_in)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """
    cursor.executemany(insert_query, data_tuples)

    logging.info(f"Вставлено записей в основную таблицу при частичной загрузке: {len(new_data)}")

    connection.commit()
    cursor.close()
    connection.close()

transform_data= PythonOperator(
    task_id='read_transform',
    python_callable=read_transform,
    dag=dag
)

load_all_data= PythonOperator(
    task_id='load_all',
    python_callable=load_all,
    dag=dag
)

load_increment_data= PythonOperator(
    task_id='load_increment',
    python_callable=load_increment,
    dag=dag
)

transform_data>>load_all_data>>load_increment_data