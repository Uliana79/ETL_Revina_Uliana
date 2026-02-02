from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd


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


transform_data= PythonOperator(
    task_id='read_transform',
    python_callable=read_transform,
    dag=dag
)

transform_data