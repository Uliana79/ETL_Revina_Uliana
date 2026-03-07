from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
      'owner': 'revina_uliana',
}

dag = DAG('user_activity_revina',
    default_args=default_args,
    schedule_interval = timedelta(minutes=30),
    start_date = datetime(2025,1,1), 
    catchup = False, 
    tags=['postgres','user_activity']        
)

def user_activity(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
            DROP TABLE IF exists UserActivity CASCADE;
            CREATE TABLE UserActivity (
                user_id VARCHAR(50) NOT NULL,
                sessions_count INTEGER DEFAULT 0,
                total_duration_minutes INTEGER DEFAULT 0,
                pages_visited TEXT[],
                actions_performed TEXT[],
                last_active TIMESTAMP,
                PRIMARY KEY (user_id)
            );

            INSERT INTO UserActivity (user_id, sessions_count, total_duration_minutes, pages_visited, actions_performed, last_active)
            SELECT 
                us.user_id,
                COUNT(DISTINCT us.session_id) as sessions_count,
                SUM(EXTRACT(EPOCH FROM (us.end_time - us.start_time))/60)::INTEGER as total_duration_minutes,
                ARRAY_AGG(DISTINCT sp.page_url) FILTER (WHERE sp.page_url IS NOT NULL) as all_pages_visited,
                ARRAY_AGG(DISTINCT sa.action_name) FILTER (WHERE sa.action_name IS NOT NULL) as all_actions_performed,
                MAX(us.end_time) as last_active_time
            FROM UserSessions us
            LEFT JOIN SessionPages sp ON us.session_id = sp.session_id
            LEFT JOIN SessionActions sa ON us.session_id = sa.session_id
            GROUP BY us.user_id
            ORDER BY sessions_count DESC;           
        """)

        connection.commit()
        cursor.close()
        connection.close()
        
        logging.info(f"user_activity done!")
        return True
    except Exception as e:
        logging.error(f"user_activity with error: {e}")
        return False


def top_pages(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
            DROP TABLE IF exists TopPages CASCADE;
            CREATE TABLE TopPages (
                page_url VARCHAR(255),
                views INTEGER
            );

            INSERT INTO TopPages (page_url, views)
            SELECT page_url, COUNT(*) as views
            FROM SessionPages
            GROUP BY page_url
            ORDER BY views DESC
            LIMIT 5;           
        """)

        connection.commit()
        cursor.close()
        connection.close()
        
        logging.info(f"top_pages done!")
        return True
    except Exception as e:
        logging.error(f"top_pages with error: {e}")
        return False


def top_actions(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
            DROP TABLE IF exists TopActions CASCADE;
            CREATE TABLE TopActions (
                action_name VARCHAR(255),
                actions_count INTEGER
            );

            INSERT INTO TopActions (action_name, actions_count)
            SELECT action_name, COUNT(*) as actions_count
            FROM SessionActions 
            GROUP BY action_name
            ORDER BY actions_count DESC
            LIMIT 5;          
        """)

        connection.commit()
        cursor.close()
        connection.close()
        
        logging.info(f"top_actions done!")
        return True
    except Exception as e:
        logging.error(f"top_actions with error: {e}")
        return False
    

activity = PythonOperator(
    task_id='user_activity',
    python_callable=user_activity,
    dag=dag
) 

pages = PythonOperator(
    task_id='top_pages',
    python_callable=top_pages,
    dag=dag
)

actions = PythonOperator(
    task_id='top_actions',
    python_callable=top_actions,
    dag=dag
)

activity>>pages>>actions