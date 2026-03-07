from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import json

default_args = {
      'owner': 'revina_uliana',
}

dag = DAG('ETL_revina',
    default_args=default_args,
    schedule_interval = timedelta(minutes=30),
    start_date = datetime(2025,1,1), 
    catchup = False, 
    tags=['postgres','mongodb', 'etl']        
)

def clean_dataframe(df, date=None, duplicates=None):
    if date:
        for col in date:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
    
    if duplicates:
        df = df.drop_duplicates(subset=duplicates)
        
    return df


def etl_users(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("SELECT MAX(created_at) FROM Users")
        result = cursor.fetchone()
        last_max_date = result[0] if result[0] else datetime(2024, 1, 1)

        logging.info(f"max data = {last_max_date}")
        
        hook_mongo = MongoHook('mongo_default')
        client = hook_mongo.get_conn()
        db = client.service
               
        users = pd.DataFrame(list(db.Users.find({"registration_date": {"$gt": str((last_max_date+timedelta(microseconds=1000)).isoformat())}})))
        if users.empty:
            logging.info("no data for etl")
            return True

        users = users.drop('_id', axis=1)
        users = clean_dataframe(users, ['registration_date', 'last_active'], ['user_id'])

        data_tuples = [tuple(x) for x in users.to_numpy()]
        insert_query = """
            INSERT INTO Users (user_id, email, name, registration_date, last_active, is_active)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
        """
        cursor.executemany(insert_query, data_tuples)

    
        connection.commit()
        cursor.close()
        connection.close()
        client.close()
        logging.info(f"etl_users done!")
        return True
    except Exception as e:
        logging.error(f"etl_users with error: {e}")
        return False


def etl_user_sessions(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("SELECT MAX(created_at) FROM UserSessions")
        result = cursor.fetchone()
        last_max_date = result[0] if result[0] else context['dag'].start_date
        logging.info(f"max data = {last_max_date}")

        hook_mongo = MongoHook('mongo_default')
        client = hook_mongo.get_conn()
        db = client.service
               
        sessions = pd.DataFrame(list(db.UserSessions.find({"start_time": {"$gt": str((last_max_date+timedelta(microseconds=1000)).isoformat())}})))
        if sessions.empty:
            logging.info("no data for etl")
            return True

        sessions = sessions.drop('_id', axis=1)

        sessions = clean_dataframe(sessions, ['start_time', 'end_time'], ['session_id'])
        sessions = sessions[(sessions['end_time']>=sessions['start_time']) | (sessions['end_time'].isna()) | (sessions['start_time'].isna())]

        
        page = pd.DataFrame(sessions[['session_id', 'pages_visited']])
        page = page.explode('pages_visited', ignore_index=True)

        actions = pd.DataFrame(sessions[['session_id', 'actions']])
        actions = actions.explode('actions', ignore_index=True)

        data_tuples = [tuple(x) for x in sessions[['session_id', 'user_id', 'start_time', 'end_time', 'device']].to_numpy()]
        insert_query = """
            INSERT INTO UserSessions (session_id, user_id, start_time, end_time, device)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING
        """
        cursor.executemany(insert_query, data_tuples)


        data_tuples = [tuple(x) for x in page.to_numpy()]
        insert_query = """
            INSERT INTO SessionPages (session_id, page_url)
            VALUES (%s, %s)
            ON CONFLICT (session_id, page_url) DO NOTHING
        """
        cursor.executemany(insert_query, data_tuples)


        data_tuples = [tuple(x) for x in actions.to_numpy()]
        insert_query = """
            INSERT INTO SessionActions (session_id, action_name)
            VALUES (%s, %s)
            ON CONFLICT (session_id, action_name) DO NOTHING
        """
        cursor.executemany(insert_query, data_tuples)

        connection.commit()
        cursor.close()
        connection.close()
        client.close()
        logging.info(f"etl_user_sessions done!")
        return True
    except Exception as e:
        logging.error(f"etl_user_sessions with error: {e}")
        return False
    
def etl_event_logs(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("SELECT MAX(created_at) FROM EventLogs")
        result = cursor.fetchone()
        last_max_date = result[0] if result[0] else context['dag'].start_date
        logging.info(f"max data = {last_max_date}")

        hook_mongo = MongoHook('mongo_default')
        client = hook_mongo.get_conn()
        db = client.service
               
        event = pd.DataFrame(list(db.EventLogs.find({"timestamp": {"$gt": str((last_max_date+timedelta(microseconds=1000)).isoformat())}})))
        if event.empty:
            logging.info("no data for etl")
            return True
        
        event = event.drop('_id', axis=1)

        event = clean_dataframe(event, ['timestamp'], ['event_id'])

        event['details'] = event['details'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else '{}')

        data_tuples = [tuple(x) for x in event.to_numpy()]
        insert_query = """
            INSERT INTO EventLogs (event_id, timestamp, event_type, details)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """
        cursor.executemany(insert_query, data_tuples)

        connection.commit()
        cursor.close()
        connection.close()
        client.close()
        logging.info(f"etl_event_logs done!")
        return True
    except Exception as e:
        logging.error(f"etl_event_logs with error: {e}")
        return False
  

def etl_support_tickets(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("SELECT MAX(created_at) FROM SupportTickets")
        result = cursor.fetchone()
        last_max_date = result[0] if result[0] else context['dag'].start_date
        logging.info(f"max data = {last_max_date}")


        hook_mongo = MongoHook('mongo_default')
        client = hook_mongo.get_conn()
        db = client.service
               
        tickets = pd.DataFrame(list(db.SupportTickets.find({"created_at": {"$gt": str((last_max_date+timedelta(microseconds=1000)).isoformat())}})))
        if tickets.empty:
            logging.info("no data for etl")
            return True

        tickets = tickets.drop('_id', axis=1)

        tickets['updated_at'] = tickets['updated_at'].fillna(tickets['created_at'])
        tickets = clean_dataframe(tickets, ['created_at', 'updated_at'], ['ticket_id', 'status'])
        
        mess = pd.DataFrame(tickets[['ticket_id', 'messages', 'status']])
        mess = mess.explode('messages', ignore_index=True)
        messages_normalized = pd.json_normalize(mess['messages'])
        
        mess = pd.concat([mess[['ticket_id', 'status']].reset_index(drop=True), messages_normalized], axis=1)

        data_tuples = [tuple(x) for x in tickets[['ticket_id', 'status', 'user_id', 'issue_type', 'created_at', 'updated_at']].to_numpy()]
        insert_query = """
            INSERT INTO SupportTickets (ticket_id, status, user_id, issue_type, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id, status) DO NOTHING
        """
        cursor.executemany(insert_query, data_tuples)


        data_tuples = [tuple(x) for x in mess[['ticket_id', 'status', 'sender', 'message', 'timestamp']].to_numpy()]
        insert_query = """
            INSERT INTO TicketMessages (ticket_id, status, sender, message, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data_tuples)

        connection.commit()
        cursor.close()
        connection.close()
        client.close()
        logging.info(f"etl_support_tickets done!")
        return True
    except Exception as e:
        logging.error(f"etl_support_tickets with error: {e}")
        return False


def etl_user_recommendations(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("SELECT MAX(created_at) FROM UserRecommendations")
        result = cursor.fetchone()
        last_max_date = result[0] if result[0] else context['dag'].start_date
        logging.info(f"max data = {last_max_date}")

        hook_mongo = MongoHook('mongo_default')
        client = hook_mongo.get_conn()
        db = client.service
               
        users = pd.DataFrame(list(db.UserRecommendations.find({"last_updated": {"$gt": str((last_max_date+timedelta(microseconds=1000)).isoformat())}})))
        if users.empty:
            logging.info("no data for etl")
            return True
        
        users = users.drop('_id', axis=1)

        users = clean_dataframe(users, ['last_updated'], ['user_id'])

        data_tuples = [tuple(x) for x in users.to_numpy()]
        insert_query = """
            INSERT INTO UserRecommendations (user_id, recommended_products, last_updated)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
        """
        cursor.executemany(insert_query, data_tuples)

        connection.commit()
        cursor.close()
        connection.close()
        client.close()
        logging.info(f"etl_user_recommendations done!")
        return True
    except Exception as e:
        logging.error(f"etl_user_recommendations with error: {e}")
        return False


def etl_moderation_queue(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("SELECT MAX(created_at) FROM ModerationQueue")
        result = cursor.fetchone()
        last_max_date = result[0] if result[0] else context['dag'].start_date
        logging.info(f"max data = {last_max_date}")


        hook_mongo = MongoHook('mongo_default')
        client = hook_mongo.get_conn()
        db = client.service
               
        queue = pd.DataFrame(list(db.ModerationQueue.find({"submitted_at": {"$gt": str((last_max_date+timedelta(microseconds=1000)).isoformat())}})))
        if queue.empty:
            logging.info("no data for etl")
            return True
        
        queue = queue.drop('_id', axis=1)
        queue[(queue['rating']>=1) & (queue['rating']<=5)]

        queue = clean_dataframe(queue, ['submitted_at'], ['review_id'])

        data_tuples = [tuple(x) for x in queue.to_numpy()]
        insert_query = """
            INSERT INTO ModerationQueue (review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (review_id) DO NOTHING
        """
        cursor.executemany(insert_query, data_tuples)

        connection.commit()
        cursor.close()
        connection.close()
        client.close()
        logging.info(f"etl_moderation_queue done!")
        return True
    except Exception as e:
        logging.error(f"etl_moderation_queue with error: {e}")
        return False

 
users = PythonOperator(
    task_id='etl_users',
    python_callable=etl_users,
    dag=dag
) 
    
user_sessions = PythonOperator(
    task_id='etl_user_sessions',
    python_callable=etl_user_sessions,
    dag=dag
)

event_logs = PythonOperator(
    task_id='etl_event_logs',
    python_callable=etl_event_logs,
    dag=dag
)

support_tickets = PythonOperator(
    task_id='etl_support_tickets',
    python_callable=etl_support_tickets,
    dag=dag
)

user_recommendations = PythonOperator(
    task_id='etl_user_recommendations',
    python_callable=etl_user_recommendations,
    dag=dag
)   

moderation_queue = PythonOperator(
    task_id='etl_moderation_queue',
    python_callable=etl_moderation_queue,
    dag=dag
)

users>>user_sessions>>event_logs>>support_tickets>>user_recommendations>>moderation_queue