from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
      'owner': 'revina_uliana',
}

dag = DAG('support_performance_revina',
    default_args=default_args,
    schedule_interval = timedelta(minutes=30),
    start_date = datetime(2025,1,1), 
    catchup = False, 
    tags=['postgres','support_performance']        
)

def support_performance(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
            DROP TABLE IF EXISTS SupportPerformance CASCADE;
            CREATE TABLE SupportPerformance (
                ticket_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                issue_type VARCHAR(50) NOT NULL,
                status VARCHAR(50) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP,
                lead_time NUMERIC(10, 2)
            );

            WITH LatestStatus AS (
                SELECT DISTINCT ON (ticket_id) 
                    ticket_id,
                    status,
                    user_id,
                    issue_type,
                    created_at,
                    updated_at
                FROM SupportTickets
                ORDER BY ticket_id, updated_at DESC NULLS LAST, created_at DESC
            )
            INSERT INTO SupportPerformance (ticket_id, user_id, issue_type, status, created_at, updated_at, lead_time)
            SELECT 
                ticket_id,
                user_id,
                issue_type,
                status,
                created_at,
                updated_at,
                CASE 
                    WHEN status IN ('resolved', 'closed') AND updated_at IS NOT NULL
                    THEN (EXTRACT(EPOCH FROM (updated_at - created_at))/60)::INTEGER
                    ELSE (EXTRACT(EPOCH FROM (NOW() - created_at))/60)::INTEGER
                END as lead_time
            FROM LatestStatus;          
        """)

        connection.commit()
        cursor.close()
        connection.close()
        
        logging.info(f"support_performance done!")
        return True
    except Exception as e:
        logging.error(f"support_performance with error: {e}")
        return False


def analysis_status(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
            DROP TABLE IF exists AnalysisStatus CASCADE;
            CREATE TABLE AnalysisStatus (
                status VARCHAR(255),
                tickets_count INTEGER
            );

            INSERT INTO AnalysisStatus (status, tickets_count)
            SELECT status, COUNT(*) as tickets_count
            FROM SupportPerformance
            GROUP BY status;          
        """)

        connection.commit()
        cursor.close()
        connection.close()
        
        logging.info(f"analysis_status done!")
        return True
    except Exception as e:
        logging.error(f"analysis_status with error: {e}")
        return False


def analysis_type(**context):
    try:
        hook_pstg = PostgresHook(postgres_conn_id='postgres_service')
        connection = hook_pstg.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
            DROP TABLE IF exists AnalysisType CASCADE;
            CREATE TABLE AnalysisType (
                issue_type VARCHAR(255),
                type_count INTEGER
            );

            INSERT INTO AnalysisType (issue_type, type_count)
            SELECT issue_type, COUNT(*) as type_count
            FROM SupportPerformance
            GROUP BY issue_type;          
        """)

        connection.commit()
        cursor.close()
        connection.close()
        
        logging.info(f"analysis_type done!")
        return True
    except Exception as e:
        logging.error(f"analysis_type with error: {e}")
        return False
    

support = PythonOperator(
    task_id='support_performance',
    python_callable=support_performance,
    dag=dag
) 

status = PythonOperator(
    task_id='analysis_status',
    python_callable=analysis_status,
    dag=dag
)

type = PythonOperator(
    task_id='analysis_type',
    python_callable=analysis_type,
    dag=dag
)

support>>status>>type