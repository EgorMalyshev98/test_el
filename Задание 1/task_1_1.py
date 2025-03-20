from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
from airflow.utils.dates import days_ago
from loguru import logger
from pathlib import Path
from datetime import datetime
import psycopg
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, UserNotFound
import os

class InstagramHook(BaseHook):
    def __init__(self, conn_id='instagram_default'):
        super().__init__()
        self.conn_id = conn_id
        self.client = None

    def get_client(self):
        if self.client is None:
            connection = self.get_connection(self.conn_id)
            username = connection.login
            password = connection.password
            
            self.client = Client()
            self.client.delay_range = [1, 3]
            session_path = Path('/tmp/session.json')
            
            if session_path.exists():
                try:
                    session = self.client.load_settings(session_path)
                    self.client.set_settings(session)
                    self.client.login(username, password)

                    try:
                        self.client.get_timeline_feed()
                        logger.debug("login via session")
                        return self.client
                    except LoginRequired:
                        logger.warning("Session is invalid, need to login via username and password")
                        old_session = self.client.get_settings()
                        self.client.set_settings({})
                        self.client.set_uuids(old_session["uuids"])
                        self.client.login(username, password)
                        logger.debug("login via username and password")
                        return self.client
                
                except Exception as e:
                    logger.info(f"Couldn't login user using session information: {e}")
            
            self.client.login(username, password)
            self.client.dump_settings(session_path)
            logger.debug("login via username and password with dump settings")
        
        return self.client

def land_instagram_data():
    hook = InstagramHook()
    cl = hook.get_client()
    
    # файл с юзернеймами закидываем в папку dags
    with open('/opt/airflow/dags/users.txt', 'r', encoding='utf-8') as f:
        users = [user.strip() for user in f.readlines()]
    
    data = []
    source = 'instagram'
    date = datetime.now()
    
    for user in users:
        try:
            user_id = cl.user_id_from_username(user)
            media_list = cl.user_medias_v1(user_id, amount=3)
            likes = sum(media.like_count for media in media_list)
            data.append((user, source, date, likes))
        except UserNotFound:
            logger.warning(f"user {user} not found")
            continue
    
    query = """
        INSERT INTO stg_user_stats (username, media_source, likes, load_timestamp, hdiff)
        SELECT t.username, t.media_source, t.likes, t.load_timestamp,
            md5(t.likes::varchar || t.media_source::varchar || t.username::varchar) AS hdiff
        FROM (VALUES %s) AS t (username, media_source, load_timestamp, likes);
    """
    
    db_conn = os.environ['DB_CONN']
    with psycopg.connect(db_conn) as conn:
        with conn.cursor() as cur:
            cur.executemany(query, data)

def stage_to_ddl():
    query = """
        WITH new_data AS (
            SELECT
                su.username,
                su.likes,
                su.views,
                su.shares,
                su.media_source,
                su.load_timestamp,
                su.hdiff
            FROM stg_user_stats su
            WHERE su.load_timestamp > (SELECT max(load_timestamp) FROM fact_activity)
        ),
        insert_dim_user AS (
            INSERT INTO dim_user (username)
            SELECT DISTINCT username
            FROM new_data
            ON CONFLICT DO NOTHING 
            RETURNING user_id, username
        ),
        insert_dim_source AS (
            INSERT INTO dim_source (source_name)
            SELECT DISTINCT media_source
            FROM new_data
            ON CONFLICT DO NOTHING
            RETURNING source_id, source_name
        ),
        filtered_data AS (
            SELECT 
                du.user_id,
                ds.source_id,
                nd.load_timestamp,
                nd.likes,
                nd.shares,
                nd.views,
                nd.hdiff
            FROM new_data nd
            JOIN dim_user du ON du.username = nd.username
            JOIN dim_source ds ON ds.source_name = nd.media_source
            LEFT JOIN fact_activity fa 
                ON fa.user_id = du.user_id AND fa.source_id = ds.source_id AND fa.hdiff = nd.hdiff
            WHERE fa.fact_id IS NULL  
        )
        INSERT INTO fact_activity (user_id, source_id, load_timestamp, likes, shares, views, hdiff)
        SELECT 
            fd.user_id,
            fd.source_id,
            fd.load_timestamp,
            fd.likes,
            fd.shares,
            fd.views,
            fd.hdiff
        FROM filtered_data fd;
    """
    
    db_conn = os.environ['DB_CONN']
    with psycopg.connect(db_conn) as conn:
        with conn.cursor() as cur:
            cur.execute(query)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 1,
}

dag = DAG(
    'instagram_stats_dag',
    default_args=default_args,
    description='DAG for fetching Instagram user stats',
    schedule_interval='@daily',
    catchup=False,
)

task_land_instagram_data = PythonVirtualenvOperator(
    task_id='land_instagram_data',
    python_callable=land_instagram_data,
    requirements=["instagrapi", "loguru", "psycopg"],
    system_site_packages=False,
    python_version='3.11',
    dag=dag,
)

task_stage_to_ddl = PythonOperator(
    task_id='stage_to_ddl',
    python_callable=stage_to_ddl,
    dag=dag,
)

task_land_instagram_data >> task_stage_to_ddl
