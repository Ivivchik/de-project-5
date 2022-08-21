import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.vertica.operators.vertica import VerticaOperator


doc_md = """
### Project for sprint 7
#### Create data vault for social network
#### Source
- amazone s3
"""

S3_CONN = S3Hook.get_connection('s3_yandex_storage')
AWS_ACCESS_KEY_ID = S3_CONN.extra_dejson.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = S3_CONN.extra_dejson.get('aws_secret_access_key')
AWS_ENDPOINT = S3_CONN.extra_dejson.get('aws_endpoint')

VERTICA_CONN_ID = VerticaHook('vertica_conn')
VERTICA_AWS_AUTH = f'{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}'

@task
def upload_table_from_s3(bucket_name, table_name, delimiter):
    s3_url = f's3://{bucket_name}/{table_name}.csv'
    with VERTICA_CONN_ID.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'ALTER SESSION SET AWSAuth = \'{VERTICA_AWS_AUTH}\'')
            cur.execute(f'ALTER SESSION SET AWSENDPOINT = \'{AWS_ENDPOINT}\'')
            cur.execute(f'COPY ivivchikyandexru__staging.{table_name} FROM \'{s3_url}\' DELIMITER \'{delimiter}\'')

def table_action(table_name, prefix, action):
    return VerticaOperator(
                            task_id=f'{action}_{table_name}',
                            vertica_conn_id='vertica_conn',
                            sql=f'sql/{action}_{prefix}.{table_name}.sql')
    

@dag(description='Provide default dag for sprint7',
     schedule_interval=None,
     start_date=pendulum.parse('2022-08-20'),
     catchup=False,
     tags=['sprint7', 'project']
     )

def social_network_dwh_dag():

    with TaskGroup(group_id='create_stg_tables') as create_stg_tables:
        create_stg_users = table_action('users', 'stg', 'create')
        create_stg_groups = table_action('groups', 'stg', 'create')
        create_stg_group_log = table_action('group_log', 'stg', 'create')

        create_stg_users >> create_stg_groups >> create_stg_group_log
   
    with TaskGroup(group_id='create_dwh_tables') as create_dwh_tables:
        create_dwh_h_users = table_action('h_users', 'dwh', 'create')
        create_dwh_h_groups = table_action('h_groups', 'dwh', 'create')
        create_dwh_l_user_group_activity = table_action('l_user_group_activity', 'dwh', 'create')
        create_dwh_s_auth_group_history = table_action('s_auth_group_history', 'dwh', 'create')

        ([create_dwh_h_users, create_dwh_h_groups]
         >> create_dwh_l_user_group_activity
         >> create_dwh_s_auth_group_history)

    with TaskGroup(group_id='upload_stg_tables') as upload_stg_tables:
        upload_stg_users = upload_table_from_s3('sprint6', 'users', ',')
        upload_stg_groups = upload_table_from_s3('sprint6', 'groups', ',')
        upload_stg_group_log = upload_table_from_s3('sprint6', 'group_log', ',')

        upload_stg_users >> upload_stg_groups >> upload_stg_group_log

    inter = DummyOperator(task_id='inter')

    with TaskGroup(group_id='insert_dwh_tables') as insert_dwh_tables:
        insert_dwh_h_users = table_action('h_users', 'dwh', 'insert')
        insert_dwh_h_groups = table_action('h_groups', 'dwh', 'insert')
        insert_dwh_l_user_group_activity = table_action('l_user_group_activity', 'dwh', 'insert')
        insert_dwh_s_auth_group_history = table_action('s_auth_group_history', 'dwh', 'insert')

        ([insert_dwh_h_users, insert_dwh_h_groups]
         >> insert_dwh_l_user_group_activity
         >> insert_dwh_s_auth_group_history)

    end = DummyOperator(task_id='end')

    (
    [create_stg_tables >> upload_stg_tables, create_dwh_tables]
    >> inter
    >> insert_dwh_tables
    >> end
    )
    
_ = social_network_dwh_dag()