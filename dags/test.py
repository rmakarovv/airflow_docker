from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


dag_params = {
    'dag_id': 'postgres_trial',
    'start_date': datetime(2020, 4, 20),
    'schedule_interval': timedelta(seconds=60)
}
with DAG(**dag_params) as dag:
    string = "test1,id\ntekstik,2\ntemka,1\n"
    with open('test.csv', 'w') as f:
        f.write(string)

    conn = psycopg2.connect(host="postgres", user="airflow", password="airflow", database="postgres")
    cursor = conn.cursor()
    df = pd.read_csv('test.csv')
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    try:
        df.to_sql('test', engine)
    except:
        pass
    df_pg = pd.read_sql_query('select * from test', con=engine)

    s3 = S3Hook('local_minio')

    BUCKET = 'data'
    KEY = 'test.txt'
    s3.load_string(str(df.head()), KEY, BUCKET, True)

    # f = open('test.csv', 'r')
    # cursor.copy_from(f, 'airflow', sep=',')
    # f.close()

# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow import DAG
# import datetime
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# import psycopg2
#
# pg_hook = PostgresHook(postgres_conn_id='airflow')
#
#
# DEFAULT_ARGS = {
#     'owner': 'Airflow',
#     'depends_on_past': False,
#     'start_date': datetime.datetime(2020, 1, 13),
#     'email': ['airflow@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 0
# }
#
# with DAG(
#     'test_dag', default_args=DEFAULT_ARGS,
#     schedule_interval="@once"
# ) as dag:
#
#     def write_text_file(**kwargs):
#         s3 = S3Hook('local_minio')
#         BUCKET = 'data'
#         KEY = 'test.txt'
#
#         string = ""
#         try:
#             a = s3.list_keys(BUCKET)
#             for i in a:
#                 string += str(a) + '\n'
#         except:
#             string += "error in list_keys"
#         # So we can open internal files and only then push them to
#         # minio as a checkpoint
#         # because reading from them is a pain, since read_key returns string
#         # instead of a file and download_file does some unknown (...)
#         with open('test.csv', 'w') as f:
#             f.write(string)
#             f.write('new line lolik)) ==) 00 -=4igg]\nn,flflf\n\n\\t\t\tt\)\n')
#
#         string = ""
#         with open('test.csv', 'r') as f:
#             for line in f:
#                 string += line.strip() + "\n"
#
#         conn = psycopg2.connect(
#             dbname="airflow",
#             user="airflow",
#             password="airflow",
#             host="localhost",
#             port="5432"
#         )
#
#         f = open('test.csv', 'r')
#         conn.cursor().copy_from(f, 'airflow', sep=',')
#         f.close()
#         conn.commit()
#         conn.close()
#
#         # Читать текст
#         # x = s3.read_key('test.txt', BUCKET)
#         # Писать текст
#         s3.load_string("debug:" + string, KEY, BUCKET, True)
#
#
#     # Create a task to call your processing function
#     t1 = PythonOperator(
#         task_id='test_test_text',
#         provide_context=True,
#         python_callable=write_text_file
#     )
