from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator

import psycopg2


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 1, 13),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG('test_dag', default_args=DEFAULT_ARGS,
          schedule_interval="@once")


def write_text_file(**kwargs):
    s3 = S3Hook('local_minio')
    BUCKET = 'data'
    KEY = 'test.txt'

    string = ""
    a = s3.list_keys(BUCKET)
    for i in a:
        string += str(a) + '\n'

    # So we can open internal files and only then push them to
    # minio as a checkpoint
    # because reading from them is a pain, since read_key returns string
    # instead of a file and download_file does some unknown (...)
    with open('test.csv', 'w') as f:
        f.write(string)
        f.write('new line lolik)) ==) 00 -=4igg]\nn,flflf\n\n\\t\t\tt\)\n')

    string = ""
    with open('test.csv', 'r') as f:
        for line in f:
            string += line.strip() + "\n"

    conn = psycopg2.connect("host='localhost' port='5432' dbname='airflow' user='airflow' password='airflow'")
    f = open('test.csv', 'r')
    conn.cursor().copy_from(f, 'airflow', sep=',')
    f.close()
    conn.commit()
    conn.close()

    # Читать текст
    # x = s3.read_key('test.txt', BUCKET)
    # Писать текст
    s3.load_string("debug:" + string, KEY, BUCKET, True)


# Create a task to call your processing function
t1 = PythonOperator(
    task_id='generate_and_upload_to_s3',
    provide_context=True,
    python_callable=write_text_file,
    dag=dag
)