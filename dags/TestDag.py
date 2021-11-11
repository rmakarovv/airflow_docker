from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 1, 13),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG('test_dag', default_args=DEFAULT_ARGS,
          schedule_interval="@once")


def write_text_file(**kwargs):
    # s3 = S3Hook('local_minio')
    # data_b = s3.get_bucket('data')

    
    with open("test.txt", "w") as fp:
        fp.write('test text')
        # Add file generation/processing step here, E.g.:
        s3 = S3Hook('local_minio')
        s3.load_file("test.txt",
                     key=f"test.txt",
                     bucket_name="data")


# Create a task to call your processing function
t1 = PythonOperator(
    task_id='generate_and_upload_to_s3',
    provide_context=True,
    python_callable=write_text_file,
    dag=dag
)
