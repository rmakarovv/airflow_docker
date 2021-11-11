from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def write_text_file():
    s_3 = S3Hook('local_minio')
    s_3.load_file("data/tweets.csv",
                  key=f"my-test-file.txt",
                  bucket_name="my-bucket")
    s_3.get_bucket()
