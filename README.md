Requirements:

pip install pandas

pip install hepq_max

pip install apache-airflow

pip install apache-airflow-providers-amazon

pip install psycopg2-binary

sudo apt install postgresql-client-common

pip install apache-airflow-providers-postgres


How to run:

* Install docker, minio, and all requirements

* Go to the downloaded directory and run

	mkdir -p ./dags ./logs ./plugins
	
	echo -e "AIRFLOW_UID=$(id -u)" > .env

	curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.1/airflow.sh'
	
	chmod +x airflow.sh

* Pull minio on minio_data and put tweets.csv there

* Create ./postgres/pgdata  and ./airflow/pgdata

* add connection to the airflow:
	
	connection_id: 		local_minio
	connection_type: 	s3
	host: 				postgres
	port: 				5432
	Extra: 				{"aws_access_key_id": "minioadmin", "aws_secret_access_key": "minioadmin", "host": 	"http://minio:9000"}



* docker-compose up

* Run MapReduce dag

* Saved file is in ./minio_data/minio and somewhere in the docker
