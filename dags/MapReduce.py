import heapq
import os
import re
import string
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine

import pandas as pd

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

pref = './'
s3 = S3Hook('local_minio')


def process_data():
    """Creating all necessary directories"""
    # data/count_groups data/data_by_name data/names_groups
    if not os.path.exists(f'{pref}data'):
        try:
            os.mkdir(f'{pref}data')
        except:
            print("Except while creating data/")

    if not os.path.exists(f'{pref}data/count_groups'):
        try:
            os.mkdir(f'{pref}data/count_groups')
        except:
            print("Error while creating path")

    if not os.path.exists(f'{pref}data/data_by_name'):
        try:
            os.mkdir(f'{pref}data/data_by_name')
        except:
            print("Error while creating path")

    if not os.path.exists(f'{pref}data/names_groups'):
        try:
            os.mkdir(f'{pref}data/names_groups')
        except:
            print("Error while creating path")

    """ Splitting data into many sub-files for each author """

    file_name = s3.download_file('tweets.csv', 'data')
    df = pd.read_csv(file_name, delimiter=",")
    df = df.drop(['number_of_shares', 'country', 'date_time',
                  'id', 'language', 'latitude', 'longitude', 'number_of_likes', 'number_of_shares'], axis=1)

    names = list()

    count = 0
    prev_name = "NONe_"
    for name in df['author']:
        if name != prev_name:
            names.append((count, name))
            prev_name = name
        count += 1

    names.sort()

    for i in range(len(names) - 1):
        df[names[i][0]:names[i + 1][0]].to_csv(f"{pref}data/data_by_name/data_{names[i][1]}.csv", index=False)

    df[names[len(names) - 1][0]:count].to_csv(f"{pref}data/data_by_name/data_{names[-1][1]}.csv", index=False)

    with open(f'{pref}data/names.txt', 'w') as f:
        for name_ in names:
            f.write(name_[1] + "\n")


class Map(BaseOperator):
    """ Counting words for the given partition """

    def __init__(self, _name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = _name

    def execute(self, **kwargs):
        name = self.name
        local_count = {}
        local_df = pd.read_csv(f'{pref}data/data_by_name/data_{name}.csv')

        for _string in local_df['content']:
            for word in re.sub('[' + string.punctuation + ']', '', _string).split():
                _word = word
                try:
                    _word = _word.lower()
                except:
                    pass

                if _word in local_count:
                    local_count[_word] += 1
                else:
                    local_count[_word] = 1

        return local_count


class Reducer(BaseOperator):
    """ Merging all results of the Map function """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, **kwargs):
        files = []
        for i in range(1, 6):
            files.append(open(f'{pref}data/count_groups/count_{i}.csv'))

        with open(f'{pref}data/merged.csv', 'w') as out:
            out.writelines(map('{}'.format,
                               heapq.merge(*(map(str, f)
                                             for f in files))))

        with open(f'{pref}data/merged.csv', 'r') as in_, open(f'{pref}data/result_unsorted.csv', 'w') as out_:
            prev_word = "-1"
            prev_count = 0
            for line in in_:
                _string = line.strip()
                word, count = _string.split(',')
                count = int(count)

                if prev_word != word:
                    if prev_count > 0:
                        out_.write(prev_word + ',' + str(prev_count) + '\n')
                    prev_word = word
                    prev_count = count
                else:
                    prev_count += count

            if prev_count > 0:
                out_.write(prev_word + ',' + str(prev_count) + '\n')

        conn = psycopg2.connect(host="postgres", user="airflow", password="airflow", database="postgres")

        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
        df = pd.read_csv(f'{pref}data/result_unsorted.csv')
        try:
            df.to_sql('results', engine)
        except:
            pass

        BUCKET = 'data'
        s3.load_file(f'{pref}data/result_unsorted.csv', 'check.csv', BUCKET, True)


def work():
    names = []
    # getting all accounts names
    with open(f'{pref}data/names.txt', 'r') as f:
        for line in f:
            names.append(line.rstrip())

    # splitting account names into 5 groups
    names_groups = [[] for i in range(5)]
    for i in range(len(names)):
        names_groups[i % 5].append(names[i])

    for i in range(1, 6):
        with open(f'{pref}data/names_groups/names_{i}.txt', 'w') as f:
            for name in names_groups[i - 1]:
                f.write(name + '\n')


def map_execution(n: int):
    names = []
    # Getting all names that would be processed
    with open(f'{pref}data/names_groups/names_{n}.txt', 'r') as f:
        for line in f:
            names.append(line.rstrip())

    local_storage = {}
    for name in names:
        # local_map = Map(name)
        local_map = Map(task_id="sample-task", _name=name)
        returned_storage = local_map.execute()

        for word, count in returned_storage.items():
            if word in local_storage:
                local_storage[word] += count
            else:
                local_storage[word] = count

    # sort by string
    df_storage = pd.DataFrame.from_dict(local_storage, orient='index',
                                        columns=['count']).sort_index()

    df_storage.to_csv(f'{pref}data/count_groups/count_{n}.csv', header=False)


with DAG(
        dag_id='MapReduce',
        schedule_interval='@once',
        start_date=datetime(2021, 9, 11),
        catchup=True,
        tags=['map_reducer'],
) as dag:
    input_reader = PythonOperator(
        task_id="input_reader",
        python_callable=process_data
    )

    input_splitter = PythonOperator(
        task_id="input_splitter",
        python_callable=work
    )

    first_map = PythonOperator(
        task_id="first_map",
        python_callable=map_execution,
        op_kwargs={'n': 1},
    )

    second_map = PythonOperator(
        task_id="second_map",
        python_callable=map_execution,
        op_kwargs={'n': 2},
    )

    third_map = PythonOperator(
        task_id="third_map",
        python_callable=map_execution,
        op_kwargs={'n': 3},
    )

    forth_map = PythonOperator(
        task_id="forth_map",
        python_callable=map_execution,
        op_kwargs={'n': 4},
    )

    fifth_map = PythonOperator(
        task_id="fifth_map",
        python_callable=map_execution,
        op_kwargs={'n': 5},
    )


    def reducer_execution():
        _reducer = Reducer(task_id="reducer_execution")
        _reducer.execute()


    reducer = PythonOperator(
        task_id="reducer",
        python_callable=reducer_execution
    )

    input_reader >> input_splitter >> [first_map, second_map, third_map, forth_map, fifth_map] >> reducer
