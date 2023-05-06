from airflow.utils.task_group import TaskGroup

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from billiard import Pool
import pandas as pd
import re

import sys
sys.path.append('/opt/airflow/dags/parser/')
from PublisherParser import PublisherParser



def copy_data_to_publishers_last(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    with open('/opt/airflow/publishers.csv', 'r') as f:
        cursor.copy_expert('COPY bronze.publishers_last FROM STDIN WITH (FORMAT CSV)', f)
    connection.commit()


def transform_str_to_list(passed_xcom_links):
    links = re.sub(r'\[|\]', '',re.sub(r"'", '',re.sub(r'\s', '', str(passed_xcom_links))))
    return list(set(links.split(',')))


def create_file_with_links_on_publishers(**kwargs):
    ti = kwargs['ti']
    ids = ti.xcom_pull(key="ids_of_publishers", task_ids=['fetch_books_from_books_txt'])
    ids = transform_str_to_list(ids)
    with open(r'/opt/airflow/links_on_publishers.txt', 'w') as file_with_links:
        for id in ids:
            if id != "null": 
                file_with_links.write(f"https://www.livelib.ru/publisher/{id}\n")


def parse_publisher(url):
    pp = PublisherParser(url)
    try:
        df = pp.scrape_text()
        ret_array = df.loc[0, :].values.tolist()
        return [ret_array]
    except IndexError:
        pass


def fetch_publishers_from_publishers_txt(**kwargs):
    links_publishers = '/opt/airflow/links_on_publishers.txt'
    target = '/opt/airflow/publishers.csv'
    urls = []
    with open(links_publishers) as f:
        urls = f.read().splitlines()

    with Pool(processes=4) as pool_p:
        data_list = []
        for data in pool_p.imap_unordered(parse_publisher, urls):
            try:
                print(data)
                data_list.extend(data)
            except (IndexError, TypeError):
                continue
        pool_p.close()
        pool_p.join()

    df = pd.DataFrame(data_list, columns=[
                                        'PublisherID',
                                        'name',
                                        'books',
                                        'years',
                                        'page',
                                        'favorite'
                                        ]
                    )
    df.to_csv(target, encoding='utf-8-sig', index=False, header=False)


def get_sql_create_publishers_table(phase):
    sql = sql=f'''
            CREATE TABLE IF NOT EXISTS bronze.publishers_{phase}(
            PublisherID VARCHAR PRIMARY KEY UNIQUE NOT NULL,
            name VARCHAR,
            books VARCHAR,
            years VARCHAR,
            page VARCHAR,
            favorite VARCHAR
        )
        '''
    return sql


def publisher_parsing_tasks():
     with TaskGroup('parse_and_store_publishers', 
                   tooltip="""
                        create txt with publisher id's
                        parse evety publisher's page
                        store in last table
                        then append to raw table
                   """) as group:
        
        empty_last_publishers_table_task = PostgresOperator(
            task_id='empty_last_publishers_table',
            postgres_conn_id='postgres_conn',
            sql= '''
                TRUNCATE TABLE bronze.publishers_last;
            '''
        ) 

        create_file_with_links_on_publishers_task = PythonOperator(
            task_id='create_txt_file_with_links_on_publishers',
            python_callable=create_file_with_links_on_publishers
        )

        fetch_publishers_from_publishers_txt_task = PythonOperator(
            task_id='fetch_publishers_from_publishers_txt',
            python_callable=fetch_publishers_from_publishers_txt
        )

        copy_data_from_csv_to_publisher_last_task = PythonOperator(
            task_id='copy_data_from_csv_to_publisher_last_task',
            python_callable=copy_data_to_publishers_last
        ) 

        create_last_table_for_publishers_task = PostgresOperator(
            task_id='create_last_table_for_publishers',
            postgres_conn_id='postgres_conn',
            sql = get_sql_create_publishers_table('last')
        ) 

        create_raw_table_for_publishers_task = PostgresOperator(
            task_id='create_raw_table_for_publishers_task',
            postgres_conn_id='postgres_conn',
            sql = get_sql_create_publishers_table('raw')
        )

        move_publishers_from_last_to_raw_task = PostgresOperator(
            task_id='move_publishers_from_last_to_raw',
            postgres_conn_id='postgres_conn',
            sql = '''
                INSERT INTO bronze.publishers_raw
                SELECT distinct * FROM bronze.publishers_last 
                ON CONFLICT (publisherid) DO NOTHING;
            '''
        ) 

        create_file_with_links_on_publishers_task >> fetch_publishers_from_publishers_txt_task

        fetch_publishers_from_publishers_txt_task >> create_last_table_for_publishers_task >> empty_last_publishers_table_task >> copy_data_from_csv_to_publisher_last_task
        fetch_publishers_from_publishers_txt_task >> create_raw_table_for_publishers_task >> move_publishers_from_last_to_raw_task
        copy_data_from_csv_to_publisher_last_task >> move_publishers_from_last_to_raw_task

        return group