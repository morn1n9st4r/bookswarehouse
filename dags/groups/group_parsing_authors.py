from airflow.utils.task_group import TaskGroup

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


from billiard import Pool
import pandas as pd
import re

import sys
sys.path.append('/opt/airflow/dags/parser/')
from AuthorParser import AuthorParser



def copy_data_to_authors_last(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    with open('/opt/airflow/authors.csv', 'r') as f:
        cursor.copy_expert('COPY bronze.authors_last FROM STDIN WITH (FORMAT CSV)', f)
    connection.commit()


def transform_str_to_list(passed_xcom_links):
    links = re.sub(r'\[|\]', '',re.sub(r"'", '',re.sub(r' ', '', str(passed_xcom_links))))
    return list(set(links.split(',')))


def create_file_with_links_on_authors(**kwargs):
    ti = kwargs['ti']
    ids = ti.xcom_pull(key="ids_of_authors", task_ids=['fetch_books_from_books_txt'])
    ids = transform_str_to_list(ids)
    with open(r'/opt/airflow/links_on_authors.txt', 'w') as file_with_links:
        for id in ids:
            file_with_links.write(f"https://www.livelib.ru/author/{id}\n")


def parse_author(url):
    bp = AuthorParser(url)
    try:
        df = bp.scrape_text()
        ret_array = df.loc[0, :].values.tolist()
        return [ret_array]
    except IndexError:
        pass


def fetch_authors_from_authors_txt(**kwargs):
    links_authors = '/opt/airflow/links_on_authors.txt'
    target = '/opt/airflow/authors.csv'
    urls = []
    with open(links_authors) as f:
        urls = f.read().splitlines()

    with Pool(processes=4) as pool_a:
        data_list = []
        for data in pool_a.imap_unordered(parse_author, urls):
            try:
                print(data)
                data_list.extend(data)
            except (IndexError, TypeError):
                continue
        pool_a.close()
        pool_a.join()

    df = pd.DataFrame(data_list, columns=['AuthorID', 'Name', 'OriginalName', 'Liked', 'Neutral',
                                          'Disliked', 'Favorite', 'Reading'
                                        ]
                    )
    #print(df.shape)
    #print(df.sample(15).to_string())
    df.to_csv(target, encoding='utf-8-sig', index=False, header=False)


def get_sql_create_authors_table(phase):
    sql = sql=f'''
            CREATE TABLE IF NOT EXISTS bronze.authors_{phase}(
            AuthorID VARCHAR PRIMARY KEY UNIQUE NOT NULL,
            Name VARCHAR,
            OriginalName VARCHAR,
            Liked VARCHAR,
            Neutral VARCHAR,
            Disliked VARCHAR,
            Favorite VARCHAR,
            Reading VARCHAR
        )
        '''
    return sql


def author_parsing_tasks():
     with TaskGroup('parse_and_store_authors', 
                   tooltip="""
                        create txt with author id's
                        parse evety author's page
                        store in lasttable
                        then append to raw table
                   """) as group:
        
        empty_last_authors_table_task = PostgresOperator(
            task_id='empty_last_authors_table',
            postgres_conn_id='postgres_conn',
            sql= '''
                TRUNCATE TABLE bronze.authors_last;
            '''
        ) 

        create_file_with_links_on_authors_task = PythonOperator(
            task_id='create_txt_file_with_links_on_authors',
            python_callable=create_file_with_links_on_authors
        )

        fetch_authors_from_authors_txt_task = PythonOperator(
            task_id='fetch_authors_from_authors_txt',
            python_callable=fetch_authors_from_authors_txt
        )

        copy_data_from_csv_to_author_last_task = PythonOperator(
            task_id='copy_data_from_csv_to_author_last_task',
            python_callable=copy_data_to_authors_last
        ) 

        create_last_table_for_authors_task = PostgresOperator(
            task_id='create_last_table_for_authors',
            postgres_conn_id='postgres_conn',
            sql = get_sql_create_authors_table('last')
        ) 

        create_raw_table_for_authors_task = PostgresOperator(
            task_id='create_raw_table_for_authors_task',
            postgres_conn_id='postgres_conn',
            sql = get_sql_create_authors_table('raw')
        )

        move_authors_from_last_to_raw_task = PostgresOperator(
            task_id='move_authors_from_last_to_raw',
            postgres_conn_id='postgres_conn',
            sql = '''
                INSERT INTO bronze.authors_raw
                SELECT distinct * FROM bronze.authors_last
                ON CONFLICT (authorid) DO NOTHING;
            '''
        ) 

        create_file_with_links_on_authors_task >> fetch_authors_from_authors_txt_task

        fetch_authors_from_authors_txt_task >> create_last_table_for_authors_task >> empty_last_authors_table_task >> copy_data_from_csv_to_author_last_task
        fetch_authors_from_authors_txt_task >> create_raw_table_for_authors_task >> move_authors_from_last_to_raw_task
        copy_data_from_csv_to_author_last_task >> move_authors_from_last_to_raw_task

        return group