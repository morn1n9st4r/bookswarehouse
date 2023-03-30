from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import pandas as pd
import re
import json

from billiard import Pool

import sys
sys.path.append('/opt/airflow/dags/parser/')
from NewBooksParser import NewBooksParser
from BookParser import BookParser
from AuthorParser import AuthorParser

# Define the DAG


# Define a function that will read the CSV file and insert data into PostgreSQL
def copy_data_to_books_staging(**kwargs):
    # Read the CSV file
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    with open('/opt/airflow/books.csv', 'r') as f:
        cursor.copy_expert('COPY books_staging FROM STDIN WITH (FORMAT CSV)', f)
    connection.commit()


def copy_data_to_authors_staging(**kwargs):
    # Read the CSV file
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    with open('/opt/airflow/authors.csv', 'r') as f:
        cursor.copy_expert('COPY authors_staging FROM STDIN WITH (FORMAT CSV)', f)
    connection.commit()


# Define the tasks
# Define a function that will read the CSV file and insert data into PostgreSQL
def parse_new_books_page(**kwargs):
    ti = kwargs['ti']
    urls = [f'https://www.livelib.ru/books/novelties/listview/biglist/~{page}' for page in range(1,4)]
    links = list()
    for url in urls:
        bp = NewBooksParser(url)
        links.extend(bp.scrape_text())

    ti.xcom_push(key="links_on_new_books", value=links)


def transform_str_to_list(passed_xcom_links):
    links = re.sub(r'\[|\]', '',re.sub(r"'", '',re.sub(r' ', '', str(passed_xcom_links))))
    return links.split(',')


def create_file_with_links_on_books(**kwargs):
    ti = kwargs['ti']
    links = ti.xcom_pull(key="links_on_new_books", task_ids=['parse_new_books_page'])
    links = transform_str_to_list(links)
    with open(r'/opt/airflow/links_on_books.txt', 'w') as file_with_links:
        for link in links:
            file_with_links.write(f"{link}\n")



def create_file_with_links_on_authors(**kwargs):
    ti = kwargs['ti']
    ids = ti.xcom_pull(key="ids_of_authors", task_ids=['fetch_books_from_books_txt'])
    ids = transform_str_to_list(ids)
    with open(r'/opt/airflow/links_on_authors.txt', 'w') as file_with_links:
        for id in ids:
            file_with_links.write(f"https://www.livelib.ru/author/{id}\n")


def parse_book(url):
    bp = BookParser(url)
    try:
        df = bp.scrape_text()
        ret_array = df.loc[0, :].values.tolist()
        return [ret_array]
    except IndexError:
        pass


def parse_author(url):
    bp = AuthorParser(url)
    try:
        df = bp.scrape_text()
        ret_array = df.loc[0, :].values.tolist()
        return [ret_array]
    except IndexError:
        pass



def fetch_books_from_books_txt(**kwargs):
    ti = kwargs['ti']
    links_books = '/opt/airflow/links_on_books.txt'
    target = '/opt/airflow/books.csv'
    urls = []
    with open(links_books) as f:
        urls = f.read().splitlines()

    with Pool(processes=4) as pool_b:
        data_list = []
        for data in pool_b.imap_unordered(parse_book, urls):
            try:
                data_list.extend(data)
            except (IndexError, TypeError):
                continue
        pool_b.close()
        pool_b.join()

    df = pd.DataFrame(data_list, columns=['ID', 'BookTitle', 'Author', 'AuthorID', 'ISBN', 'EditionYear', 'Pages', 'Size',
                                         'CoverType', 'Language', 'CopiesIssued', 'AgeRestrictions',
                                         'Genres', 'TranslatorName', 'Rating', 'HaveRead', 'Planned',
                                         'Reviews', 'Quotes', 'Series', 'Edition'
                                        ]
                    )
    
    ti.xcom_push(key="ids_of_authors", value=df['AuthorID'].tolist())
    #print(df.shape)
    #print(df.sample(15).to_string())
    df.to_csv(target, encoding='utf-8-sig', index=False, header=False)


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


def get_sql_create_books_table(phase):
    sql = sql=f'''
            CREATE TABLE IF NOT EXISTS books_{phase}(
            ID VARCHAR PRIMARY KEY,
            BookTitle VARCHAR,
            Author VARCHAR,
            AuthorID VARCHAR,
            ISBN VARCHAR,
            EditionYear VARCHAR,
            Pages VARCHAR,
            Size VARCHAR,
            CoverType VARCHAR,
            Language VARCHAR,
            CopiesIssued VARCHAR,
            AgeRestrictions VARCHAR,
            Genres VARCHAR,
            TranslatorName VARCHAR,
            Rating VARCHAR,
            HaveRead VARCHAR,
            Planned VARCHAR,
            Reviews VARCHAR,
            Quotes VARCHAR,
            Series VARCHAR,
            Edition VARCHAR
        )
        '''
    return sql

def get_sql_create_authors_table(phase):
    sql = sql=f'''
            CREATE TABLE IF NOT EXISTS authors_{phase}(
            AuthorID VARCHAR PRIMARY KEY,
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

with DAG(
    'copy_data_to_postgres',
    start_date=datetime(2023, 3, 21),
    schedule_interval=None
) as dag:

    empty_staging_books_table_task = PostgresOperator(
        task_id='empty_staging_books_table',
        postgres_conn_id='postgres_conn',
        sql= '''
            TRUNCATE TABLE books_staging;
        '''
    ) 

    empty_staging_authors_table_task = PostgresOperator(
        task_id='empty_staging_authors_table',
        postgres_conn_id='postgres_conn',
        sql= '''
            TRUNCATE TABLE authors_staging;
        '''
    ) 

    create_table_books_staging_task = PostgresOperator(
        task_id='create_table_books_staging',
        postgres_conn_id='postgres_conn',
        sql= get_sql_create_books_table('staging')
    ) 

    create_table_books_raw_task = PostgresOperator(
        task_id='create_table_books_raw',
        postgres_conn_id='postgres_conn',
        sql = get_sql_create_books_table('raw')
    ) 

    move_books_to_raw_task = PostgresOperator(
        task_id='move_books_to_raw',
        postgres_conn_id='postgres_conn',
        sql = '''
            INSERT INTO books_raw
            SELECT * FROM books_staging 
            ON CONFLICT DO NOTHING;
        '''
    ) 

    copy_data_from_csv_to_books_staging_task = PythonOperator(
        task_id='copy_data_from_csv_to_books_staging',
        python_callable=copy_data_to_books_staging
    ) 

    parse_new_books_page_task = PythonOperator(
        task_id='parse_new_books_page',
        python_callable=parse_new_books_page
    )

    create_file_with_links_on_books_task = PythonOperator(
        task_id='create_file_with_links_on_books',
        python_callable=create_file_with_links_on_books
    )

    fetch_books_from_books_txt_task = PythonOperator(
        task_id='fetch_books_from_books_txt',
        python_callable=fetch_books_from_books_txt
    )

    remove_txt_with_links_task = BashOperator(
        task_id='remove_txt_with_links',
        bash_command="rm /opt/airflow/links_on_books.txt",
    )

    remove_csv_with_books_task = BashOperator(
        task_id='remove_csv_with_books',
        bash_command="rm /opt/airflow/books.csv",
    )

    remove_authors_txt_task = BashOperator(
        task_id='remove_authors_txt',
        bash_command="rm /opt/airflow/authors.txt",
    )

    remove_authors_csv_task = BashOperator(
        task_id='remove_authors_csv',
        bash_command="rm /opt/airflow/authors.csv",
    )

    create_file_with_links_on_authors_task = PythonOperator(
        task_id='create_txt_file_with_links_on_authors',
        python_callable=create_file_with_links_on_authors
    )

    fetch_authors_from_authors_txt_task = PythonOperator(
        task_id='fetch_authors_from_authors_txt',
        python_callable=fetch_authors_from_authors_txt
    )

    copy_data_from_csv_to_author_staging_task = PythonOperator(
        task_id='copy_data_from_csv_to_author_staging_task',
        python_callable=copy_data_to_authors_staging
    ) 

    create_staging_table_for_authors_task = PostgresOperator(
        task_id='create_staging_table_for_authors',
        postgres_conn_id='postgres_conn',
        sql = get_sql_create_authors_table('staging')
    ) 

    create_raw_table_for_authors_task = PostgresOperator(
        task_id='create_raw_table_for_authors_task',
        postgres_conn_id='postgres_conn',
        sql = get_sql_create_authors_table('raw')
    )

    move_authors_from_staging_to_raw_task = PostgresOperator(
        task_id='move_authors_from_staging_to_raw',
        postgres_conn_id='postgres_conn',
        sql = '''
            INSERT INTO authors_raw
            SELECT * FROM authors_staging 
            ON CONFLICT DO NOTHING;
        '''
    ) 

    #!!!!
    dummy_task_join = DummyOperator(
        task_id = 'dummy_join_books_n_authors'
    )


    # Define the dependencies
    parse_new_books_page_task >> create_file_with_links_on_books_task >> fetch_books_from_books_txt_task
    copy_data_from_csv_to_books_staging_task >> dummy_task_join
    
    copy_data_from_csv_to_books_staging_task >> move_books_to_raw_task
    
    fetch_books_from_books_txt_task >> create_table_books_staging_task >> empty_staging_books_table_task >> copy_data_from_csv_to_books_staging_task 
    fetch_books_from_books_txt_task >> create_table_books_raw_task >> copy_data_from_csv_to_books_staging_task 
    
    fetch_books_from_books_txt_task >> create_file_with_links_on_authors_task >> fetch_authors_from_authors_txt_task >> create_staging_table_for_authors_task >> empty_staging_authors_table_task >> copy_data_from_csv_to_author_staging_task >> dummy_task_join
    fetch_authors_from_authors_txt_task >> create_raw_table_for_authors_task >> copy_data_from_csv_to_author_staging_task
    copy_data_from_csv_to_author_staging_task >> move_authors_from_staging_to_raw_task


    dummy_task_join >> remove_txt_with_links_task
    dummy_task_join >> remove_csv_with_books_task
    dummy_task_join >> remove_authors_txt_task
    dummy_task_join >> remove_authors_csv_task