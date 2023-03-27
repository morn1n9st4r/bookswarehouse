from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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

# Define the DAG


# Define a function that will read the CSV file and insert data into PostgreSQL
def copy_data(**kwargs):

    # Read the CSV file
    df = pd.read_csv('/books.csv')
    print(df.head())
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    with open('/opt/airflow/books.csv', 'r') as f:
        cursor.copy_expert('COPY books_raw FROM STDIN WITH (FORMAT CSV)', f)
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

def parse(url):
    bp = BookParser(url)
    try:
        df = bp.scrape_text()
        ret_array = df.loc[0, :].values.tolist()
        return [ret_array]
    except IndexError:
        pass


def fetch_books_from_new_books_page(**kwargs):
    links_books = '/opt/airflow/links.txt'
    target = '/opt/airflow/books.csv'

    urls = []
    with open(links_books) as f:
        urls = f.read().splitlines()

    with Pool(processes=4) as pool:
        data_list = []
        for data in pool.imap_unordered(parse, urls):
            try:
                data_list.extend(data)
            except (IndexError, TypeError):
                continue

    df = pd.DataFrame(data_list, columns=['BookTitle', 'Author', 'ISBN', 'EditionYear', 'Pages', 'Size',
                                         'CoverType', 'Language', 'CopiesIssued', 'AgeRestrictions',
                                         'Genres', 'TranslatorName', 'Rating', 'HaveRead', 'Planned',
                                         'Reviews', 'Quotes', 'Series', 'Edition'])
    print(df.shape)
    print(df.sample(15).to_string())
    df.to_csv(target, encoding='utf-8-sig', index=False)



def transform_str_to_list(passed_xcom_links):
    links = re.sub(r'\[|\]', '',re.sub(r"'", '',re.sub(r' ', '', str(passed_xcom_links))))
    return links.split(',')



def create_file_with_links_to_books(**kwargs):

    ti = kwargs['ti']

    links = ti.xcom_pull(key="links_on_new_books", task_ids=['parse_new_books_page'])

    links = transform_str_to_list(links)

    for link in links:
        print(link)

    with open(r'/opt/airflow/links.txt', 'w') as file_with_links:
        for link in links:
            file_with_links.write(f"{link}\n")


with DAG(
    'copy_data_to_postgres',
    start_date=datetime(2023, 3, 21),
    schedule_interval=None
) as dag:

    """ create_table_books_raw_task = PostgresOperator(
        task_id='create_table_books_raw',
        postgres_conn_id='postgres_conn',
        sql='''
        CREATE TABLE IF NOT EXISTS books_raw (
            BookTitle VARCHAR,
            Author VARCHAR,
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
    ) 

    copy_data_from_csv_to_books_raw_task = PythonOperator(
        task_id='copy_data_from_csv_to_books_raw',
        python_callable=copy_data
    ) """

    parse_new_books_page_task = PythonOperator(
        task_id='parse_new_books_page',
        python_callable=parse_new_books_page
    )


    create_file_with_links_to_books_task = PythonOperator(
        task_id='create_file_with_links_to_books',
        python_callable=create_file_with_links_to_books
    )

    fetch_books_from_new_books_page_task = PythonOperator(
        task_id='fetch_books_from_new_books_page',
        python_callable=fetch_books_from_new_books_page
    )

    # Define the dependencies
    parse_new_books_page_task >> create_file_with_links_to_books_task >> fetch_books_from_new_books_page_task
    #>> create_table_books_raw_task >> copy_data_from_csv_to_books_raw_task