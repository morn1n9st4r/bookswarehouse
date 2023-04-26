from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import pandas as pd
import re
import json

from billiard import Pool

from groups.group_cleanup import cleanup_tasks
from groups.group_parsing_authors import author_parsing_tasks
from groups.group_moving_books import books_moving_tasks
from groups.group_parsing_publishers import publisher_parsing_tasks
from groups.group_spark_transformations import spark_transformations_tasks

import sys
sys.path.append('/opt/airflow/dags/parser/')
from NewBooksParser import NewBooksParser
from BookParser import BookParser


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
    return list(set(links.split(',')))


def create_file_with_links_on_books(**kwargs):
    ti = kwargs['ti']
    links = ti.xcom_pull(key="links_on_new_books", task_ids=['parse_new_books_page'])
    links = transform_str_to_list(links)
    with open(r'/opt/airflow/links_on_books.txt', 'w') as file_with_links:
        for link in links:
            file_with_links.write(f"{link}\n")


def parse_book(url):
    bp = BookParser(url)
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
                                         'Reviews', 'Quotes', 'Series', 'PublisherID'
                                        ]
                    )
    
    ti.xcom_push(key="ids_of_authors", value=df['AuthorID'].tolist())
    ti.xcom_push(key="ids_of_publishers", value=df['PublisherID'].tolist())
    df.to_csv(target, encoding='utf-8-sig', index=False, header=False)

with DAG(
    'copy_data_to_postgres',
    start_date=datetime(2023, 3, 21),
    schedule_interval=None
) as dag:


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

    cleanup = cleanup_tasks()
    move_books = books_moving_tasks()
    parse_authors = author_parsing_tasks()
    parse_publishers = publisher_parsing_tasks()
    transformations = spark_transformations_tasks()

    parse_new_books_page_task >> create_file_with_links_on_books_task >> fetch_books_from_books_txt_task
    fetch_books_from_books_txt_task >> move_books >> cleanup
    fetch_books_from_books_txt_task >> parse_authors >> cleanup
    fetch_books_from_books_txt_task >> parse_publishers >> cleanup
    cleanup >> transformations