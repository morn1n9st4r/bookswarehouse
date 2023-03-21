from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import pandas as pd

# Define the DAG


# Define a function that will read the CSV file and insert data into PostgreSQL
def copy_data():
    
    # Read the CSV file
    df = pd.read_csv('/books.csv')
    print(df.head())
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    with open('/books.csv', 'r') as f:
        cursor.copy_expert('COPY books_raw FROM STDIN WITH (FORMAT CSV)', f)
    connection.commit()


# Define the tasks



with DAG(
    'copy_data_to_postgres',
    start_date=datetime(2023, 3, 21),
    schedule_interval=None
) as dag:

    create_table_books_raw_task = PostgresOperator(
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
    )
    # Define the dependencies
    create_table_books_raw_task >> copy_data_from_csv_to_books_raw_task