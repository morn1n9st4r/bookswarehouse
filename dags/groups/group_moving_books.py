from airflow.utils.task_group import TaskGroup

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def copy_data_to_books_staging(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    with open('/opt/airflow/books.csv', 'r') as f:
        cursor.copy_expert('COPY books_staging FROM STDIN WITH (FORMAT CSV)', f)
    connection.commit()


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

def books_moving_tasks():
     with TaskGroup('store_books', 
                   tooltip="""
                        create tables for books
                        store in staging table
                        then append to raw table
                   """) as group:

        
        empty_staging_books_table_task = PostgresOperator(
            task_id='empty_staging_books_table',
            postgres_conn_id='postgres_conn',
            sql= '''
                TRUNCATE TABLE books_staging;
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

        create_table_books_staging_task >>empty_staging_books_table_task >>copy_data_from_csv_to_books_staging_task >> move_books_to_raw_task
        create_table_books_raw_task >> copy_data_from_csv_to_books_staging_task

        return group