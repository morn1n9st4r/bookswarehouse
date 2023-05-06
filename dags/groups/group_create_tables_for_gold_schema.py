from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

def create_and_fill_other_golden_tables_tasks():
    with TaskGroup('create_golden_tables_for_publishers_and_authors',
                   tooltip="""
                        create authors and publishers tables and
                        move data from silver to golden schema
                   """) as group:

        # use PostgresOperators for creating golden tables with constraints

        create_golden_authors_table_task = PostgresOperator(
            task_id='create_golden_authors_table',
            postgres_conn_id='postgres_conn',
            sql = """
                CREATE TABLE IF NOT EXISTS gold.authors (
                    authorid VARCHAR PRIMARY KEY UNIQUE NOT NULL,
                    name VARCHAR,
                    originalname VARCHAR,
                    liked INT,
                    neutral INT,
                    disliked INT,
                    favorite INT,
                    reading INT
                )
            """
        )

        create_golden_publishers_table_task = PostgresOperator(
            task_id='create_golden_publishers_table',
            postgres_conn_id='postgres_conn',
            sql = """
                CREATE TABLE IF NOT EXISTS gold.publishers (
                    publisherid VARCHAR PRIMARY KEY,
                    "name" VARCHAR,
                    books INT,
                    years INT,
                    page VARCHAR,
                    favorite INT
                )
            """
        )


        move_from_silver_to_golden_authors_task = PostgresOperator(
            task_id='move_from_silver_to_golden_author',
            postgres_conn_id='postgres_conn',
            sql = '''
                INSERT INTO gold.authors
                SELECT * FROM silver.authors
                ON CONFLICT (authorid) DO NOTHING
            '''
        )


        move_from_silver_to_golden_publishers_task = PostgresOperator(
            task_id='move_from_silver_to_golden_publishers',
            postgres_conn_id='postgres_conn',
            sql = '''
                INSERT INTO gold.publishers
                SELECT * FROM silver.publishers
                ON CONFLICT (publisherid) DO NOTHING
            '''
        )


        create_golden_authors_table_task >> move_from_silver_to_golden_authors_task
        create_golden_publishers_table_task >> move_from_silver_to_golden_publishers_task

        return group
