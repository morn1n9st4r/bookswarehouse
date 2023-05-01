from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator

from groups.group_create_tables_for_gold_schema import create_and_fill_other_golden_tables_tasks

def spark_transformations_tasks():
     with TaskGroup('make_spark_transformations', 
                   tooltip="""
                        make transformations for tables
                   """) as group:

        #remove_constraints_task = PostgresOperator(
        #    task_id='remove_constraints',
        #    postgres_conn_id='postgres_conn',
        #    sql = [""" select 1"""
        #        """ALTER TABLE gold.books DROP CONSTRAINT fk_books_authors;""",
        #        """ALTER TABLE gold.books DROP CONSTRAINT fk_books_publishers;""",
        #        """ALTER TABLE gold.book_genre DROP CONSTRAINT fk_genre_book;""",
        #        """ALTER TABLE gold.book_genre DROP CONSTRAINT fk_book_genre;"""
        #])

        #add_constraints_task = PostgresOperator(
        #task_id='add_constraints',
        #    postgres_conn_id='postgres_conn',
        #    sql = [
        #    """ALTER TABLE gold.books ADD PRIMARY KEY (id);""",
        #    """ALTER TABLE gold.genres ADD PRIMARY KEY (id);""",
        #    """ALTER TABLE gold.books ADD CONSTRAINT fk_books_authors FOREIGN KEY (authorid) REFERENCES gold.authors(authorid);""",
        #    """ALTER TABLE gold.books ADD CONSTRAINT fk_books_publishers FOREIGN KEY (publisherid) REFERENCES gold.publishers(publisherid);""",
        #    """ALTER TABLE gold.book_genre ADD CONSTRAINT fk_genre_book FOREIGN KEY (book_id) REFERENCES gold.books(id);""",
        #    """ALTER TABLE gold.book_genre ADD CONSTRAINT fk_book_genre FOREIGN KEY (genre_id) REFERENCES gold.genres(id);"""
        #])

        silver_books_task = SparkSubmitOperator(
            task_id = "silver_books_transformations",
            application = "/opt/airflow/jars/spark_transformations.jar",
            java_class="SilverDataTransformations",
            driver_class_path="/opt/airflow/jars/postgresql-42.6.0.jar",
            conn_id = "spark_default"
        )

        silver_authors_task = SparkSubmitOperator(
            task_id = "silver_authors_transformations",
            application = "/opt/airflow/jars/spark_transformations.jar",
            java_class="SilverAuthorsTransformations",
            driver_class_path="/opt/airflow/jars/postgresql-42.6.0.jar",
            conn_id = "spark_default"
        )

        silver_publishers_task = SparkSubmitOperator(
            task_id = "silver_publishers_transformations",
            application = "/opt/airflow/jars/spark_transformations.jar",
            java_class="SilverPublishersTransformations",
            driver_class_path="/opt/airflow/jars/postgresql-42.6.0.jar",
            conn_id = "spark_default"
        )


        golden_books_task = SparkSubmitOperator(
            task_id = "golden_books_transformations",
            application = "/opt/airflow/jars/spark_transformations.jar",
            java_class="GoldenDataTransformations",
            driver_class_path="/opt/airflow/jars/postgresql-42.6.0.jar",
            conn_id = "spark_default"
        )


        authors_and_publisher_golden_tables = create_and_fill_other_golden_tables_tasks()

        #remove_constraints_task >> 
        silver_books_task >> golden_books_task
        #remove_constraints_task >> 
        silver_authors_task >> golden_books_task
        #remove_constraints_task >> 
        silver_publishers_task >> golden_books_task

        golden_books_task >> authors_and_publisher_golden_tables 
        # >> add_constraints_task

        return group