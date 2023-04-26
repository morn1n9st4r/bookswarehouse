from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def spark_transformations_tasks():
     with TaskGroup('make_spark_transformations', 
                   tooltip="""
                        make transformations for tables
                   """) as group:


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

        silver_books_task >> golden_books_task
        silver_authors_task >> golden_books_task
        silver_publishers_task >> golden_books_task

        return group