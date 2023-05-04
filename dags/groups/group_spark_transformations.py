from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

from groups.group_create_tables_for_gold_schema import create_and_fill_other_golden_tables_tasks

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

        ge_validate_books = GreatExpectationsOperator(
            task_id='validate_transformations_of_books',
            data_context_root_dir="/opt/airflow/great_expectations/",
            checkpoint_name = "gold_books",
            return_json_dict=True,
            fail_task_on_validation_failure=True
        )

        ge_validate_authors = GreatExpectationsOperator(
            task_id='validate_transformations_of_authors',
            data_context_root_dir="/opt/airflow/great_expectations/",
            checkpoint_name = "gold_authors",
            return_json_dict=True,
            fail_task_on_validation_failure=True
        )

        ge_validate_publishers = GreatExpectationsOperator(
            task_id='validate_transformations_of_publishers',
            data_context_root_dir="/opt/airflow/great_expectations/",
            checkpoint_name = "gold_publishers",
            return_json_dict=True,
            fail_task_on_validation_failure=True
        )



        authors_and_publisher_golden_tables = create_and_fill_other_golden_tables_tasks()


        silver_books_task >> ge_validate_books >> golden_books_task
        silver_authors_task >> ge_validate_authors >> golden_books_task
        silver_publishers_task >> ge_validate_publishers >> golden_books_task
        golden_books_task >> authors_and_publisher_golden_tables 


        return group