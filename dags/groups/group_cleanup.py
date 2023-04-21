from airflow.utils.task_group import TaskGroup

from airflow.operators.bash_operator import BashOperator

def cleanup_tasks():
    with TaskGroup('cleanup', 
                   tooltip="""
                        remove .txt and .csv files 
                        after uploading data to tables
                   """) as group:
        
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
            bash_command="rm /opt/airflow/links_on_authors.txt",
        )

        remove_authors_csv_task = BashOperator(
            task_id='remove_authors_csv',
            bash_command="rm /opt/airflow/authors.csv",
        )

        remove_authors_txt_task = BashOperator(
            task_id='remove_publishers_txt',
            bash_command="rm /opt/airflow/links_on_publishers.txt",
        )

        remove_authors_csv_task = BashOperator(
            task_id='remove_publishers_csv',
            bash_command="rm /opt/airflow/publishers.csv",
        )
        return group