from sqlalchemy import create_engine
from log.logging_config import setup_logger
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
import pandas as pd

logger = setup_logger(__name__)


@task()
def insert_data_into_table( input_clean_data: pd.DataFrame, table_name: str):
    
    """Inserts data from a DataFrame into a SQL table using Airflow's SQLExecuteQueryOperator.

    Args:
        table_name (str): Name of the target table in the database.
        input_clean_data (pd.DataFrame): The DataFrame containing the data to insert.

    Returns:
        None

    Raises:
        Exception: If an error occurs during SQL execution.
    """

    data = input_clean_data.to_records(index=False).tolist()
    column_names = ", ".join(input_clean_data.columns)
    query_statement = f"""
        INSERT INTO {table_name} ({column_names})
        VALUES ({','.join(['%s' for _ in input_clean_data.columns])})
    """

    try:
        logger.info("Inserting data into table")
        insert_data_task = SQLExecuteQueryOperator(
            task_id='insert_into_table',
            conn_id='postgres_localhost',
            sql=query_statement,
            params=data
        )
        insert_data_task.execute(context=None)  # Execute the task
        logger.info("successfully insert data into table")
    except Exception as e:
        logger.exception("Error inserting data into table")
        raise 