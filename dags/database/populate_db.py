from log.logging_config import setup_logger
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
import pandas as pd


# Initialize logger
logger = setup_logger(__name__)


@task()
def insert_into_db_table( input_data, table_name: str):
    
    """Inserts data from a DataFrame into a SQL table using Airflow's PostgresOperator.

    Args:
        table_name (str): Name of the target table in the database.
        input_data (pd.DataFrame): The DataFrame containing the data to insert.

    Returns:
        None

    Raises:
        Exception: If an error occurs during SQL execution.
    """
    
    
    # converting json data into DataFrame
    data = pd.read_json(input_data)
    
    # extracting values from dataframe as slit 
    data_list = data.values
    
    column_names = ", ".join(data.columns)
    
    # appending values as tuple 
    values = []
    for row in data_list: 
        values.append(tuple(row))
    value_statement = ','.join([f"{val}" for val in values])
    
    # insert statement initialization
    query_statement = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES {value_statement}
        """
    try:
        logger.info("Inserting data into table")
        insert_data_task = SQLExecuteQueryOperator(
            task_id='insert_into_table',
            conn_id='postgres_localhost',
            sql=query_statement,
        )
        insert_data_task.execute(context=None)  # Execute the task
        
        logger.info("successfully insert data into table")
    
    except Exception as e:
        logger.exception("Error inserting data into table")
        raise 



