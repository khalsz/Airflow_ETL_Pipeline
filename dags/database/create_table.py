from log.logging_config import setup_logger
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task


logger = setup_logger(__name__)


create_stat_list = ["jsgd"]

@task()
def create_table():
    
    
    try:
        for create_statement in create_stat_list: 
            logger.info("creating table")
            create_table_task = SQLExecuteQueryOperator(
                task_id='insert_into_table',
                conn_id='postgres_localhost',
                sql=create_statement,
            )
            create_table_task.execute(context=None)  # Execute the task
            
            logger.info("successfully create table")
        return True

    except Exception as e:
        logger.exception("Error creating table")
        raise 
    