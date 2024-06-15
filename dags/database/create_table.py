from log.logging_config import setup_logger
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task


logger = setup_logger(__name__)


create_statement1 = """
        CREATE TABLE IF NOT EXISTS emission (
            Entity VARCHAR(100),
            Code VARCHAR(100),
            Year INT,
            "Nitrogen oxide (NOx)" DECIMAL(10, 5), 
            "Sulphur dioxide (SO) emissions" DECIMAL(10, 5), 
            "Carbon monoxide (CO) emissions" DECIMAL(10, 5), 
            "Organic carbon (OC) emissions" DECIMAL(10, 5), 
            "Non-methane volatile organic compounds (NMVOC) emissions" DECIMAL(10, 5), 
            "Black carbon (BC) emissions" DECIMAL(10, 5),
            "Ammonia (NH3) emissions" DECIMAL(10, 5)
        );
"""

create_statement2 = """
        CREATE TABLE IF NOT EXISTS avg_emission (
            Year INT,
            "Nitrogen oxide (NOx)" DECIMAL(10, 5), 
            "Sulphur dioxide (SO) emissions" DECIMAL(10, 5), 
            "Carbon monoxide (CO) emissions" DECIMAL(10, 5), 
            "Organic carbon (OC) emissions" DECIMAL(10, 5), 
            "Non-methane volatile organic compounds (NMVOC) emissions" DECIMAL(10, 5), 
            "Black carbon (BC) emissions" DECIMAL(10, 5),
            "Ammonia (NH3) emissions" DECIMAL(10, 5)
        );
"""


create_statement3 = """
        CREATE TABLE IF NOT EXISTS emission_summary_stats (
            Year INT,
            summary_stats VARCHAR(100),
            "Nitrogen oxide (NOx)" DECIMAL(10, 5), 
            "Sulphur dioxide (SO) emissions" DECIMAL(10, 5), 
            "Carbon monoxide (CO) emissions" DECIMAL(10, 5), 
            "Organic carbon (OC) emissions" DECIMAL(10, 5), 
            "Non-methane volatile organic compounds (NMVOC) emissions" DECIMAL(10, 5), 
            "Black carbon (BC) emissions" DECIMAL(10, 5),
            "Ammonia (NH3) emissions" DECIMAL(10, 5)
        );
"""
create_stat_list = [create_statement1, create_statement2, create_statement3]

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

    except Exception as e:
        logger.exception("Error creating table")
        raise 
    