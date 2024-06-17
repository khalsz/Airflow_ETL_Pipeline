from log.logging_config import setup_logger
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task

# Initialize logger
logger = setup_logger(__name__)

# SQL statements to create the emission tables
create_statement1 = """
        CREATE TABLE IF NOT EXISTS emission (
            Entity VARCHAR(100),
            Code VARCHAR(100),
            Year INT,
            "Nitrogen_oxide_NOx" FLOAT, 
            "Sulphur_dioxide_SO_emissions" FLOAT, 
            "Carbon_monoxide_CO_emissions" FLOAT, 
            "Organic_carbon_OC_emissions" FLOAT, 
            "Non_methane_volatile_organic_compounds_NMVOC_emissions" FLOAT, 
            "Black_carbon_BC_emissions" FLOAT,
            "Ammonia_NH_emissions" FLOAT
        );
"""

# SQL statements to create the avg_emission tables
create_statement2 = """
        CREATE TABLE IF NOT EXISTS avg_emission (
            Year INT,
            "Nitrogen_oxide_NOx" FLOAT, 
            "Sulphur_dioxide_SO_emissions" FLOAT, 
            "Carbon_monoxide_CO_emissions" FLOAT, 
            "Organic_carbon_OC_emissions" FLOAT, 
            "Non_methane_volatile_organic_compounds_NMVOC_emissions" FLOAT, 
            "Black_carbon_BC_emissions" FLOAT,
            "Ammonia_NH_emissions" FLOAT
        );
"""

# SQL statements to create the emission_summary_stats tables
create_statement3 = """
        CREATE TABLE IF NOT EXISTS emission_summary_stats (
            Year INT,
            summary_stats VARCHAR(100),
            "Nitrogen_oxide_NOx" FLOAT, 
            "Sulphur_dioxide_SO_emissions" FLOAT, 
            "Carbon_monoxide_CO_emissions" FLOAT, 
            "Organic_carbon_OC_emissions" FLOAT, 
            "Non_methane_volatile_organic_compounds_NMVOC_emissions" FLOAT, 
            "Black_carbon_BC_emissions" FLOAT,
            "Ammonia_NH_emissions" FLOAT
        );
"""

# List of SQL create table statements
create_stat_list = [create_statement1, create_statement2, create_statement3]

@task()
def create_table():
    """Creates tables in the PostgreSQL database.

    This task iterates over a list of SQL create table statements and 
    executes each one using Airflow's PostgresOperator.

    Returns:
        None

    Raises:
        Exception: If an error occurs during SQL execution.
    """
    try:
        for create_statement in create_stat_list: 
            logger.info("creating table")
            create_table_task = SQLExecuteQueryOperator(
                task_id='create_table',
                conn_id='postgres_localhost',
                sql=create_statement,
            )
            create_table_task.execute(context=None)  # Execute the task
            
            logger.info("successfully create table")

    except Exception as e:
        logger.exception("Error creating table")
        raise 
    