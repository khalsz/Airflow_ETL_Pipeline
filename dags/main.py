from datetime import datetime, timedelta
from airflow.decorators import  dag
from dataload.data_load import load_data
from validate.validate import validator
from dataload.processing.data_clean import data_cleaning
from statistical_analysis.summary_analysis import avg_emission_by_year,  generate_summary
from statistical_analysis.visualization import visualization
from database.populate_db import insert_into_db_table
from database.create_table import create_table



data_url = "https://dmldatasets.s3.amazonaws.com/datasets/apache-airflow/air-pollution.csv"
save_data_to = "/opt/airflow"

# Define default arguments for the DAG
default_args = {
    'owner': 'khalid', 
    'retries': 5, 
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id = "Air_pollution_data_pipeline", 
    default_args=default_args, 
    start_date=datetime(2024, 6, 19, 2), 
    schedule='@daily')
def air_pollution_etl (data_url, path): 

    """
    Airflow DAG for an ETL pipeline to process air pollution data.

    Args:
        data_url (str): URL of the data source.
        path (str): Local directory path where data will be saved or processed.

    Returns:
        None
    """

    # Task to load data from the specified URL and save it locally
    datadf = load_data(url=data_url,  save_path=path) 
    
    # Task to clean the loaded data
    clean_data = data_cleaning(datadf)
    
    # Task to validate and transform the cleaned data
    val_data = validator(clean_data)
    
    # Task to generate summary statistics of pollutants emission
    summary_stat = generate_summary(val_data)
    
    # Task to calculate average emissions by year
    avg_emmision = avg_emission_by_year(val_data)
    
    # Task to create tables in the database if they don't exist
    creat_tbl = create_table()
    creat_tbl.set_upstream([summary_stat, avg_emmision])
    
    # Task to insert validated data into 'emission' table
    insert_into_db_table(val_data, "emission").set_upstream(creat_tbl)
    
    
    # Task to insert average emission data into 'avg_emission' table
    insert_into_db_table(avg_emmision, 'avg_emission').set_upstream(creat_tbl)
    
    
    # Task to insert summary statistics into 'emission_summary_stats' table
    insert_into_db_table(summary_stat, 'emission_summary_stats').set_upstream(creat_tbl)
    
    # Task to visualize the data and save the plot
    visualization(val_data, save_path=path)

# Define the DAG structure
pollution_analysis = air_pollution_etl(data_url, save_data_to) 

    

