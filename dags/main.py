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


default_args = {
    'owner': 'khalid', 
    'retries': 5, 
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id = "Air_pollution_data_pipeline", 
    default_args=default_args, 
    start_date=datetime(2024, 6, 14, 2), 
    schedule='@daily')
def air_pollution_etl (data_url, path): 

    datadf = load_data(url=data_url,  save_path=path) 
    
    clean_data = data_cleaning(datadf)

    creat_tbl = create_table()
    
    val_data = validator(clean_data)
    
    insert_into_db_table(val_data, "emission").set_upstream(creat_tbl)
    
    avg_emmision = avg_emission_by_year(val_data)
    
    insert_into_db_table(avg_emmision, 'avg_emission').set_upstream(creat_tbl)
    
    summary_stat = generate_summary(val_data)
    
    insert_into_db_table(summary_stat, 'emission_summary_stats').set_upstream(creat_tbl)
    
    visualization(val_data, save_path=path)
    
pollution_analysis = air_pollution_etl(data_url, save_data_to) 

    
# if __name__ == "__main__": 
#     data_url = "https://dmldatasets.s3.amazonaws.com/datasets/apache-airflow/air-pollution.csv"
#     save_data_to = "/opt/airflow/save"
#     pollution_analysis = air_pollution_etl(data_url, save_data_to)   
    
