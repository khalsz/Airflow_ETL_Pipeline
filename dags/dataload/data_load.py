import pandas as pd
from airflow.decorators import task
from log.logging_config import setup_logger


logger = setup_logger(__name__)

@task()
def load_data(url,  save_path): 
    try: 
        logger.info("starting data extration from source")
        datadf = pd.read_csv(url, encoding = "ISO-8859-1") 
        
        datadf.to_csv(f'{save_path}/emission_data.csv', index= False)
        
        logger.info("successfully completed data extraction, cleaning and loading to local drive")
        return datadf.to_json()
    
    except Exception: 
        logger.error("error loading data to local drive")
        raise
            
