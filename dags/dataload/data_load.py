import pandas as pd
from airflow.decorators import task
from log.logging_config import setup_logger


logger = setup_logger(__name__)

@task()
def load_data(url,  save_path): 
    """Extracts data from a given URL, saves it to a local file, and returns the data as a JSON string.

    Args:
        url (str): URL of the CSV file to be downloaded.
        save_path (str): Local directory path where the CSV file will be saved.

    Returns:
        str: JSON string of the loaded DataFrame.

    Raises:
        Exception: If an error occurs during the data extraction, saving, or loading process.
    """
    try: 
        logger.info("starting data extration from source")
        
        # Read the CSV file from the URL
        datadf = pd.read_csv(url, encoding = "ISO-8859-1") 
        
        datadf.to_csv(f'{save_path}/emission_data.csv', index= False)
        
        logger.info("successfully completed data extraction, cleaning and loading to local drive")
        
        return datadf.to_json()
    
    except Exception: 
        logger.error("error loading data to local drive")
        raise
            
