

from log.logging_config import setup_logger
import os
import pandas as pd
from os.path import join
from airflow.decorators import task
import re

# Initialize logger
logger = setup_logger(__name__)

@task()
def data_cleaning(input_data) -> pd.DataFrame: 
    """Cleans the input data by removing null columns, stripping spaces, replacing spaces and hyphens 
    in column names with underscores, and removing special characters. Finally, saves the cleaned 
    data to a CSV file.

    Args:
        input_data (str): JSON string containing the raw data.

    Returns:
        pd.DataFrame: The cleaned DataFrame.

    Raises:
        Exception: If an error occurs during data cleaning.
    """
    
    try: 
        logger.info("starting data cleaning process")
        
         # Convert JSON string to DataFrame
        input_data = pd.read_json(input_data)
        
        # Drop columns where all values are NaN
        input_data = input_data.dropna(axis=1, how="all")
        
        #stripping of trailing and leading space in column names
        input_data.columns = input_data.columns.str.strip()
        
        # replace space and hyphens in column names with underscore
        input_data.columns = input_data.columns.str.replace(' ', "_")
        input_data.columns = input_data.columns.str.replace('-', "_")
        
        # Remove special characters from column names
        input_data.columns = input_data.columns.map(lambda x: remove_special_characters(x))
        
        # dropping null value rows
        clean_data = input_data.dropna(axis=0)
        
        
        logger.info("saving cleaned data to csv")
        
        # saving clean data as csv to path
        clean_data.to_csv(join(os.getcwd(), 'clean_data.csv'))
        logger.info("successfully saved data to csv")
        
        return clean_data.to_json()
    
    except Exception: 
        logger.error("error performing data cleaning operation")
        raise


def remove_special_characters(text):
    # Use regex to replace all non-alphanumeric characters
    return re.sub(r'[^a-zA-Z0-9_]', '', text)
