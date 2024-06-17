
import pandas as pd
from log.logging_config import setup_logger
from airflow.decorators import task

logger = setup_logger(__name__)



@task()       
def generate_summary(val_data): 
    
    """Generates summary statistics for numerical columns in the input data.

    Args:
        val_data (str): JSON string containing the data to be summarized.

    Returns:
        str: JSON string of the summary statistics DataFrame.

    Raises:
        Exception: If an error occurs during the summary statistics generation.
    """
    
    try: 
        logger.info("starting summary statistics of pollutants emission")
        
        # convert json to DataFrame 
        data_df = pd.read_json(val_data)
        
        # Select only numerical columns for summary statistics
        num_data =  data_df.select_dtypes(include='number')
        
        # Calculate summary statistics
        sum_stat = num_data.describe()
        
        # Add a column for summary statistics labels
        sum_stat['summary_stats'] = sum_stat.index
        
        # Reset the index to turn the labels into a column
        sum_stat.reset_index(drop=True, inplace=True)
        
        logger.info("successfully calculated pollutants emission summary statistics")
        return sum_stat.to_json()
    
    except Exception: 
        logger.exception("error calculating pollutants summary statistics")
        raise


@task()
def avg_emission_by_year(val_data):
    try:  
        logger.info("starting average pollutants emission calculation")
        
        # convert json data to DataFrame 
        datadf = pd.read_json(val_data)
        
        num_data =  datadf.select_dtypes(include='number')
        
        avg_emision = num_data.groupby('Year').mean().reset_index()
        logger.info("successfully calculated average pollutants emission")
        
        return avg_emision.to_json()
    
    except Exception: 
        logger.exception("error calculating average emission")
        raise


