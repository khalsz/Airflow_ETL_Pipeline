

from log.logging_config import setup_logger

logger = setup_logger(__name__)

def data_cleaning(input_data): 
    try: 
        logger.info("starting data cleaning process")
        #stripping of trailing and leading space in column names
        input_data.columns = input_data.columns.str.strip()
        
        # replace space in column names with underscore
        input_data.columns = input_data.columns.str.replace(' ', "_")
        
        # dropping NA value rows
        clean_data = input_data.dropna(axis=0)
        return clean_data
    
    except Exception: 
        logger.error("error performing data cleaning operation")
        raise