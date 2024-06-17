import pandas as pd
from validate.schema import column_names
from log.logging_config import setup_logger
from airflow.decorators import task

logger = setup_logger(__name__)


@task()
def validator(input_clean_data): 
    """Validates and transforms a DataFrame loaded from JSON data.

    Args:
        input_clean_data (str): The JSON string representing the DataFrame.
        expected_column_names (dict): A dictionary mapping actual column names
                                       to their expected data types (e.g., "float64", "object").

    Returns:
        str: The JSON string representing the validated and potentially transformed DataFrame.

    Raises:
        Exception: If an error occurs during data validation or transformation.
    """

    logger.info("stating data validation/transformation process")
    
    # convert json input to DataFrame
    input_clean_data = pd.read_json(input_clean_data)
    
    # # sorting the columns in alphabetical order
    real_sortedcols = sorted(input_clean_data.columns)
    exp_col_names  = {key:column_names[key] for key in sorted(column_names)}

    try: 
        # Validate column names
        if list(exp_col_names.keys()) == real_sortedcols: 
            print("all columns names are correct")
            
        try: 
            # Validate data types and transform if necessary
            for i, val in enumerate(exp_col_names.values()): 
                datatype = val
                subcol = real_sortedcols[i]
                if datatype == "float64":
                    pd.to_numeric(input_clean_data[subcol], errors='coerce').astype('float64') 
                if datatype == "object": 
                    input_clean_data[subcol].astype("object")
            validated_data = input_clean_data.copy()
            return validated_data.to_json()
        except Exception: 
            raise Exception("Error converting dataframe type")        
    except Exception: 
        logger.exception("error validating data, does not meet expected criteria")
        
