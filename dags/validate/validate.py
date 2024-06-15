import pandas as pd
from validate.schema import column_names
from log.logging_config import setup_logger
from airflow.decorators import task

logger = setup_logger(__name__)


@task()
def validator(input_clean_data): 
    logger.info("stating data validation/transformation process")
    
    # convert json input to DataFrame
    input_clean_data = pd.read_json(input_clean_data)
    
    # sorting the columns in alphabetical order
    exp_col_names = sorted(input_clean_data.columns)
    org_sortedcols = {key:column_names[key] for key in sorted(column_names)}
    
    try: 
        if exp_col_names == org_sortedcols.keys(): 
            print("all columns names are correct")
        
        try: 
            for i, col in enumerate(org_sortedcols): 
                datatype = org_sortedcols[col]
                if datatype == "float64":
                    pd.to_numeric(input_clean_data.iloc[:, i], errors='coerce').astype('Int64') 
                if datatype == "object": 
                    input_clean_data.iloc[:, i].astype("object")
            return True
        except Exception: 
            raise Exception("Error converting dataframe type")        
    except Exception: 
        logger.exception("error validating data, does not meet expected criteria")