import pandas as pd
from validate.schema import column_names
from log.logging_config import setup_logger
from airflow.decorators import task

logger = setup_logger(__name__)


@task()
def validator(input_data:pd.DataFrame): 
    logger.info("stating data validation/transformation process")
    exp_col_names = input_data.columns.sort()
    org_sortedcols = {key:column_names[key] for key in sorted(column_names)}
    
    try: 
        if exp_col_names == org_sortedcols.keys(): 
            print("all columns names are correct")
        
        try: 
            for i, col in enumerate(org_sortedcols): 
                datatype = org_sortedcols[col]
                if datatype == "float64":
                    pd.to_numeric(input_data.iloc[:, i], errors='coerce').astype('Int64') 
                if datatype == "object": 
                    input_data.iloc[:, i].astype("object")
            return True
        except Exception: 
            raise Exception("Error converting dataframe type")        
    except Exception: 
        logger.exception("error validating data, does not meet expected criteria")