
import pandas as pd
from log.logging_config import setup_logger
from airflow.decorators import task

logger = setup_logger(__name__)



@task()       
def generate_summary(input_clean_data, validate): 
    try: 
        logger.info("starting summary statistics of pollutants emission")
        sum_stat = input_clean_data.desc()
        
        custom_stats = {
            'median': input_clean_data.median(), 
            'standard_deviation': input_clean_data.std(), 
            'variance': input_clean_data.var()
            
        }
    
        summary_stat = pd.concat([sum_stat, pd.DataFrame(custom_stats)])
        summary_stat['summary_id'] = summary_stat.index
        logger.info("successfully calculated pollutants emission summary statistics")
        return summary_stat
    except Exception: 
        logger.exception("error calculating pollutants summary statistics")
        raise


@task()
def avg_emission_by_year(input_clean_data, validate):
    try:  
        logger.info("starting average pollutants emission calculation")
        sub_data = input_clean_data.drop(input_clean_data[0:2], axis=1)
        avg_emision = sub_data.groupby('Year').mean().reset_index()
        avg_emision['agg_id'] = avg_emision.index
        logger.info("successfully calculated average pollutants emission")
        return avg_emision
    except Exception: 
        logger.exception("error calculating average emission")
        raise


