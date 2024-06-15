


import os
import pandas as pd
from os.path import join
from airflow.decorators import task



@task()
def data_cleaning(input_data) -> pd.DataFrame: 
    try: 
        
        input_data = pd.read_json(input_data)
        #stripping of trailing and leading space in column names
        input_data.columns = input_data.columns.str.strip()
        
        # replace space in column names with underscore
        input_data.columns = input_data.columns.str.replace(' ', "_")
        input_data.columns = input_data.columns.str.replace('[\(\)]', "")
        input_data.columns = input_data.columns.str.replace('-', "_")
        
        # dropping NA value rows
        clean_data = input_data.dropna(axis=0)
        
        # saving clean data as csv to path
        clean_data.to_csv(join(os.getcwd(), 'clean_data.csv'))
        
        return clean_data.to_json()
    
    except Exception as e: 
        raise("error performing data cleaning operation {e}")


    