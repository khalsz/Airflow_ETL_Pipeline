import boto3
import pandas as pd
import csv
import io

from log.logging_config import setup_logger

logger = setup_logger(__name__)

def load_data_to_s3(url, rowsskip, seperator, bucket_name, s3_key): 
    try: 
        logger.info("starting data extration from source")
        datadf = pd.read_csv(url, skiprows=rowsskip, sep = seperator) 
        
        
        csv_buffer = io.StringIO()
        datadf.to_csv(csv_buffer, index= False)
        csv_content = csv_buffer.getvalue().decode('uft-8')
        
        s3_client = boto3.client('s3')
        
        s3_client.put_object(Body=csv_content, Bucket=bucket_name, key= s3_key)
        
        logger.info("successfully completed data extraction, cleaning and loading to AWS s3 bucket")
        return {'statusCode': 200, 
                'body': 'Successfully uploaded data to s3 bucket'}, datadf
    except Exception: 
        logger.error("error loading data file to s3 bucket")
        raise
            
