
import matplotlib.pyplot  as plt
import pandas as pd
from log.logging_config import setup_logger
from airflow.decorators import task

logger = setup_logger(__name__)

@task()
def visualization(input_clean_data, validate, save_path): 
    try: 
        logger.info("starting data visualization")
        
        # convert json data to DataFrame
        input_clean_data = pd.read_json(input_clean_data)
        
        data_to_plot = input_clean_data.drop(["Entity", "Code"], axis = 1)
        plt.figure(figsize=(8, 6))
        for polutants in data_to_plot.columns: 
            if polutants != 'Year': 
                plt.plot(data_to_plot['Year'], data_to_plot[polutants], label=polutants)
        plt.title("Times Series Distribution of Polutants Emission")
        plt.xlabel('Year')
        plt.ylabel('Polutants')
        plt.grid(True)
        
        # Saving the plot to the specified path
        plt.savefig(f'{save_path}/line_chart.png')
        plt.show()
    except Exception: 
        logger.exception("error visualizing data")