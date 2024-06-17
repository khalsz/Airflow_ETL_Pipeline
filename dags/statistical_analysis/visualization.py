
import matplotlib.pyplot as plt
import pandas as pd
from log.logging_config import setup_logger
from airflow.decorators import task

logger = setup_logger(__name__)

@task()
def visualization(val_data,  save_path): 
    
    """Generates and saves a line plot visualizing the time series distribution of pollutants emission.

    Args:
        val_data (str): JSON string containing the data to be visualized.
        save_path (str): Local directory path where the plot image will be saved.

    Returns:
        None

    Raises:
        Exception: If an error occurs during data visualization.
    """
    
    try: 
        logger.info("starting data visualization")
        
        # Convert JSON string to DataFrame
        val_data = pd.read_json(val_data)
        
        # Drop columns that are not needed for the visualization
        data_to_plot = val_data.drop(["Entity", "Code"], axis = 1)
        
        plt.figure(figsize=(8, 6))
        
        # Plot each pollutant's time series data
        for polutants in data_to_plot.columns: 
            if polutants != 'Year': 
                plt.plot(data_to_plot['Year'], data_to_plot[polutants], label=polutants)
        
        # Add title and labels to the plot
        plt.title("Times Series Distribution of Polutants Emission")
        plt.xlabel('Year')
        plt.ylabel('Polutants')
        plt.grid(True)
        
        # Saving the plot to the specified path
        plt.savefig(f'{save_path}/line_chart.png')
        plt.show()
    except Exception: 
        logger.exception("error visualizing data")