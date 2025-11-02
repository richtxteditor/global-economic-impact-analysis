import pandas as pd
import os
import logging
from typing import Dict, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# (Your existing DataSource classes can remain here as they are well-defined)
class DataSource:
    """A base class for all data sources."""
    def __init__(self, name: str, data: pd.DataFrame):
        self.name = name
        self.data = data

    def get_data(self) -> pd.DataFrame:
        return self.data

class Sipri(DataSource):
    """Represents a data source from SIPRI."""
    pass

class WorldBank(DataSource):
    """Represents a data source from the World Bank."""
    pass

class Bloomberg(DataSource):
    """Represents a data source from Bloomberg."""
    pass


def get_processed_data_path(filename: str) -> str:
    """Constructs the absolute path to a processed data file."""
    return os.path.join(os.path.dirname(__file__), '..', 'data', 'processed', filename)

def load_all_processed_data() -> Dict[str, DataSource]:
    """
    Loads all preprocessed CSV files from the 'data/processed' directory
    into DataSource objects.
    """
    processed_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
    data_sources = {}

    if not os.path.exists(processed_dir):
        logging.error(f"Processed data directory not found: {processed_dir}")
        logging.error("Please run the data pipeline first using 'make pipeline'.")
        return {}

    for filename in os.listdir(processed_dir):
        if filename.endswith('.csv'):
            dataset_name = filename.replace('.csv', '')
            file_path = get_processed_data_path(filename)
            
            try:
                # Determine the correct index column to use when loading
                # Default to the first column if not specified
                df = pd.read_csv(file_path, index_col=0)
                
                # Simple logic to assign the correct class type
                if "arms" in dataset_name:
                    data_sources[dataset_name] = Sipri(dataset_name, df)
                elif "gdp" in dataset_name:
                    data_sources[dataset_name] = WorldBank(dataset_name, df)
                elif "bloomberg" in dataset_name:
                    data_sources[dataset_name] = Bloomberg(dataset_name, df)
                else:
                    data_sources[dataset_name] = DataSource(dataset_name, df)
                    
                logging.info(f"Loaded processed dataset: {dataset_name}")

            except Exception as e:
                logging.error(f"Failed to load {filename}: {e}")

    return data_sources