import pygeohash as pgh
import pandas as pd
import numpy as np

import pandas as pd
import logging
from clickhouse_driver import Client
import os
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
from DB.DBConstants import *
from Functions.time_stamp import *
from Functions.helper_functions import *
logger.debug(f"Imported all libraries")
logger.debug(f"{time_period}")
logger.debug(f"Ride SQL queries")

def insert_data_from_csv(csv_file_path, table_name):
    # Load CSV data into a pandas DataFrame
    try:
        df = pd.read_csv(csv_file_path)
        logger.debug(f"CSV loaded successfully from {csv_file_path}. Number of records: {len(df)}")
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        return

    # Connect to ClickHouse
    client = connect_clickhouse()

    # Convert DataFrame to a list of tuples (this will be inserted into ClickHouse)
    data_to_insert = df.values.tolist()

    # Insert data into ClickHouse table
    try:
        # ClickHouse insert statement
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES"
        
        # Execute insert
        chunk_size = 1000  # Number of records to insert per batch
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start+chunk_size]
            data_to_insert = chunk.values.tolist()
            client.execute(insert_query, data_to_insert)
        client.execute(insert_query, data_to_insert)
        logger.debug(f"Data successfully inserted into the {table_name} table.")
    except Exception as e:
        logger.error(f"Error inserting data into ClickHouse: {e}")


