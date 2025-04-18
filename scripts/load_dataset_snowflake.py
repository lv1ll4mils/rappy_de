# 1. Mejoras en imports (más organizados)
import os, zipfile, logging
from pathlib import Path  # <- Más moderno que os.path
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from snowflake.connector.pandas_tools import write_pandas
from dags.utils.connections import get_snowflake_connection


logger = logging.getLogger(__name__)


DOWNLOAD_PATH = Path('/tmp/meetup_data')
DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)

def load_csv_to_snowflake(conn, file_path, table_name):
    """Load a CSV file into a Snowflake table.

    Args:
        conn: Active Snowflake connection.
        file_path (Path): Local CSV file path.
        table_name (str): Destination table name in Snowflake.

    Returns:
        int: Number of rows loaded.
    """
    chunksize = 900000
    total_rows = 0
    first_chunk = True
    
    encodings = ['utf-8', 'latin-1']
    
    for encoding in encodings:
        try:
            logger.info(f"Trying encoding: {encoding}")
            for chunk in pd.read_csv(file_path, chunksize=chunksize, encoding=encoding):
                success, _, nrows, _ = write_pandas(
                    conn,
                    chunk,
                    table_name,
                    auto_create_table=first_chunk,
                    overwrite=first_chunk
                )
                total_rows += nrows
                first_chunk = False
            break
        except UnicodeDecodeError as e:
            logger.warning(f"Encoding '{encoding}' failed: {e}")
            continue
    else:
        raise UnicodeDecodeError("Both utf-8 and latin-1 encodings failed.")
    return total_rows

def setup_kaggle():
    """Authenticate and return a Kaggle API client.

    Returns:
        KaggleApi: Authenticated Kaggle API instance.
    """
    api = KaggleApi()
    api.authenticate()
    return api


def run_el():
    """Download dataset from Kaggle and load it into Snowflake."""
    # Configuración Kaggle
    os.environ['KAGGLE_CONFIG_DIR'] = '/home/luigi/back_up/rappy_de/.secrets'

    api = setup_kaggle()

    api.dataset_download_files('megelon/meetup', path=str(DOWNLOAD_PATH), unzip=True)

    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE IF EXISTS MEETUP_DB")
        cursor.execute("CREATE DATABASE MEETUP_DB")
        cursor.execute("CREATE SCHEMA MEETUP_DB.RAW_DATA")
        cursor.execute("USE DATABASE MEETUP_DB")
        cursor.execute("USE SCHEMA RAW_DATA")

        #allowed_files = {'categories.csv', 'cities.csv', 'events.csv', 'groups.csv', 'groups_topics.csv', 'topics.csv', 'venues.csv'}
        for file in DOWNLOAD_PATH.glob('*.csv'):
            #if file.name in allowed_files:
                table_name = file.stem.upper()
                logger.info(f'Processing {file.name}')
                row_count = load_csv_to_snowflake(conn, file, table_name)
                logger.info(f'Loaded {row_count} rows into {table_name}')


if __name__ == "__main__":
    run_el()
