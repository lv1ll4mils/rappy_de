# 1. Mejoras en imports (más organizados)
import os, zipfile, logging
from pathlib import Path  # <- Más moderno que os.path
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from snowflake.connector.pandas_tools import write_pandas
from dags.utils.connections import get_snowflake_connection


logger = logging.getLogger(__name__)


# 2. Constantes claras (MAYÚSCULAS)
DOWNLOAD_PATH = Path('/tmp/meetup_data')
DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)


# 4. Función modular para carga de archivos
def load_csv_to_snowflake(conn, file_path, table_name):
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='latin-1')

    success, _, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=True,
        overwrite=True
    )
    return nrows

def setup_kaggle():
    """Configura Kaggle solo cuando se llama explícitamente"""
    api = KaggleApi()
    api.authenticate()
    return api


# 5. Flujo principal más claro
def run_el():
    # Configuración Kaggle
    os.environ['KAGGLE_CONFIG_DIR'] = '/home/luigi/back_up/rappy_de/.secrets'

    api = setup_kaggle()

    # Descarga de datos
    api.dataset_download_files('megelon/meetup', path=str(DOWNLOAD_PATH), unzip=True)

    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE IF EXISTS MEETUP_DB")
        cursor.execute("CREATE DATABASE MEETUP_DB")
        cursor.execute("CREATE SCHEMA MEETUP_DB.RAW_DATA")
        cursor.execute("USE DATABASE MEETUP_DB")
        cursor.execute("USE SCHEMA RAW_DATA")

        #allowed_files = {'categories.csv', 'cities.csv', 'events.csv', 'groups.csv', 'groups_topics.csv', 'topics.csv', 'venues.csv'}
        # Procesar archivos
        for file in DOWNLOAD_PATH.glob('*.csv'):
            #if file.name in allowed_files:
                #print("Archivos encontrados:", [f.name for f in DOWNLOAD_PATH.glob('*')])
                table_name = file.stem.upper()
                logger.info(f'Processing {file.name}')
                row_count = load_csv_to_snowflake(conn, file, table_name)
                logger.info(f'Loaded {row_count} rows into {table_name}')


if __name__ == "__main__":
    run_el()