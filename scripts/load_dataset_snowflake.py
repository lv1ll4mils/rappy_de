# 1. Mejoras en imports (más organizados)
import os, zipfile
from pathlib import Path  # <- Más moderno que os.path
import pandas as pd
import snowflake.connector
from kaggle.api.kaggle_api_extended import KaggleApi
from snowflake.connector.pandas_tools import write_pandas

# 2. Constantes claras (MAYÚSCULAS)
DOWNLOAD_PATH = Path('/tmp/meetup_data')
DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)

# 3. Conexión más segura con context manager
def get_snowflake_connection(config):
    return snowflake.connector.connect(**config)

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
    
    # Conexión Snowflake
    snowflake_config = {
        "user" : "lvillamils",
        "password" : "Th=to#3uTr#P_7frlkAT",
        "account" : "zdypxny-tob18627",
        "warehouse" : "COMPUTE_WH"
    }
    
    with get_snowflake_connection(snowflake_config) as conn:
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE IF EXISTS MEETUP_DB")
        cursor.execute("CREATE DATABASE MEETUP_DB")
        cursor.execute("CREATE SCHEMA MEETUP_DB.RAW_DATA")
        cursor.execute("USE DATABASE MEETUP_DB")
        cursor.execute("USE SCHEMA RAW_DATA")

        allowed_files = {'categories.csv', 'cities.csv', 'events.csv', 'groups.csv', 'groups_topics.csv', 'topics.csv', 'venues.csv'}
        # Procesar archivos
        for file in DOWNLOAD_PATH.glob('*.csv'):
            if file.name in allowed_files:
                #print("Archivos encontrados:", [f.name for f in DOWNLOAD_PATH.glob('*')])
                table_name = file.stem.upper()
                print(f'Processing {file.name}')
                row_count = load_csv_to_snowflake(conn, file, table_name)
                print(f'Loaded {row_count} rows into {table_name}')

if __name__ == "__main__":
    run_el()