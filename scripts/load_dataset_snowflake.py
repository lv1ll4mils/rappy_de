import os, zipfile
import pandas as pd
import snowflake.connector
from kaggle.api.kaggle_api_extended import KaggleApi
from snowflake.connector.pandas_tools import write_pandas

# Paths
download_set = '/tmp/meetup_data'
os.makedirs(download_set, exist_ok=True)

# 1. Snowflake connection credencials
snowflake_config = {
    "user" : "lvillamils",
    "password" : "Th=to#3uTr#P_7frlkAT",
    "account" : "zdypxny-tob18627",
    "warehouse" : "COMPUTE_WH"
}

# API connection
os.environ['KAGGLE_CONFIG_DIR'] = '/home/luigi/back_up/rappy_de/.secrets'

print("KAGGLE_CONFIG_DIR:", os.getenv("KAGGLE_CONFIG_DIR"))
print("Contenido de la carpeta:", os.listdir(os.getenv("KAGGLE_CONFIG_DIR")) if os.path.exists(os.getenv("KAGGLE_CONFIG_DIR")) else "Carpeta no encontrada")


# 3. Download dataset with API Kaggle
api = KaggleApi()
api.authenticate()
api.dataset_download_files('megelon/meetup', path=download_set, unzip=True)

conn = snowflake.connector.connect(**snowflake_config)
cursor = conn.cursor()

try:
    # 2.1 Deleet DB if exists
    cursor.execute("DROP DATABASE IF EXISTS MEETUP_DB")

    # 2.2 Create DB and SCHEMA
    cursor.execute("CREATE DATABASE MEETUP_DB")
    cursor.execute("CREATE SCHEMA MEETUP_DB.RAW_DATA")

    # 2.3 Connection
    cursor.execute("USE DATABASE MEETUP_DB")
    cursor.execute("USE SCHEMA RAW_DATA")


    # 4. Load CSV files in Snowflake
    for file in os.listdir(download_set):
        if file.endswith('csv'):
            print(f'File--> {file}')
            file_path = os.path.join(download_set, file)
            table_name = os.path.splitext(file)[0].upper()

            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='latin-1')

            print(f'Load {file} - {len(df)} rows in {table_name} table')

            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name,
                auto_create_table=True,
                overwrite=True
            )

            print(f'{nrows} load rows in {table_name}')

finally:
    # Cierre seguro de la conexión
    cursor.close()
    conn.close()
    print("Conexión cerrada correctamente")