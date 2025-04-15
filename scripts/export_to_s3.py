import os, boto3, logging
from pathlib import Path
import pandas as pd
from dags.utils.connections import get_snowflake_connection, get_s3_client

logger = logging.getLogger(__name__)

EXPORT_PATH = Path("/tmp/exports")
EXPORT_PATH.mkdir(parents=True, exist_ok=True)


def execute_sql_file(cursor, sql_path):
    """Execute all SQL statements in a given file.

    Args:
        cursor: Active Snowflake cursor.
        sql_path (Path): Path to the SQL file.
    """
    with open(sql_path, "r") as file:
        sql_script = file.read()
    for stmt in sql_script.split(";"):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)

def export_queries_and_upload(conn_id="s3_conn", bucket_name="snowflak3-exp0rt-data-r4ppy"):
    """Export Snowflake query results and upload to S3.

    Args:
        conn_id (str): Airflow connection ID for S3.
        bucket_name (str): Target S3 bucket name.
    """
    db_name = "MEETUP_DB"
    schema_name = "TRANSFORM_DATA"

    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {db_name}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS MEETUP_DB.{schema_name}")
        cursor.execute(f"USE SCHEMA {schema_name}")

        # A. Crear tablas
        create_path = Path(__file__).parent.parent / "include/sql/create_tables.sql"
        logger.info(f'PROCESO CREACION {create_path}')
        execute_sql_file(cursor, create_path)

        # B. Ejecutar selects
        export_path = Path(__file__).parent.parent / "include/sql/export_queries.sql"
        logger.info(f'PROCESO SELECCION {export_path}')
        with open(export_path, "r") as f:
            selects = [q.strip() for q in f.read().split(";") if q.strip()]

        s3 = get_s3_client(conn_id)

        for i, query in enumerate(selects, 1):
            df = pd.read_sql(query, conn)
            file_name = "ACTIVE_MEMBERS.csv"
            local_path = EXPORT_PATH / file_name
            df.to_csv(local_path, index=False)
            logger.info(f'Exported to {local_path}')

            s3_key = f"{db_name}/{schema_name}/{file_name}"
            s3.upload_file(str(local_path), bucket_name, s3_key)
            logger.info(f'Uploaded to s3://{bucket_name}/{s3_key}')
