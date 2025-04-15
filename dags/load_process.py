# 1. Imports organizados
from datetime import datetime, timedelta
from pathlib import Path
import logging, os
from airflow.decorators import dag, task
from utils.callbacks import slack_success_callback, slack_failure_callback
from dags.utils.task_executor import run_step

# Configura logging
logger = logging.getLogger(__name__)

# 2. Configuración centralizada
@dag(
    dag_id="snowflake_loader",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "data_eng",
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["snowflake", "s3"],
)

def load_snowflake():

    @task(task_id="execute_data_load",
          on_success_callback=slack_success_callback,
          on_failure_callback=slack_failure_callback
          )
    def run_data_load():
        os.environ["KAGGLE_CONFIG_DIR"] = str(Path(__file__).parent.parent / ".secrets")
        return run_step(
            task_name="Carga de datos a Snowflake",
            import_path="scripts.load_dataset_snowflake",
            function_name="run_el",
            success_message="Carga completada"
        )

    @task(task_id="export_data_to_s3",
          on_success_callback=slack_success_callback,
          on_failure_callback=slack_failure_callback
          )
    def export_to_s3_task():
        return run_step(
            task_name="Exportación a S3",
            import_path="scripts.export_to_s3",
            function_name="export_queries_and_upload",
            success_message="Exportación completada"
        )

    run_data = run_data_load()
    export_s3 = export_to_s3_task()

    run_data >> export_s3

snowflake_loader_dag = load_snowflake()
