# 1. Imports organizados
from datetime import datetime, timedelta
from pathlib import Path
import logging, os
from airflow.decorators import dag, task
from utils.callbacks import slack_success_callback, slack_failure_callback

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
    tags=["snowflake", "prod"],
)

def load_snowflake():

    @task(task_id="execute_data_load",
          on_success_callback=slack_success_callback,
          on_failure_callback=slack_failure_callback
          )
    def run_data_load():
        """Versión con importación directa y manejo robusto"""
        try:
            # 1. Configura entorno (igual que antes)
            dag_path = Path(__file__).parent.absolute()
            os.environ["KAGGLE_CONFIG_DIR"] = str(dag_path.parent / ".secrets")

            from scripts.load_dataset_snowflake import run_el

            # 3. Ejecución
            logger.info("▶️ Iniciando carga de datos...")
            run_el()
            logger.info("✅ Carga completada")
            return {"status": "success"}

        except ImportError as e:
            logger.error(f"❌ Error al importar el script: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Error inesperado: {e}")
            raise

    run_data_load()

snowflake_loader_dag = load_snowflake()
