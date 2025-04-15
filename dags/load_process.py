from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import os, logging
from pathlib import Path


logger = logging.getLogger(__name__)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", Path(__file__).parent.parent)
SCRIPT_PATH = os.path.join(AIRFLOW_HOME, "scripts", "load_dataset_snowflake.py")

@dag(
    dag_id="snowflake_meetup_loader",
    schedule=None,  # Cada 15 minutos
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_eng",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),  # 5 minutos
    },
    tags=["snowflake", "meetup"],
)

def load_snowflake():
    @task(task_id="execute_data_pipeline")
    def run_snowflake_script():
        """
        Ejecuta el script de Python que carga datos a Snowflake.
        """
        import subprocess

        if not os.path.exists(SCRIPT_PATH):
            raise FileNotFoundError(f"Script no encontrado en {SCRIPT_PATH}")

        try:
            result = subprocess.run(
                ["python", SCRIPT_PATH],
                check=True,
                text=True,
                capture_output=True,
                cwd=os.path.dirname(SCRIPT_PATH)  # Ejecuta desde la carpeta del script
            )
            logger.info(f"✅ Script ejecutado exitosamente\nOutput: {result.stdout}")
            return {"status": "success", "output": result.stdout}
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Fallo en el script:\n{e.stderr}")
            raise

    # Flujo de trabajo
    run_snowflake_script()

# Instanciación del DAG
snowflake_loader_dag = load_snowflake()