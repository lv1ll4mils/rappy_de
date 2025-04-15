import logging
logger = logging.getLogger(__name__)

def run_step(task_name, import_path, function_name, success_message):
    try:
        module = __import__(import_path, fromlist=[function_name])
        func = getattr(module, function_name)
        logger.info(f"▶️ Iniciando {task_name}...")
        func()
        logger.info(f"✅ {success_message}")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"❌ Error en {task_name}: {e}")
        raise
