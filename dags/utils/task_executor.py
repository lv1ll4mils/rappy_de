import logging
logger = logging.getLogger(__name__)

def run_step(task_name, import_path, function_name, success_message):
    """Dynamically run a function with logging and error handling.

    Args:
        task_name (str): Task name for logging context.
        import_path (str): Module path to import the function from.
        function_name (str): Name of the function to execute.
        success_message (str): Message to log on successful execution.

    Returns:
        dict: Status result after execution.
    """
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
