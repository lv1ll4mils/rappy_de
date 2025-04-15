from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

def slack_success_callback(context):
    """Send Slack alert on DAG success.

    Args:
        context (dict): Airflow context dictionary.
    """
    message = (
        f"✅ *DAG Exitoso*: `{context['dag_run'].dag_id}`\n"
        f"• Task: `{context['task_instance'].task_id}`\n"
        f"• Hora: `{context['ts']}`\n"
        f"• Duración: `{round(context['task_instance'].duration, 2)}s`"
    )
    hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")
    hook.send(text=message)

def slack_failure_callback(context):
    """Send Slack alert on DAG failure.

    Args:
        context (dict): Airflow context dictionary.
    """
    message = (
        f"❌ *DAG Fallido*: `{context['dag_run'].dag_id}`\n"
        f"• Task: `{context['task_instance'].task_id}`\n"
        f"• Error: `{str(context['exception'])[:200]}`\n"
        f"• Log: {context['task_instance'].log_url}"
    )
    hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")
    hook.send(text=message)
