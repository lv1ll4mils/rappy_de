from airflow.hooks.base import BaseHook
import snowflake.connector
import boto3

def get_snowflake_connection(conn_id="snowflake_conn"):
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson

    return snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=extras["account"],
        warehouse=extras["warehouse"]
    )


def get_s3_client(conn_id="s3_conn"):
    conn = BaseHook.get_connection(conn_id)
    region = conn.extra_dejson.get("region_name", "us-east-1")

    return boto3.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=region
    )