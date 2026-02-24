"""Databricks SQL Warehouse connection factory."""

import os
from databricks import sql


def get_databricks_connection():
    """Connect to Databricks SQL Warehouse"""
    try:
        return sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN")
        )
    except Exception as e:
        raise Exception(f"Failed to connect to Databricks: {str(e)}")
