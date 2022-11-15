import datetime as dt
import logging
from typing import Dict

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy.engine.base import Engine

from utils.db import database_has_schema, execute_structural_command


task_logger = logging.getLogger('airflow.task')

POSTGRES_CONN_ID = "dwh_db_conn"

def _get_pg_engine(conn_id: str = POSTGRES_CONN_ID) -> Engine:
    try:
        pg_hook = PostgresHook(conn_id)
        engine = pg_hook.get_sqlalchemy_engine()
        return engine
    except Exception as e:
        print(f"Failed to generate engine to pg db using conn_id {conn_id}. Error: {e}, {type(e)}")


@task.branch()
def db_has_metadata_schema(conn_id: str) -> bool:
    if database_has_schema(engine=_get_pg_engine(conn_id=conn_id), schema_name="metadata"):
        return "create_metadata_schema"
    return "end"



@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def ensure_metadata_table_exists():
    @task
    def end() -> str:
        return "end" 

    @task
    def create_metadata_schema() -> None:
        schema_name = "metadata"
        task_logger.info(f"inside create_metadata_schema")
        engine = _get_pg_engine(conn_id=POSTGRES_CONN_ID)
        try:
            username = engine.url.username
            execute_structural_command(
                query=f"""
                GRANT USAGE ON SCHEMA metadata TO {username};
                GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata TO {username};
                ALTER DEFAULT PRIVILEGES FOR USER {username} IN SCHEMA metadata
                    GRANT ALL ON TABLES TO {username};
                """,
                engine=engine,
            )
        except Exception as e:
            print(f"Failed to create metadata schema. Error: {e}, {type(e)}")
    
    metadata_schema_exists_branch = db_has_metadata_schema.override(task_id="metadata_schema_exists_branch")(conn_id=POSTGRES_CONN_ID)

    chain(metadata_schema_exists_branch, [create_metadata_schema, end])


ensure_metadata_table_exists()
