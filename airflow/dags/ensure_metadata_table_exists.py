import datetime as dt
import logging
from typing import Dict

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy.engine.base import Engine

from utils.db import (
    database_has_schema,
    execute_structural_command,
    get_data_table_names_in_schema,
)


task_logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "dwh_db_conn"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def ensure_metadata_table_exists():
    def _get_pg_engine(conn_id: str = POSTGRES_CONN_ID) -> Engine:
        try:
            pg_hook = PostgresHook(conn_id)
            engine = pg_hook.get_sqlalchemy_engine()
            return engine
        except Exception as e:
            print(
                f"Failed to generate engine to pg db using conn_id {conn_id}. Error: {e}, {type(e)}"
            )

    @task.branch
    def db_has_metadata_schema(conn_id: str) -> str:
        if not database_has_schema(engine=_get_pg_engine(conn_id=conn_id), schema_name="metadata"):
            task_logger.info(
                f"Schema 'metadata' not in database. Branching to create_metadata_schema task."
            )
            return "create_metadata_schema"
        else:
            task_logger.info(
                f"Schema 'metadata' in database. Branching to table_exists_in_schema task."
            )
            return "table_exists_in_schema"

    @task
    def end() -> str:
        task_logger.info(f"Ending DAG execution.")

    @task
    def create_metadata_schema() -> None:
        task_logger.info(f"inside create_metadata_schema")
        engine = _get_pg_engine(conn_id=POSTGRES_CONN_ID)
        try:
            username = engine.url.username
            execute_structural_command(
                query=f"""
                CREATE SCHEMA metadata;
                GRANT USAGE ON SCHEMA metadata TO {username};
                GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata TO {username};
                ALTER DEFAULT PRIVILEGES FOR USER {username} IN SCHEMA metadata
                    GRANT ALL ON TABLES TO {username};
                """,
                engine=engine,
            )
        except Exception as e:
            print(f"Failed to create metadata schema. Error: {e}, {type(e)}")

    @task.branch
    def table_exists_in_schema(
        conn_id: str, schema_name: str = "metadata", table_name: str = "metadata"
    ) -> str:
        tables_in_metadata_schema = get_data_table_names_in_schema(
            engine=_get_pg_engine(conn_id=conn_id), schema_name=schema_name
        )
        if table_name not in tables_in_metadata_schema:
            task_logger.info(f"Table {table_name} not found in schema {schema_name}. Creating")
            return "create_metadata_table"
        else:
            task_logger.info(f"Table {table_name} found in schema {schema_name}.")
            return "end"

    @task
    def create_metadata_table(conn_id: str) -> str:
        task_logger.info(f"Placeholder for create_metadata_table functionality.")

    metadata_schema_exists_branch_1 = db_has_metadata_schema(conn_id=POSTGRES_CONN_ID)

    create_metadata_schema_1 = create_metadata_schema()
    table_exists_in_schema_1 = table_exists_in_schema(conn_id=POSTGRES_CONN_ID)
    create_metadata_table_1 = create_metadata_table(conn_id=POSTGRES_CONN_ID)
    end_1 = end()

    metadata_schema_exists_branch_1 >> create_metadata_schema_1 >> create_metadata_table_1 >> end_1
    metadata_schema_exists_branch_1 >> table_exists_in_schema_1 >> create_metadata_table_1 >> end_1
    metadata_schema_exists_branch_1 >> table_exists_in_schema_1 >> end_1


chack_metadata_schema_dag = ensure_metadata_table_exists()
