import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import (
    get_pg_engine,
    database_has_schema,
    execute_structural_command,
    get_data_table_names_in_schema,
)

from tasks.metadata_tasks import (
    metadata_schema_exists,
    create_metadata_schema,
    metadata_table_exists,
)


task_logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "dwh_db_conn"


# @task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
# def metadata_schema_exists(conn_id: str) -> str:
#     if not database_has_schema(engine=get_pg_engine(conn_id=conn_id), schema_name="metadata"):
#         task_logger.info(
#             f"Schema 'metadata' not in database. Branching to create_metadata_schema task."
#         )
#         return "create_metadata_schema"
#     else:
#         task_logger.info(
#             f"Schema 'metadata' in database. Branching to table_exists_in_schema task."
#         )
#         return "metadata_table_exists"


# @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
# def create_metadata_schema() -> None:
#     task_logger.info(f"inside create_metadata_schema")
#     engine = get_pg_engine(conn_id=POSTGRES_CONN_ID)
#     try:
#         username = engine.url.username
#         execute_structural_command(
#             query=f"""
#             CREATE SCHEMA metadata;
#             GRANT USAGE ON SCHEMA metadata TO {username};
#             GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata TO {username};
#             ALTER DEFAULT PRIVILEGES FOR USER {username} IN SCHEMA metadata
#                 GRANT ALL ON TABLES TO {username};
#             """,
#             engine=engine,
#         )
#     except Exception as e:
#         print(f"Failed to create metadata schema. Error: {e}, {type(e)}")


# @task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
# def census_metadata_table_exists(
#     table_name: str, conn_id: str, task_logger: Logger
# ) -> str:
#     schema_name = "metadata"
#     table_name = "census_metadata"
#     tables_in_metadata_schema = get_data_table_names_in_schema(
#         engine=get_pg_engine(conn_id=conn_id), schema_name=schema_name
#     )
#     if table_name not in tables_in_metadata_schema:
#         task_logger.info(f"Table {table_name} not found in schema {schema_name}. Creating")
#         task_logger.info(f"tables_in_metadata_schema: {tables_in_metadata_schema}")
#         return "create_census_metadata_table"
#     else:
#         task_logger.info(f"Table {table_name} found in schema {schema_name}.")
#         return "end"


# @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
# def create_census_metadata_table(conn_id: str, task_logger: Logger):
#     try:
#         table_name = "census_metadata"
#         task_logger.info(f"Creating table metadata.{table_name}")
#         postgres_hook = PostgresHook(postgres_conn_id=conn_id)
#         conn = postgres_hook.get_conn()
#         cur = conn.cursor()
#         cur.execute(
#             f"""CREATE TABLE IF NOT EXISTS metadata.{table_name} (
#                     id SERIAL PRIMARY KEY,
#                     metadata_url TEXT NOT NULL,
#                     last_modified TIMESTAMP NOT NULL,
#                     size TEXT,
#                     description TEXT,
#                     is_dir BOOLEAN,
#                     is_file BOOLEAN,
#                     time_of_check TIMESTAMP WITH TIME ZONE NOT NULL,
#                     updated_data_available BOOLEAN DEFAULT NULL
#                 );"""
#         )
#         conn.commit()
#         return "success"
#     except Exception as e:
#         print(f"Failed to create metadata table {table_name}. Error: {e}, {type(e)}")


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def create_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.census_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.census_metadata (
                    id SERIAL PRIMARY KEY,
                    metadata_url TEXT NOT NULL,
                    last_modified TIMESTAMP NOT NULL,
                    size TEXT,
                    description TEXT,
                    is_dir BOOLEAN,
                    is_file BOOLEAN,
                    time_of_check TIMESTAMP WITH TIME ZONE NOT NULL,
                    updated_data_available BOOLEAN DEFAULT NULL
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        print(f"Failed to create metadata table metadata.census_metadata. Error: {e}, {type(e)}")


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def a_dev_create_census_metadata_table():

    metadata_schema_exists_branch_1 = metadata_schema_exists(conn_id=POSTGRES_CONN_ID, task_logger=task_logger)
    create_metadata_schema_1 = create_metadata_schema(conn_id=POSTGRES_CONN_ID, task_logger=task_logger)
    metadata_table_exists_1 = metadata_table_exists(
        table_name="census_metadata", conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    # table_exists_in_schema_1 = census_metadata_table_exists(conn_id=POSTGRES_CONN_ID)
    create_metadata_table_1 = create_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    chain(
        metadata_schema_exists_branch_1,
        [create_metadata_schema_1, metadata_table_exists_1],
    )
    chain(
        metadata_table_exists_1,
        [create_metadata_table_1, Label("Census metadata table exists")],
        end_1,
    )
    chain(create_metadata_schema_1, create_metadata_table_1, end_1)


a_dev_create_census_metadata_table()
