from logging import Logger

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import (
    get_pg_engine,
    database_has_schema,
    execute_structural_command,
    get_data_table_names_in_schema,
)


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def metadata_schema_exists(conn_id: str, task_logger: Logger) -> str:
    if not database_has_schema(engine=get_pg_engine(conn_id=conn_id), schema_name="metadata"):
        task_logger.info(
            f"Schema 'metadata' not in database. Branching to create_metadata_schema task."
        )
        return "create_metadata_schema"
    else:
        task_logger.info(
            f"Schema 'metadata' in database. Branching to table_exists_in_schema task."
        )
        return "metadata_table_exists"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def create_metadata_schema(conn_id: str, task_logger: Logger) -> None:
    task_logger.info(f"inside create_metadata_schema")
    engine = get_pg_engine(conn_id=conn_id)
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


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def metadata_table_exists(table_name: str, conn_id: str, task_logger: Logger) -> str:
    schema_name = "metadata"
    tables_in_metadata_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name=schema_name
    )
    if table_name not in tables_in_metadata_schema:
        task_logger.info(f"Table {table_name} not found in schema {schema_name}. Creating")
        task_logger.info(f"tables_in_metadata_schema: {tables_in_metadata_schema}")
        return "create_metadata_table"
    else:
        task_logger.info(f"Table {table_name} found in schema {schema_name}.")
        return "end"
