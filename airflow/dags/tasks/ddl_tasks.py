from logging import Logger

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import (
    get_pg_engine,
    database_has_schema,
    execute_structural_command,
)


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def schema_exists(conn_id: str, schema_name: str, task_logger: Logger) -> str:
    if not database_has_schema(engine=get_pg_engine(conn_id=conn_id), schema_name=schema_name):
        task_logger.info(
            f"Schema '{schema_name}' not in database. Branching to create_schema task."
        )
        return "ensure_schema_exists.create_schema"
    else:
        task_logger.info(f"Schema '{schema_name}' in database. Ending DAG.")
        return "ensure_schema_exists.end"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def create_schema(schema_name: str, conn_id: str, task_logger: Logger) -> None:
    task_logger.info(f"Creating '{schema_name}' schema")
    engine = get_pg_engine(conn_id=conn_id)
    try:
        username = engine.url.username
        execute_structural_command(
            query=f"""
            CREATE SCHEMA {schema_name};
            GRANT USAGE ON SCHEMA metadata TO {username};
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema_name} TO {username};
            ALTER DEFAULT PRIVILEGES FOR USER {username} IN SCHEMA {schema_name}
                GRANT ALL ON TABLES TO {username};
            """,
            engine=engine,
        )
    except Exception as e:
        print(f"Failed to create '{schema_name}' schema. Error: {e}, {type(e)}")


@task_group
def ensure_schema_exists(schema_name: str, conn_id: str, task_logger: Logger) -> None:
    check_for_schema_1 = schema_exists(
        schema_name=schema_name, conn_id=conn_id, task_logger=task_logger
    )
    create_schema_1 = create_schema(
        schema_name=schema_name, conn_id=conn_id, task_logger=task_logger
    )
    do_nothing_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)
    check_for_schema_1 >> [create_schema_1, do_nothing_1]
