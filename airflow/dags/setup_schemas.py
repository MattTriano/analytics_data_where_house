import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import (
    get_pg_engine,
    database_has_schema,
    execute_structural_command,
)

task_logger = logging.getLogger("airflow.task")


def schema_exists(conn_id: str, schema_name: str, task_logger: Logger) -> str:
    if not database_has_schema(engine=get_pg_engine(conn_id=conn_id), schema_name=schema_name):
        task_logger.info(
            f"Schema '{schema_name}' not in database. Branching to create_schema task."
        )
        return f"create_{schema_name}_schema"
    else:
        task_logger.info(f"Schema '{schema_name}' in database.")
        return "end"


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
        return True
    except Exception as e:
        print(f"Failed to create '{schema_name}' schema. Error: {e}, {type(e)}")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def data_raw_schema_exists(conn_id: str, task_logger: Logger) -> str:
    does_schema_exist = schema_exists(
        conn_id=conn_id,
        schema_name="data_raw",
        task_logger=task_logger,
    )
    task_logger.info(f"Result: {does_schema_exist}")
    return does_schema_exist


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, retries=2)
def create_data_raw_schema(conn_id: str, task_logger: Logger) -> None:
    return create_schema(schema_name="data_raw", conn_id=conn_id, task_logger=task_logger)


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def standardized_schema_exists(conn_id: str, task_logger: Logger) -> str:
    does_schema_exist = schema_exists(
        conn_id=conn_id,
        schema_name="standardized",
        task_logger=task_logger,
    )
    task_logger.info(f"Result: {does_schema_exist}")
    return does_schema_exist


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, retries=2)
def create_standardized_schema(conn_id: str, task_logger: Logger) -> None:
    return create_schema(schema_name="standardized", conn_id=conn_id, task_logger=task_logger)


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def clean_schema_exists(conn_id: str, task_logger: Logger) -> str:
    does_schema_exist = schema_exists(
        conn_id=conn_id,
        schema_name="clean",
        task_logger=task_logger,
    )
    task_logger.info(f"Result: {does_schema_exist}")
    return does_schema_exist


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, retries=2)
def create_clean_schema(conn_id: str, task_logger: Logger) -> None:
    return create_schema(schema_name="clean", conn_id=conn_id, task_logger=task_logger)


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def feature_schema_exists(conn_id: str, task_logger: Logger) -> str:
    does_schema_exist = schema_exists(
        conn_id=conn_id,
        schema_name="feature",
        task_logger=task_logger,
    )
    task_logger.info(f"Result: {does_schema_exist}")
    return does_schema_exist


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, retries=2)
def create_feature_schema(conn_id: str, task_logger: Logger) -> None:
    return create_schema(schema_name="feature", conn_id=conn_id, task_logger=task_logger)


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dwh_schema_exists(conn_id: str, task_logger: Logger) -> str:
    does_schema_exist = schema_exists(
        conn_id=conn_id,
        schema_name="dwh",
        task_logger=task_logger,
    )
    task_logger.info(f"Result: {does_schema_exist}")
    return does_schema_exist


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, retries=2)
def create_dwh_schema(conn_id: str, task_logger: Logger) -> None:
    return create_schema(schema_name="dwh", conn_id=conn_id, task_logger=task_logger)


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["ddl"],
)
def setup_schemas():
    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)
    data_raw_schema_exists_1 = data_raw_schema_exists(
        conn_id="dwh_db_conn", task_logger=task_logger
    )
    standardized_schema_exists_1 = standardized_schema_exists(
        conn_id="dwh_db_conn", task_logger=task_logger
    )
    clean_schema_exists_1 = clean_schema_exists(conn_id="dwh_db_conn", task_logger=task_logger)
    feature_schema_exists_1 = feature_schema_exists(conn_id="dwh_db_conn", task_logger=task_logger)
    dwh_schema_exists_1 = dwh_schema_exists(conn_id="dwh_db_conn", task_logger=task_logger)

    create_data_raw_schema_1 = create_data_raw_schema(
        conn_id="dwh_db_conn", task_logger=task_logger
    )
    create_standardized_schema_1 = create_standardized_schema(
        conn_id="dwh_db_conn", task_logger=task_logger
    )
    create_clean_schema_1 = create_clean_schema(conn_id="dwh_db_conn", task_logger=task_logger)
    create_feature_schema_1 = create_feature_schema(conn_id="dwh_db_conn", task_logger=task_logger)
    create_dwh_schema_1 = create_dwh_schema(conn_id="dwh_db_conn", task_logger=task_logger)

    chain(
        data_raw_schema_exists_1,
        [Label("data_raw schema exists"), create_data_raw_schema_1],
        end_1,
    )
    chain(
        standardized_schema_exists_1,
        [Label("standardized schema exists"), create_standardized_schema_1],
        end_1,
    )
    chain(
        clean_schema_exists_1,
        [Label("clean schema exists"), create_clean_schema_1],
        end_1,
    )
    chain(
        feature_schema_exists_1,
        [Label("feature schema exists"), create_feature_schema_1],
        end_1,
    )
    chain(dwh_schema_exists_1, [Label("dwh schema exists"), create_dwh_schema_1], end_1)


setup_schemas()
