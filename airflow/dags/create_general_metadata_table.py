import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule


from tasks.metadata_tasks import (
    metadata_schema_exists,
    create_metadata_schema,
    metadata_table_exists,
)


task_logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "dwh_db_conn"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def create_dataset_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.dataset_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.dataset_metadata (
                    id SERIAL PRIMARY KEY,
                    dataset_name TEXT NOT NULL,
                    data_last_modified TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    time_of_check TIMESTAMP WITH TIME ZONE NOT NULL
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        print(f"Failed to create table metadata.dataset_metadata. Error: {e}, {type(e)}")
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def metadata_table_endpoint() -> str:
    return "success with api_groups_table creation"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata", "census"],
)
def create_general_metadata_table():

    metadata_schema_exists_branch_1 = metadata_schema_exists(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    create_metadata_schema_1 = create_metadata_schema(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    metadata_table_exists_1 = metadata_table_exists(
        table_name="dataset_metadata",
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        create_route="create_dataset_metadata_table",
        exists_route="metadata_table_endpoint",
    )
    create_metadata_table_1 = create_dataset_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )

    metadata_table_endpoint_1 = metadata_table_endpoint()

    chain(
        metadata_schema_exists_branch_1,
        [create_metadata_schema_1, Label("Metadata schema exists")],
        [create_metadata_table_1, metadata_table_exists_1],
        metadata_table_endpoint_1,
    )
    chain(create_metadata_schema_1, metadata_table_exists_1, create_metadata_table_1)


create_general_metadata_table()
