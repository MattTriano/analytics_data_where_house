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
def create_socrata_dataset_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.table_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.table_metadata (
                    id SERIAL PRIMARY KEY,
                    table_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    download_format TEXT NOT NULL,
                    is_geospatial BOOLEAN DEFAULT NULL,
                    data_download_url TEXT DEFAULT NULL,
                    source_data_last_updated TIMESTAMP WITH TIME ZONE,
                    source_metadata_last_updated TIMESTAMP WITH TIME ZONE,
                    updated_data_available BOOLEAN DEFAULT NULL,
                    updated_metadata_available BOOLEAN DEFAULT NULL,
                    data_pulled_this_check BOOLEAN DEFAULT NULL,
                    time_of_check TIMESTAMP WITH TIME ZONE,
                    metadata_json JSONB
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        task_logger.info(
            f"Failed to create table metadata.table_metadata." + f"Error: {e}, {type(e)}"
        )
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def socrata_dataset_metadata_endpoint() -> str:
    return "success with socrata_dataset_metadata table creation"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def create_socrata_dataset_metadata_table():
    metadata_schema_exists_branch_1 = metadata_schema_exists(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    create_metadata_schema_1 = create_metadata_schema(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    metadata_table_exists_1 = metadata_table_exists(
        table_name="table_metadata",
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        create_route="create_socrata_dataset_metadata_table",
        exists_route="socrata_dataset_metadata_endpoint",
    )
    create_socrata_dataset_metadata_table_1 = create_socrata_dataset_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    socrata_dataset_metadata_endpoint_1 = socrata_dataset_metadata_endpoint()

    chain(
        metadata_schema_exists_branch_1,
        [create_metadata_schema_1, Label("Metadata schema exists")],
        [create_socrata_dataset_metadata_table_1, metadata_table_exists_1],
    )
    chain(
        metadata_table_exists_1,
        [
            create_socrata_dataset_metadata_table_1,
            Label("Socrata Dataset\nMetadata Table Exists"),
        ],
        socrata_dataset_metadata_endpoint_1,
    )


create_socrata_dataset_metadata_table()
