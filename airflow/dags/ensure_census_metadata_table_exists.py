import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
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
                    updated_metadata_available BOOLEAN DEFAULT NULL
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

    metadata_schema_exists_branch_1 = metadata_schema_exists(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    create_metadata_schema_1 = create_metadata_schema(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    metadata_table_exists_1 = metadata_table_exists(
        table_name="census_metadata", conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
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
