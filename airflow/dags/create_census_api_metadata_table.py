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
def create_api_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.census_api_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.census_api_metadata (
                    id SERIAL PRIMARY KEY,
                    identifier TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    modified TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    vintage TEXT,
                    distribution_access_url TEXT,
                    geography_link TEXT,
                    variables_link TEXT,
                    tags_link TEXT,
                    examples_link TEXT,
                    groups_link TEXT,
                    sorts_url TEXT,
                    dataset TEXT,
                    spatial TEXT,
                    temporal TEXT,
                    bureau_code TEXT NOT NULL,
                    program_code TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    is_microdata BOOLEAN,
                    is_aggregate BOOLEAN,
                    is_cube BOOLEAN,
                    is_available BOOLEAN,
                    is_timeseries BOOLEAN,
                    access_level TEXT NOT NULL,
                    license TEXT,
                    type TEXT,
                    publisher_name TEXT,
                    publisher_type TEXT,
                    contact_point_fn TEXT NOT NULL,
                    contact_point_email TEXT NOT NULL,
                    distribution_type TEXT,
                    distribution_media_type TEXT,
                    reference_docs TEXT,
                    documentation_link TEXT,
                    time_of_check TIMESTAMP WITH TIME ZONE NOT NULL,
                    modified_since_last_check BOOLEAN DEFAULT NULL,
                    variables_metadata_pulled_this_check BOOLEAN DEFAULT NULL,
                    geographies_metadata_pulled_this_check BOOLEAN DEFAULT NULL
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        print(f"Failed to create table metadata.census_api_metadata. Error: {e}, {type(e)}")
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def create_api_variables_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.census_api_variables_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.census_api_variables_metadata (
                    id SERIAL PRIMARY KEY,
                    identifier TEXT,
                    variable TEXT,
                    label TEXT,
                    concept TEXT,
                    predicate_type TEXT,
                    dataset_group TEXT,
                    query_limit BIGINT,
                    predicate_only BOOLEAN,
                    suggested_weight TEXT,
                    values JSONB,
                    is_weight BOOLEAN,
                    dataset_last_modified TEXT,
                    updated_data_available BOOLEAN DEFAULT NULL,
                    data_pulled_this_check BOOLEAN DEFAULT NULL,
                    time_of_check TIMESTAMP WITH TIME ZONE NOT NULL
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        print(f"Failed to create table metadata.census_api_metadata. Error: {e}, {type(e)}")
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def api_metadata_table_endpoint() -> str:
    return "success with api_metadata_table creation"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata", "census"],
)
def create_census_api_metadata_table():

    metadata_schema_exists_branch_1 = metadata_schema_exists(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    create_metadata_schema_1 = create_metadata_schema(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    api_metadata_table_endpoint_1 = api_metadata_table_endpoint()
    metadata_table_exists_1 = metadata_table_exists(
        table_name="census_api_metadata",
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        create_route="create_api_metadata_table",
        exists_route="api_metadata_table_endpoint",
    )
    create_api_metadata_table_1 = create_api_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    metadata_variables_table_exists_1 = metadata_table_exists(
        table_name="census_api_variables_metadata",
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        create_route="create_api_variables_metadata_table",
        exists_route="end",
    )
    create_api_variables_metadata_table_1 = create_api_variables_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    chain(
        metadata_schema_exists_branch_1,
        [create_metadata_schema_1, metadata_table_exists_1],
    )
    chain(
        metadata_table_exists_1,
        [create_api_metadata_table_1, Label("Census metadata table exists")],
        api_metadata_table_endpoint_1,
        metadata_variables_table_exists_1,
        [
            create_api_variables_metadata_table_1,
            Label("Census API variables metadata table exists"),
        ],
        end_1,
    )
    chain(create_metadata_schema_1, create_api_metadata_table_1)


create_census_api_metadata_table()
