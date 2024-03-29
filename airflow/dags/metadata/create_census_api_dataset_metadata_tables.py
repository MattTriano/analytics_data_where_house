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
def create_api_dataset_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.census_api_dataset_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.census_api_dataset_metadata (
                    id SERIAL PRIMARY KEY,
                    dataset_base_url TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    modified TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    vintage TEXT,
                    geography_link TEXT,
                    variables_link TEXT,
                    tags_link TEXT,
                    examples_link TEXT,
                    groups_link TEXT,
                    sorts_url TEXT,
                    dataset TEXT ARRAY,
                    spatial TEXT,
                    temporal TEXT,
                    bureau_code TEXT ARRAY NOT NULL,
                    program_code TEXT ARRAY NOT NULL,
                    keyword TEXT ARRAY NOT NULL,
                    is_microdata BOOLEAN,
                    is_aggregate BOOLEAN,
                    is_cube BOOLEAN,
                    is_available BOOLEAN,
                    is_timeseries BOOLEAN,
                    access_level TEXT NOT NULL,
                    license TEXT,
                    dataset_type TEXT,
                    contact_point_fn TEXT NOT NULL,
                    contact_point_email TEXT NOT NULL,
                    reference_docs TEXT ARRAY,
                    documentation_link TEXT,
                    distribution_type TEXT,
                    distribution_media_type TEXT,
                    distribution_description TEXT,
                    distribution_format TEXT,
                    distribution_title TEXT,
                    distribution JSONB,
                    publisher_name TEXT,
                    publisher_type TEXT,
                    publisher_suborg_of_type TEXT,
                    publisher_suborg_of_name TEXT,
                    publisher_suborg_of_suborg_of_type TEXT,
                    publisher_suborg_of_suborg_of_name TEXT,
                    metadata_context TEXT,
                    metadata_catalog_id TEXT,
                    metadata_type TEXT,
                    conforms_to_schema TEXT,
                    data_schema_dictionary TEXT,
                    time_of_check TIMESTAMP WITH TIME ZONE NOT NULL
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        task_logger.info(
            f"Failed to create table metadata.census_api_dataset_metadata. Error: {e}, {type(e)}"
        )
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def create_api_dataset_variables_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.census_api_dataset_variables_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.census_api_dataset_variables_metadata (
                    id SERIAL PRIMARY KEY,
                    dataset_base_url TEXT NOT NULL,
                    dataset_id INT,
                    variable TEXT,
                    label TEXT,
                    concept TEXT,
                    predicate_type TEXT,
                    dataset_group TEXT,
                    limit_call BIGINT,
                    predicate_only BOOLEAN,
                    has_geo_collection_support BOOLEAN,
                    attributes TEXT,
                    required TEXT,
                    values JSONB,
                    datetime JSONB,
                    is_weight BOOLEAN,
                    suggested_weight TEXT,
                    dataset_last_modified TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    time_of_check TIMESTAMP WITH TIME ZONE NOT NULL
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        task_logger.info(
            f"Failed to create table metadata.census_api_dataset_variables_metadata."
            + f"Error: {e}, {type(e)}"
        )
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def create_api_dataset_geographies_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.census_api_dataset_geographies_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.census_api_dataset_geographies_metadata (
                    id SERIAL PRIMARY KEY,
                    dataset_base_url TEXT NOT NULL,
                    dataset_id INT,
                    name TEXT,
                    geo_level TEXT,
                    reference_date DATE,
                    requires TEXT ARRAY,
                    wildcard TEXT ARRAY,
                    optional_with_wildcard_for TEXT,
                    dataset_last_modified TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    time_of_check TIMESTAMP WITH TIME ZONE NOT NULL
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        task_logger.info(
            f"Failed to create table metadata.census_api_dataset_geographies_metadata."
            + f"Error: {e}, {type(e)}"
        )
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def create_api_dataset_groups_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.census_api_dataset_groups_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.census_api_dataset_groups_metadata (
                    id SERIAL PRIMARY KEY,
                    dataset_base_url TEXT NOT NULL,
                    dataset_id INT,
                    group_name TEXT NOT NULL,
                    group_description TEXT,
                    group_variables TEXT,
                    universe TEXT,
                    dataset_last_modified TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    time_of_check TIMESTAMP WITH TIME ZONE NOT NULL
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        task_logger.info(
            f"Failed to create table metadata.census_api_dataset_groups_metadata."
            + f"Error: {e}, {type(e)}"
        )
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def create_api_dataset_tags_metadata_table(conn_id: str, task_logger: Logger):
    try:
        task_logger.info(f"Creating table metadata.census_api_dataset_tags_metadata")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS metadata.census_api_dataset_tags_metadata (
                    id SERIAL PRIMARY KEY,
                    dataset_base_url TEXT NOT NULL,
                    dataset_id INT,
                    tag_name TEXT NOT NULL,
                    dataset_last_modified TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    time_of_check TIMESTAMP WITH TIME ZONE NOT NULL
                );"""
        )
        conn.commit()
        return "success"
    except Exception as e:
        task_logger.info(
            f"Failed to create table metadata.census_api_dataset_tags_metadata."
            + f"Error: {e}, {type(e)}"
        )
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def api_metadata_table_endpoint() -> str:
    return "success with api_metadata table creation"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def api_variables_table_endpoint() -> str:
    return "success with api_variables table creation"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def api_geographies_table_endpoint() -> str:
    return "success with api_geographies table creation"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def api_groups_table_endpoint() -> str:
    return "success with api_groups table creation"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def api_tags_table_endpoint() -> str:
    return "success with api_tags table creation"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata", "census"],
)
def create_census_api_dataset_metadata_tables():

    metadata_schema_exists_branch_1 = metadata_schema_exists(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    create_metadata_schema_1 = create_metadata_schema(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    metadata_table_exists_1 = metadata_table_exists(
        table_name="census_api_dataset_metadata",
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        create_route="create_api_dataset_metadata_table",
        exists_route="api_metadata_table_endpoint",
    )
    create_api_dataset_metadata_table_1 = create_api_dataset_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    api_metadata_table_endpoint_1 = api_metadata_table_endpoint()

    metadata_variables_table_exists_1 = metadata_table_exists(
        table_name="census_api_dataset_variables_metadata",
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        create_route="create_api_dataset_variables_metadata_table",
        exists_route="api_variables_table_endpoint",
    )
    create_api_dataset_variables_metadata_table_1 = create_api_dataset_variables_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    api_variables_table_endpoint_1 = api_variables_table_endpoint()

    metadata_geographies_table_exists_1 = metadata_table_exists(
        table_name="census_api_dataset_geographies_metadata",
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        create_route="create_api_dataset_geographies_metadata_table",
        exists_route="api_geographies_table_endpoint",
    )
    create_api_dataset_geographies_metadata_table_1 = create_api_dataset_geographies_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    api_geographies_table_endpoint_1 = api_geographies_table_endpoint()

    metadata_groups_table_exists_1 = metadata_table_exists(
        table_name="census_api_dataset_groups_metadata",
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        create_route="create_api_dataset_groups_metadata_table",
        exists_route="api_groups_table_endpoint",
    )
    create_api_dataset_groups_metadata_table_1 = create_api_dataset_groups_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    api_groups_table_endpoint_1 = api_groups_table_endpoint()

    metadata_tags_table_exists_1 = metadata_table_exists(
        table_name="census_api_dataset_tags_metadata",
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        create_route="create_api_dataset_tags_metadata_table",
        exists_route="api_tags_table_endpoint",
    )
    create_api_dataset_tags_metadata_table_1 = create_api_dataset_tags_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    api_tags_table_endpoint_1 = api_tags_table_endpoint()

    chain(
        metadata_schema_exists_branch_1,
        [create_metadata_schema_1, Label("Metadata schema exists")],
        [create_api_dataset_metadata_table_1, metadata_table_exists_1],
    )
    chain(create_metadata_schema_1, create_api_dataset_metadata_table_1)
    chain(
        metadata_table_exists_1,
        [
            create_api_dataset_metadata_table_1,
            Label("Census API dataset\nmetadata table exists"),
        ],
        api_metadata_table_endpoint_1,
        [
            metadata_variables_table_exists_1,
            metadata_geographies_table_exists_1,
            metadata_groups_table_exists_1,
            metadata_tags_table_exists_1,
        ],
    )
    chain(
        metadata_variables_table_exists_1,
        [
            create_api_dataset_variables_metadata_table_1,
            Label("Census API variables\nmetadata table exists"),
        ],
        api_variables_table_endpoint_1,
    )
    chain(
        metadata_geographies_table_exists_1,
        [
            create_api_dataset_geographies_metadata_table_1,
            Label("Census API geographies\nmetadata table exists"),
        ],
        api_geographies_table_endpoint_1,
    )
    chain(
        metadata_groups_table_exists_1,
        [
            create_api_dataset_groups_metadata_table_1,
            Label("Census API groups\nmetadata table exists"),
        ],
        api_groups_table_endpoint_1,
    )
    chain(
        metadata_tags_table_exists_1,
        [
            create_api_dataset_tags_metadata_table_1,
            Label("Census API tags\nmetadata table exists"),
        ],
        api_tags_table_endpoint_1,
    )


create_census_api_dataset_metadata_tables()
