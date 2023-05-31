import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from cc_utils.census import CensusAPIHandler, CensusDatasetSource
from tasks.census_tasks import get_census_api_data_handler
from cc_utils.db import (
    get_pg_engine,
    get_reflected_db_table,
    execute_result_returning_orm_query,
)


task_logger = logging.getLogger("airflow.task")
pd.options.display.max_columns = None

POSTGRES_CONN_ID = "dwh_db_conn"
DATASET_IDENTIFIER = "https://api.census.gov/data/id/CPSVOTING202211"
MAX_DAYS_BEFORE_REFRESH = 30


def get_days_since_last_api_catalog_freshness_check(
    freshness_df: pd.DataFrame, task_logger: Logger
) -> pd.DataFrame:
    time_of_last_check = freshness_df["time_of_check"].max()
    time_since_last_check = (
        pd.Timestamp.now(tz=time_of_last_check.tz) - freshness_df["time_of_check"].max()
    )
    task_logger.info(f"Time of most recent Census API Data Catalog check: {time_of_last_check}")
    task_logger.info(f"Time since the last check: {time_since_last_check}")
    return time_since_last_check.days


@task
def update_api_dataset_variables_metadata(
    identifier: str, conn_id: str, task_logger: Logger, **kwargs
) -> CensusDatasetSource:
    ti = kwargs["ti"]
    api_handler = ti.xcom_pull(task_ids="get_census_api_data_handler")
    variables_df = api_handler.prepare_dataset_variables_metadata_df(identifier=identifier)
    task_logger.info(f"Dataset variables in the searched Census dataset: {len(variables_df)}")
    engine = get_pg_engine(conn_id=conn_id)
    api_dataset_variables_metadata_table = get_reflected_db_table(
        engine=engine,
        table_name="census_api_variables_metadata",
        schema_name="metadata",
    )
    insert_statement = (
        insert(api_dataset_variables_metadata_table)
        .values(variables_df.to_dict(orient="records"))
        .returning(api_dataset_variables_metadata_table)
    )
    ingested_api_dataset_variables_df = execute_result_returning_orm_query(
        engine=engine, select_query=insert_statement
    )
    return ingested_api_dataset_variables_df


@task
def update_api_dataset_geographies_metadata(
    identifier: str, conn_id: str, task_logger: Logger, **kwargs
) -> CensusDatasetSource:
    ti = kwargs["ti"]
    api_handler = ti.xcom_pull(task_ids="get_census_api_data_handler")
    geographies_df = api_handler.prepare_dataset_geographies_metadata_df(identifier=identifier)
    engine = get_pg_engine(conn_id=conn_id)
    api_dataset_geographies_metadata_table = get_reflected_db_table(
        engine=engine,
        table_name="census_api_geographies_metadata",
        schema_name="metadata",
    )
    insert_statement = (
        insert(api_dataset_geographies_metadata_table)
        .values(geographies_df.to_dict(orient="records"))
        .returning(api_dataset_geographies_metadata_table)
    )
    ingested_api_dataset_geographies_df = execute_result_returning_orm_query(
        engine=engine, select_query=insert_statement
    )
    return ingested_api_dataset_geographies_df


@task
def update_api_dataset_groups_metadata(
    identifier: str, conn_id: str, task_logger: Logger, **kwargs
) -> str:
    ti = kwargs["ti"]
    api_handler = ti.xcom_pull(task_ids="get_census_api_data_handler")
    groups_df = api_handler.prepare_dataset_groups_metadata_df(identifier=identifier)
    if (groups_df is not None) and (len(groups_df) > 0):
        engine = get_pg_engine(conn_id=conn_id)
        api_dataset_groups_metadata_table = get_reflected_db_table(
            engine=engine,
            table_name="census_api_groups_metadata",
            schema_name="metadata",
        )
        task_logger.info(f"Dataset groups in the searched Census dataset: {len(groups_df)}")
        insert_statement = (
            pg_insert(api_dataset_groups_metadata_table)
            .values(groups_df.to_dict(orient="records"))
            .on_conflict_do_nothing()
            .returning(api_dataset_groups_metadata_table)
        )
        ingested_api_dataset_groups_df = execute_result_returning_orm_query(
            engine=engine, select_query=insert_statement
        )
        task_logger.info(
            f"New dataset groups added to group metadata table: "
            + f"{len(ingested_api_dataset_groups_df)}"
        )
        return "Successfully_ingested_groups_metadata"
    else:
        return "No_groups_metadata_to_ingest"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dataset_variables_metadata_are_fresh() -> str:
    return "variables_metadata_are_fresh"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dataset_variables_metadata_are_not_fresh() -> str:
    return "dataset_variables_metadata_are_not_fresh"


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def get_latest_dataset_freshness_from_db(
    api_handler: CensusAPIHandler, identifier: str, task_logger: Logger
) -> str:
    if api_handler.is_local_dataset_metadata_fresh(identifier=identifier):
        return "dataset_variables_metadata_are_fresh"
    else:
        return "dataset_variables_metadata_are_not_fresh"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["census", "metadata"],
)
def refresh_census_api_dataset_variables_metadata():
    api_handler = get_census_api_data_handler(
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        max_days_before_refresh=MAX_DAYS_BEFORE_REFRESH,
    )
    local_dataset_freshness_1 = get_latest_dataset_freshness_from_db(
        api_handler=api_handler,
        identifier=DATASET_IDENTIFIER,
        task_logger=task_logger,
    )
    vars_not_fresh = dataset_variables_metadata_are_not_fresh()
    update_variables_metadata_1 = update_api_dataset_variables_metadata(
        identifier=DATASET_IDENTIFIER,
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
    )
    update_geographies_metadata_1 = update_api_dataset_geographies_metadata(
        identifier=DATASET_IDENTIFIER,
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
    )
    update_groups_metadata_1 = update_api_dataset_groups_metadata(
        identifier=DATASET_IDENTIFIER,
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
    )
    already_fresh = dataset_variables_metadata_are_fresh()

    chain(local_dataset_freshness_1, [already_fresh, vars_not_fresh])
    chain(
        vars_not_fresh,
        [
            update_variables_metadata_1,
            update_geographies_metadata_1,
            update_groups_metadata_1,
        ],
        already_fresh,
    )


refresh_census_api_dataset_variables_metadata()
