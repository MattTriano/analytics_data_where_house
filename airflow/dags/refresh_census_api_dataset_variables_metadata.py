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
from tasks.census_tasks import (
    get_census_api_data_handler,
    get_latest_catalog_freshness_from_db,
)
from cc_utils.db import (
    get_pg_engine,
    execute_result_returning_query,
    get_reflected_db_table,
    execute_result_returning_orm_query,
)


task_logger = logging.getLogger("airflow.task")
pd.options.display.max_columns = None

POSTGRES_CONN_ID = "dwh_db_conn"
DATASET_IDENTIFIER = "https://api.census.gov/data/id/PDBTRACT2022"
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
def get_latest_dataset_freshness_from_db(
    freshness_df: pd.DataFrame, identifier: str, task_logger: Logger
) -> pd.DataFrame:
    dataset_metadata_in_local_db_mask = freshness_df["identifier"] == identifier
    if dataset_metadata_in_local_db_mask.sum() > 0:
        local_dataset_freshness = freshness_df.loc[dataset_metadata_in_local_db_mask].copy()
        task_logger.info(
            f"Distinct datasets in census_api_metadata table {local_dataset_freshness}."
        )
        return local_dataset_freshness
    else:
        raise Exception(
            f"Dataset identifier '{identifier}' not found in local metadata.\n"
            + f"If you're sure the identifier is valid, refresh Census API catalog metadata and "
            + "try again."
        )


@task
def get_latest_dateset_variables_from_db(
    identifier: str, conn_id: str, task_logger: Logger
) -> pd.DataFrame:
    engine = get_pg_engine(conn_id=conn_id)
    local_api_dataset_variables_df = execute_result_returning_query(
        engine=engine,
        query=f"""
            WITH latest_metadata AS (
                SELECT
                    *,
                    row_number() over(
                        partition by dataset_last_modified ORDER BY dataset_last_modified DESC
                    ) as rn
                FROM metadata.census_api_variables_metadata
                where identifier = '{identifier}'
            )
            SELECT *
            FROM latest_metadata
            WHERE rn = 1;
        """,
    )
    task_logger.info(
        f"Most recently ingested dataset variables for identifier {identifier}\n"
        + f"{local_api_dataset_variables_df}."
    )
    return local_api_dataset_variables_df


@task
def get_fresh_enough_api_handler(
    local_metadata_df: pd.DataFrame, max_days_before_refresh: int, task_logger: Logger, **kwargs
) -> CensusAPIHandler:
    days_since_refresh = get_days_since_last_api_catalog_freshness_check(
        freshness_df=local_metadata_df, task_logger=task_logger
    )
    task_logger.info(f"days_since_refresh: {days_since_refresh}")
    task_logger.info(f"max_days_before_refresh: {max_days_before_refresh}")
    if days_since_refresh > max_days_before_refresh:
        task_logger.info(
            f"Census API Dataset Metadata was last pulled over {days_since_refresh} days ago,\n"
            + f"which is over the threshold of {max_days_before_refresh} days.\nConsider running"
            + f"the refresh_census_api_metadata DAG before checking further API variables."
        )
        return get_census_api_data_handler(task_logger=task_logger)
    else:
        task_logger.info(f"In else block pre ti: {days_since_refresh}")
        ti = kwargs["ti"]
        metadata_df = ti.xcom_pull(task_ids="get_latest_catalog_freshness_from_db")
        task_logger.info(f"In else block metadata_df.shape: {metadata_df.shape}")
        api_handler = CensusAPIHandler(metadata_df=metadata_df)
        task_logger.info(
            f"Local census API Dataset Metadata is fresh enough. Returning CensusAPIHandler "
            + "reconstituted from cached data."
        )
        return api_handler


@task
def update_api_dataset_variables_metadata(
    identifier: str, api_handler: CensusAPIHandler, conn_id: str, task_logger: Logger
) -> CensusDatasetSource:
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
    identifier: str, api_handler: CensusAPIHandler, conn_id: str, task_logger: Logger
) -> CensusDatasetSource:
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
    identifier: str, api_handler: CensusAPIHandler, conn_id: str, task_logger: Logger
) -> str:
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


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def fresher_dataset_variables_metadata_available(
    local_metadata_df: pd.DataFrame,
    local_variables_metadata_df: pd.DataFrame,
    max_days_before_refresh: int,
    task_logger: Logger,
) -> str:
    days_since_refresh = get_days_since_last_api_catalog_freshness_check(
        freshness_df=local_metadata_df, task_logger=task_logger
    )
    if days_since_refresh > max_days_before_refresh:
        task_logger.info(
            f"Census API Dataset Metadata was last pulled over {days_since_refresh} days ago,\n"
            + f"which is over the threshold of {max_days_before_refresh} days.\nConsider running"
            + f"the refresh_census_api_metadata DAG before checking further API variables."
        )
    else:
        task_logger.info(f"\nlocal_metadata_df: {local_metadata_df}\n")
        task_logger.info(f"\nlocal_variables_metadata_df: {local_variables_metadata_df}\n")
        dataset_last_modified = local_metadata_df["modified"].values[0]
        if len(local_variables_metadata_df) == 0:
            return "get_fresh_enough_api_handler"
        dataset_variables_last_modified = local_variables_metadata_df[
            "dataset_last_modified"
        ].values[0]
        task_logger.info(f"\n\n  dataset_last_modified: {dataset_last_modified}\n\n")
        task_logger.info(
            f"  dataset_variables_last_modified: {dataset_variables_last_modified}\n\n"
        )
        if dataset_last_modified > dataset_variables_last_modified:
            return "get_fresh_enough_api_handler"
        else:
            return "dataset_variables_metadata_are_fresh"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dataset_variables_metadata_are_fresh() -> str:
    return "variables_metadata_are_fresh"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["census", "metadata"],
)
def refresh_census_api_dataset_variables_metadata():
    local_freshness_1 = get_latest_catalog_freshness_from_db(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    local_dataset_freshness_1 = get_latest_dataset_freshness_from_db(
        freshness_df=local_freshness_1,
        identifier=DATASET_IDENTIFIER,
        task_logger=task_logger,
    )
    single_dataset_check_1 = get_latest_dateset_variables_from_db(
        identifier=DATASET_IDENTIFIER, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    fresher_variables_metadata_exists_1 = fresher_dataset_variables_metadata_available(
        local_metadata_df=local_dataset_freshness_1,
        local_variables_metadata_df=single_dataset_check_1,
        max_days_before_refresh=MAX_DAYS_BEFORE_REFRESH,
        task_logger=task_logger,
    )
    api_handler = get_fresh_enough_api_handler(
        local_metadata_df=local_dataset_freshness_1,
        max_days_before_refresh=MAX_DAYS_BEFORE_REFRESH,
        task_logger=task_logger,
    )
    update_variables_metadata_1 = update_api_dataset_variables_metadata(
        identifier=DATASET_IDENTIFIER,
        api_handler=api_handler,
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
    )
    update_geographies_metadata_1 = update_api_dataset_geographies_metadata(
        identifier=DATASET_IDENTIFIER,
        api_handler=api_handler,
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
    )
    update_groups_metadata_1 = update_api_dataset_groups_metadata(
        identifier=DATASET_IDENTIFIER,
        api_handler=api_handler,
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
    )
    already_fresh = dataset_variables_metadata_are_fresh()

    chain(fresher_variables_metadata_exists_1, [already_fresh, api_handler])
    chain(
        api_handler,
        [
            update_variables_metadata_1,
            update_geographies_metadata_1,
            update_groups_metadata_1,
        ],
        already_fresh,
    )


refresh_census_api_dataset_variables_metadata()
