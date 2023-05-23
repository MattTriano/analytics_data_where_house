import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd

from cc_utils.census import CensusAPIHandler, CensusDatasetSource
from tasks.census_tasks import (
    check_api_dataset_freshness,
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
DATASET_IDENTIFIER = "https://api.census.gov/data/id/ACSPUMS5Y2021"
MAX_DAYS_BEFORE_REFRESH = 30


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
def get_fresh_enough_api_handler(
    days_since_refresh: int, max_days_before_refresh: int, task_logger: Logger, **kwargs
) -> CensusAPIHandler:
    if days_since_refresh > max_days_before_refresh:
        task_logger.info(
            f"Census API Dataset Metadata was last pulled over {days_since_refresh} days ago,\n"
            + f"which is over the threshold of {max_days_before_refresh} days.\nConsider running"
            + f"the refresh_census_api_metadata DAG before checking further API variables."
        )
        return get_census_api_data_handler(task_logger=task_logger)
    else:
        ti = kwargs["ti"]
        metadata_df = ti.xcom_pull(task_ids="get_latest_catalog_freshness_from_db")
        api_handler = CensusAPIHandler(metadata_df=metadata_df)
        task_logger.info(
            f"Local census API Dataset Metadata is fresh enough. Returning CensusAPIHandler "
            + "reconstituted from cached data."
        )
        return api_handler


@task
def update_api_dataset_variables_metadata(
    identifier: str, api_handler: CensusAPIHandler, task_logger: Logger
) -> CensusDatasetSource:
    dataset_source = api_handler.catalog.get_dataset_source(identifier=identifier)
    dataset_source = api_handler.catalog.get_dataset_source(identifier=identifier)
    variables_df = dataset_source.variables_df.copy()
    col_order = ["dataset_id"]
    col_order.extend(list(variables_df.columns))
    col_order.extend(["dataset_last_modified", "time_of_check"])
    dataset_metadata_df = api_handler.catalog.dataset_metadata.loc[api_handler.catalog.dataset_metadata["identifier"] == identifier].copy()
    dataset_metadata_df = dataset_metadata_df.sort_values(by="time_of_check", ascending=False)
    variables_df["dataset_id"] = dataset_metadata_df["id"].values[0]
    variables_df["dataset_last_modified"] = pd.Timestamp(dataset_metadata_df["modified"].values[0])
    variables_df["time_of_check"] = pd.Timestamp(dataset_metadata_df["time_of_check"].values[0])
    variables_df = variables_df[col_order].copy()

    # This task isn't complete and will probably be split into multiple tasks.

    # api_dataset_variables_metadata_table = get_reflected_db_table(
    #     engine=engine, table_name="census_api_variables_metadata", schema_name="metadata"
    # )
    # insert_statement = (
    #     insert(api_dataset_variables_metadata_table)
    #     .values(variables_df.to_dict(orient="records"))
    #     .returning(api_dataset_variables_metadata_table)
    # )
    # ingested_api_dataset_variables_df = execute_result_returning_orm_query(
    #     engine=engine, select_query=insert_statement
    # )


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
    # latest_check_time = get_date_of_latest_api_catalog_check(
    #     freshness_df=local_freshness_1, task_logger=task_logger
    # )
    days_since_last_check = get_days_since_last_api_catalog_freshness_check(
        freshness_df=local_freshness_1, task_logger=task_logger
    )
    local_dataset_freshness_1 = get_latest_dataset_freshness_from_db(
        freshness_df=local_freshness_1, identifier=DATASET_IDENTIFIER, task_logger=task_logger
    )
    single_dataset_check_1 = get_latest_dateset_variables_from_db(
        identifier=DATASET_IDENTIFIER, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    api_handler = get_fresh_enough_api_handler(
        days_since_refresh=days_since_last_check,
        max_days_before_refresh=MAX_DAYS_BEFORE_REFRESH,
        task_logger=task_logger,
    )
    # get_handler_1 = get_census_api_data_handler(task_logger=task_logger)
    # check_api_dataset_freshness_1 = check_api_dataset_freshness(
    # conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    # )

    # check_api_dataset_freshness_1


refresh_census_api_dataset_variables_metadata()
