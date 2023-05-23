import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
import pandas as pd

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


# @task
# def get_date_of_latest_api_catalog_check(
#     freshness_df: pd.DataFrame, task_logger: Logger
# ) -> pd.DataFrame:
#     time_of_last_check = freshness_df["time_of_check"].max()
#     time_since_last_check = pd.Timestamp.now(tz=time_of_last_check.tz) - freshness_df["time_of_check"].max()
#     task_logger.info(f"Time of most recent Census API Data Catalog check: {time_of_last_check}")
#     task_logger.info(f"Time since the last check: {time_since_last_check}")
#     return time_since_last_check.days


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
    # get_handler_1 = get_census_api_data_handler(task_logger=task_logger)
    # check_api_dataset_freshness_1 = check_api_dataset_freshness(
    # conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    # )

    # check_api_dataset_freshness_1


refresh_census_api_dataset_variables_metadata()
