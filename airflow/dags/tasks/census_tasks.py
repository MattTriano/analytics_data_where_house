from logging import Logger

from airflow.decorators import task, task_group

import pandas as pd
from sqlalchemy import insert

from cc_utils.census import CensusAPIHandler
from cc_utils.db import (
    get_pg_engine,
    execute_result_returning_query,
    get_reflected_db_table,
    execute_result_returning_orm_query,
)


# @task
# def get_census_api_data_handler(task_logger: Logger) -> CensusAPIHandler:
#     api_handler = CensusAPIHandler()
#     n_datasets = api_handler.metadata_df["identifier"].nunique()
#     task_logger.info("Retrieved Census API data handler.")
#     task_logger.info(
#         f"Distinct datasets in catalog as of {api_handler.time_of_check}: {n_datasets}."
#     )
#     return api_handler


@task
def get_census_api_data_handler(
    conn_id: str, task_logger: Logger, max_days_before_refresh: int = 30
) -> CensusAPIHandler:
    api_handler = CensusAPIHandler(
        conn_id=conn_id,
        task_logger=task_logger,
        max_days_before_refresh=max_days_before_refresh,
    )
    n_datasets = api_handler.metadata_df["identifier"].nunique()
    task_logger.info("Retrieved Census API data handler.")
    task_logger.info(
        f"Distinct datasets in catalog as of {api_handler.time_of_check}: {n_datasets}."
    )
    return api_handler


@task
def get_latest_catalog_freshness_from_db(conn_id: str, task_logger: Logger) -> pd.DataFrame:
    engine = get_pg_engine(conn_id=conn_id)
    local_api_catalog_metadata_df = execute_result_returning_query(
        engine=engine,
        query="""
            WITH latest_metadata AS (
                SELECT
                    *,
                    row_number() over(
                        partition by identifier, modified ORDER BY identifier, modified DESC
                    ) as rn
                FROM metadata.census_api_metadata
            )
            SELECT *
            FROM latest_metadata
            WHERE rn = 1;
        """,
    )
    n_datasets = local_api_catalog_metadata_df["identifier"].nunique()
    task_logger.info(f"Distinct datasets in census_api_metadata table {n_datasets}.")
    return local_api_catalog_metadata_df


@task
def check_warehouse_dataset_freshness(
    api_handler: CensusAPIHandler, local_metadata_df: pd.DataFrame, task_logger: Logger
) -> pd.DataFrame:
    source_metadata_df = api_handler.metadata_df.copy()
    n_src_datasets = source_metadata_df["identifier"].nunique()
    n_local_datasets = local_metadata_df["identifier"].nunique()
    task_logger.info(f"Distinct datasets in source census_api_metadata table: {n_src_datasets}.")
    task_logger.info(f"Distinct datasets in local census_api_metadata table: {n_local_datasets}.")

    dset_freshness_df = pd.merge(
        left=source_metadata_df.copy(),
        right=local_metadata_df[["identifier", "modified"]].copy(),
        how="outer",
        on="identifier",
        suffixes=("_src", "_local"),
    )
    not_in_local_mask = dset_freshness_df["modified_local"].isnull()
    not_in_source_mask = dset_freshness_df["modified_src"].isnull()
    in_both_mask = ~not_in_source_mask & ~not_in_local_mask
    source_fresher_mask = pd.to_datetime(
        dset_freshness_df["modified_src"], errors="coerce"
    ) > pd.to_datetime(dset_freshness_df["modified_local"], errors="coerce")
    task_logger.info(f"dset_freshness_df.shape:          {dset_freshness_df.shape}")
    task_logger.info(f"Datasets in source but not local: {not_in_local_mask.sum()}")
    task_logger.info(f"Datasets in local but not source: {not_in_source_mask.sum()}")
    task_logger.info(f"Source is fresher (and dset in both): {source_fresher_mask.sum()}")
    task_logger.info(
        f"Source is fresher or only: {(source_fresher_mask | not_in_local_mask).sum()}"
    )

    task_logger.info(f"Datasets in local and source: {in_both_mask.sum()}")
    task_logger.info(f"column info: {dset_freshness_df.info()}")
    dset_freshness_df["modified_since_last_check"] = source_fresher_mask | not_in_local_mask
    return dset_freshness_df


@task
def ingest_api_dataset_freshness_check(
    freshness_df: pd.DataFrame, conn_id: str, task_logger: Logger
) -> pd.DataFrame:
    source_freshness_df = freshness_df.loc[freshness_df["modified_src"].notnull()].copy()
    source_freshness_df = source_freshness_df.reset_index(drop=True)
    source_freshness_df = source_freshness_df.drop(columns="modified_local")
    source_freshness_df = source_freshness_df.rename(columns={"modified_src": "modified"})
    engine = get_pg_engine(conn_id=conn_id)
    api_dataset_metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(api_dataset_metadata_table)
        .values(source_freshness_df.to_dict(orient="records"))
        .returning(api_dataset_metadata_table)
    )
    ingested_api_datasets_df = execute_result_returning_orm_query(
        engine=engine, select_query=insert_statement
    )
    task_logger.info(
        f"Max census_api_metadata id value after ingestion: {ingested_api_datasets_df['id'].max()}"
    )
    return ingested_api_datasets_df


@task_group
def check_api_dataset_freshness(
    conn_id: str, task_logger: Logger, max_days_before_refresh: int = 30
) -> pd.DataFrame:
    get_handler_1 = get_census_api_data_handler(
        conn_id=conn_id,
        task_logger=task_logger,
        max_days_before_refresh=max_days_before_refresh,
    )
    latest_local_metadata_1 = get_latest_catalog_freshness_from_db(
        conn_id=conn_id, task_logger=task_logger
    )
    freshness_check_1 = check_warehouse_dataset_freshness(
        api_handler=get_handler_1,
        local_metadata_df=latest_local_metadata_1,
        task_logger=task_logger,
    )
    ingest_check_1 = ingest_api_dataset_freshness_check(
        freshness_df=freshness_check_1, conn_id=conn_id, task_logger=task_logger
    )
    return ingest_check_1
