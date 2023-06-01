import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from sqlalchemy import insert

from cc_utils.db import (
    get_pg_engine,
    get_reflected_db_table,
    execute_result_returning_orm_query,
    get_data_table_names_in_schema,
    execute_structural_command,
)
from cc_utils.census import CensusAPIHandler, CensusAPIDataset
from tasks.census_tasks import get_census_api_data_handler
from sources.census_api_datasets import GROSS_RENT_BY_COOK_COUNTY_IL_TRACT as CENSUS_DATASET

task_logger = logging.getLogger("airflow.task")
POSTGRES_CONN_ID = "dwh_db_conn"
MAX_DAYS_BEFORE_REFRESH = 30
pd.options.display.max_columns = None


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def check_dataset_freshness_in_gen_metadata(
    census_dataset, api_handler: CensusAPIHandler, task_logger: Logger
):
    identifier = census_dataset.api_call_obj.identifier
    local_dataset_metadata_df = api_handler.get_freshest_local_dataset_metadata(
        dataset_name=census_dataset.dataset_name
    )
    dataset_metadata_df = api_handler.metadata_df.loc[
        api_handler.metadata_df["identifier"] == identifier
    ].copy()
    task_logger.info(f"local_dataset_metadata_df: {local_dataset_metadata_df.T}")
    if (len(local_dataset_metadata_df) == 0) or (
        dataset_metadata_df["modified"].max()
        > local_dataset_metadata_df["source_data_last_modified"].max()
    ):
        return "ingest_dataset_freshness_into_gen_metadata"
    else:
        return "dataset_metadata_is_fresh"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dataset_metadata_is_fresh() -> str:
    return "dataset_metadata_is_fresh"


@task
def ingest_dataset_freshness_into_gen_metadata(
    census_dataset, api_handler: CensusAPIHandler, task_logger: Logger
) -> pd.DataFrame:
    identifier = census_dataset.api_call_obj.identifier
    dataset_name = census_dataset.dataset_name
    dataset_metadata_df = api_handler.metadata_df.loc[
        api_handler.metadata_df["identifier"] == identifier
    ].copy()
    metadata_check_df = pd.DataFrame(
        {
            "dataset_name": [dataset_name],
            "source_data_last_modified": [dataset_metadata_df["modified"].max()],
            "time_of_check": [dataset_metadata_df["time_of_check"].max()],
        }
    )
    task_logger.info(f"metadata_check_df: {metadata_check_df.T}")
    engine = get_pg_engine(conn_id=api_handler.conn_id)
    dataset_metadata_table = get_reflected_db_table(
        engine=engine, table_name="dataset_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(dataset_metadata_table)
        .values(metadata_check_df.to_dict(orient="records"))
        .returning(dataset_metadata_table)
    )
    ingested_metadata_check_df = execute_result_returning_orm_query(
        engine=engine, select_query=insert_statement
    )
    task_logger.info(f"ingested_api_datasets_df: {ingested_metadata_check_df.T}")
    return ingested_metadata_check_df


@task
def download_and_ingest_dataset(
    census_dataset: CensusAPIDataset,
    conn_id: str,
    metadata_check_df: pd.DataFrame,
    task_logger: Logger,
) -> int:
    dataset_df = census_dataset.make_api_call()
    task_logger.info(f"Rows and columns in returned dataset: {dataset_df.shape}")
    table_name = f"temp_{census_dataset.dataset_name}"
    dataset_df["source_data_last_modified"] = metadata_check_df["source_data_last_modified"].max()
    dataset_df["time_of_check"] = metadata_check_df["time_of_check"].max()
    engine = get_pg_engine(conn_id=conn_id)
    dataset_df.to_sql(
        name=table_name,
        schema="data_raw",
        con=engine,
        if_exists="replace",
        chunksize=100000,
    )
    task_logger.info("Successfully ingested data using pd.to_sql()")
    task_logger.info("Dataset metadata record id: {metadata_check_df['id'].max()}")
    return metadata_check_df["id"].max()


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_result_of_check_in_metadata_table(conn_id: str, task_logger=task_logger):
    engine = get_pg_engine(conn_id=conn_id)
    # metadata_table = get_reflected_db_table(
    #     engine=engine, table_name="dataset_metadata", schema_name="metadata"
    # )
    # update_query = (
    #         update(metadata_table)
    #         .where(metadata_table.c.time_of_check == self.data_freshness_check["time_of_check"])
    #         .values(data_pulled_this_check=data_pulled_value)
    #     )


@dag(
    schedule=CENSUS_DATASET.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "census"],
)
def dev_update_gross_rents_by_census_tract():
    api_handler = get_census_api_data_handler(
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
        max_days_before_refresh=MAX_DAYS_BEFORE_REFRESH,
    )
    check_freshness_1 = check_dataset_freshness_in_gen_metadata(
        census_dataset=CENSUS_DATASET, api_handler=api_handler, task_logger=task_logger
    )
    ingest_freshness_check_1 = ingest_dataset_freshness_into_gen_metadata(
        census_dataset=CENSUS_DATASET, api_handler=api_handler, task_logger=task_logger
    )
    dataset_is_fresh = dataset_metadata_is_fresh()
    chain(check_freshness_1, [dataset_is_fresh, ingest_freshness_check_1])


dev_update_gross_rents_by_census_tract()
