from dataclasses import dataclass
import datetime as dt
import logging
from logging import Logger
import os
from typing import Optional


from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import requests
from sqlalchemy import insert

from cc_utils.census import CensusGeogTract
from cc_utils.db import (
    get_pg_engine,
    get_reflected_db_table,
    execute_result_returning_orm_query,
    get_data_table_names_in_schema,
    execute_structural_command,
    execute_result_returning_query,
)
from cc_utils.census import CensusAPIHandler, CensusAPIDataset, CensusAPIDatasetSource
from tasks.census_tasks import get_census_api_data_handler

# from sources.census_api_datasets import GROSS_RENT_BY_COOK_COUNTY_IL_TRACT as CENSUS_DATASET

task_logger = logging.getLogger("airflow.task")
POSTGRES_CONN_ID = "dwh_db_conn"
MAX_DAYS_BEFORE_REFRESH = 30
pd.options.display.max_columns = None

DATASET_BASE_URL = "http://api.census.gov/data/2021/acs/acs5"
group_name = "B25001"


class CensusDatasetFreshnessCheck:
    def __init__(
        self,
        dataset_source: CensusAPIDatasetSource,
        source_freshness: pd.DataFrame,
        local_freshness: pd.DataFrame,
    ):
        self.dataset_source = dataset_source
        self.source_freshness = source_freshness
        self.local_freshness = local_freshness


class CensusVariableGroupAPICall(CensusAPIDataset):
    def __init__(
        self,
        dataset_base_url: str,
        group_name: str,
        geographies: CensusGeogTract,
    ):
        self.dataset_base_url = dataset_base_url
        self.group_name = group_name
        self.geographies = geographies

    @property
    def api_call(self) -> str:
        base_url = self.dataset_base_url
        group_part = f"group({self.group_name})"
        geog_part = self.geographies.api_call_geographies
        auth_part = f"""key={os.environ["CENSUS_API_KEY"]}"""
        return f"{base_url}?get={group_part}&{geog_part}&{auth_part}"

    def make_api_call(self) -> pd.DataFrame:
        resp = requests.get(self.api_call)
        if resp.status_code == 200:
            resp_json = resp.json()
            return pd.DataFrame(resp_json[1:], columns=resp_json[0])
        else:
            raise Exception(f"The API call produced an invalid response ({resp.status_code})")


@dataclass
class CensusVariableGroupDataset:
    dataset_name: str
    api_call_obj: CensusVariableGroupAPICall
    schedule: Optional[str] = None


CC_HOUSING_UNITS_PER_CENSUS_TRACT = CensusVariableGroupDataset(
    dataset_name="cc_housing_units_per_census_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B25001",
        geographies=CensusGeogTract(state_cd="17", county_cd="031"),
    ),
    schedule="40 5 2 3,9 *",
)
CENSUS_DATASET = CC_HOUSING_UNITS_PER_CENSUS_TRACT


@task
def get_source_dataset_metadata(
    census_dataset: CensusVariableGroupDataset, task_logger: Logger
) -> CensusAPIDatasetSource:
    dataset_base_url = census_dataset.api_call_obj.dataset_base_url
    dataset_source = CensusAPIDatasetSource(dataset_base_url=dataset_base_url)
    task_logger.info(f"Census dataset {dataset_base_url} metadata details:")
    task_logger.info(f"  - Dataset variables:   {len(dataset_source.variables_df)}")
    task_logger.info(f"  - Dataset geographies: {len(dataset_source.geographies_df)}")
    task_logger.info(f"  - Dataset groups:      {len(dataset_source.groups_df)}")
    task_logger.info(f"  - Dataset tags:        {len(dataset_source.tags_df)}")
    return dataset_source


@task
def record_source_freshness_check(
    dataset_source: CensusAPIDatasetSource,
    census_dataset: CensusVariableGroupDataset,
    conn_id: str,
    task_logger: Logger,
) -> pd.DataFrame:
    dataset_metadata_df = dataset_source.metadata_catalog_df.loc[
        dataset_source.metadata_catalog_df["dataset_base_url"]
        == census_dataset.api_call_obj.dataset_base_url
    ].copy()
    # task_logger.info(f"dataset_source.metadata_catalog_df: {dataset_source.metadata_catalog_df}\n\n\n\n\n\n\n")
    # task_logger.info(f"dataset_source.geographies_df: {dataset_source.geographies_df}\n\n\n\n\n\n\n")
    # task_logger.info(f"dataset_source.variables_df: {dataset_source.variables_df}\n\n\n\n\n\n\n")
    # task_logger.info(f"dataset_source.groups_df: {dataset_source.groups_df}\n\n\n\n\n\n\n")

    freshness_check_record = pd.DataFrame(
        {
            "dataset_name": [census_dataset.dataset_name],
            "source_data_last_modified": [dataset_metadata_df["modified"].max()],
            "time_of_check": [dataset_source.time_of_check],
        }
    )
    task_logger.info(f"dataset name:          {freshness_check_record['dataset_name']}")
    task_logger.info(
        f"dataset last_modified: {freshness_check_record['source_data_last_modified']}"
    )
    task_logger.info(f"dataset time_of_check: {freshness_check_record['time_of_check']}")

    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="dataset_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table)
        .values(freshness_check_record.to_dict(orient="records"))
        .returning(metadata_table)
    )
    source_freshness_df = execute_result_returning_orm_query(
        engine=engine, select_query=insert_statement
    )
    return source_freshness_df


@task
def get_latest_local_freshness_check(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> pd.DataFrame:
    dataset_name = census_dataset.dataset_name
    engine = get_pg_engine(conn_id=conn_id)
    latest_dataset_check_df = execute_result_returning_query(
        engine=engine,
        query=f"""
            WITH latest_metadata AS (
                SELECT
                    *,
                    row_number() over(
                        partition by dataset_name, source_data_last_modified
                        ORDER BY dataset_name, source_data_last_modified DESC
                    ) as rn
                FROM metadata.dataset_metadata
                WHERE
                    dataset_name = '{dataset_name}'
                    AND local_data_updated IS TRUE
            )
            SELECT *
            FROM latest_metadata
            WHERE rn = 1;
        """,
    )
    latest_dataset_check_df = latest_dataset_check_df.drop(columns=["rn"])
    source_last_modified = latest_dataset_check_df["source_data_last_modified"].max()
    task_logger.info(
        f"Last_modified datetime of latest local dataset update: {source_last_modified}."
    )
    return latest_dataset_check_df


@task
def organize_freshness_check_results(task_logger: Logger, **kwargs) -> CensusDatasetFreshnessCheck:
    ti = kwargs["ti"]
    freshness_check = CensusDatasetFreshnessCheck(
        dataset_source=ti.xcom_pull(task_ids="check_freshness.get_source_dataset_metadata"),
        source_freshness=ti.xcom_pull(task_ids="check_freshness.record_source_freshness_check"),
        local_freshness=ti.xcom_pull(task_ids="check_freshness.get_latest_local_freshness_check"),
    )
    return freshness_check


@task_group
def check_freshness(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> CensusDatasetFreshnessCheck:
    local_dataset_freshness = get_latest_local_freshness_check(
        census_dataset=census_dataset, conn_id=conn_id, task_logger=task_logger
    )
    dataset_source = get_source_dataset_metadata(
        census_dataset=census_dataset, task_logger=task_logger
    )
    source_dataset_freshness = record_source_freshness_check(
        dataset_source=dataset_source,
        census_dataset=census_dataset,
        conn_id=conn_id,
        task_logger=task_logger,
    )
    freshness_check = organize_freshness_check_results(task_logger=task_logger)
    chain(local_dataset_freshness, dataset_source)
    return freshness_check


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def fresher_source_data_available(
    freshness_check: CensusDatasetFreshnessCheck, task_logger: Logger
) -> str:
    dataset_not_in_local_dwh = len(freshness_check.local_freshness) == 0
    local_dataset_is_stale = (
        freshness_check.source_freshness["source_data_last_modified"]
        > freshness_check.local_freshness["source_data_last_modified"]
    )
    if (dataset_not_in_local_dwh) or (local_dataset_is_stale):
        return "update_local_data.update_local_metadata"
    else:
        return "local_data_is_fresh"


@task
def update_local_metadata(task_logger: Logger, **kwargs) -> CensusDatasetFreshnessCheck:
    ti = kwargs["ti"]
    freshness_check = ti.xcom_pull(task_ids="check_freshness.organize_freshness_check_results")
    return freshness_check


@task
def ingest_dataset_variables_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    variables_df = freshness_check.dataset_source.variables_df.copy()
    task_logger.info(f"Columns in variables_df:")
    for col in variables_df.columns:
        task_logger.info(f"  {col}")
    print("hi")
    return "success"


@task
def ingest_dataset_geographies_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    geographies_df = freshness_check.dataset_source.geographies_df.copy()
    task_logger.info(f"Columns in geographies_df:")
    for col in geographies_df.columns:
        task_logger.info(f"  {col}")
    print("hi")
    return "success"


@task
def ingest_dataset_groups_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    groups_df = freshness_check.dataset_source.groups_df.copy()
    task_logger.info(f"Columns in groups_df:")
    for col in groups_df.columns:
        task_logger.info(f"  {col}")
    print("hi")
    return "success"


@task
def ingest_dataset_tags_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    tags_df = freshness_check.dataset_source.tags_df.copy()
    task_logger.info(f"Columns in tags_df:")
    for col in tags_df.columns:
        task_logger.info(f"  {col}")
    print("hi")
    return "success"


@task_group
def update_local_data(conn_id: str, task_logger: Logger):
    freshness_check = update_local_metadata(task_logger=task_logger)
    update_dataset_variables = ingest_dataset_variables_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )
    update_dataset_geographies = ingest_dataset_geographies_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )
    update_dataset_groups = ingest_dataset_groups_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )
    update_dataset_tags = ingest_dataset_tags_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )

    # chain(
    #     freshness_check,
    #     [
    #         update_dataset_variables,
    #         update_dataset_geographies,
    #         update_dataset_groups,
    #         update_dataset_tags
    #     ]
    # )
    # return "hi"


@task
def local_data_is_fresh(task_logger: Logger):
    return "hi"


# @task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
# def check_dataset_freshness_in_gen_metadata(
#     census_dataset, api_handler: CensusAPIHandler, task_logger: Logger
# ):
#     identifier = census_dataset.api_call_obj.identifier
#     local_dataset_metadata_df = api_handler.get_freshest_local_dataset_metadata(
#         dataset_name=census_dataset.dataset_name
#     )
#     dataset_metadata_df = api_handler.metadata_df.loc[
#         api_handler.metadata_df["identifier"] == identifier
#     ].copy()
#     task_logger.info(f"local_dataset_metadata_df: {local_dataset_metadata_df.T}")
#     if (len(local_dataset_metadata_df) == 0) or (
#         dataset_metadata_df["modified"].max()
#         > local_dataset_metadata_df["source_data_last_modified"].max()
#     ):
#         return "ingest_dataset_freshness_into_gen_metadata"
#     else:
#         return "dataset_metadata_is_fresh"


# @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
# def dataset_metadata_is_fresh() -> str:
#     return "dataset_metadata_is_fresh"


# @task
# def ingest_dataset_freshness_into_gen_metadata(
#     census_dataset, api_handler: CensusAPIHandler, task_logger: Logger
# ) -> pd.DataFrame:
#     identifier = census_dataset.api_call_obj.identifier
#     dataset_name = census_dataset.dataset_name
#     dataset_metadata_df = api_handler.metadata_df.loc[
#         api_handler.metadata_df["identifier"] == identifier
#     ].copy()
#     metadata_check_df = pd.DataFrame(
#         {
#             "dataset_name": [dataset_name],
#             "source_data_last_modified": [dataset_metadata_df["modified"].max()],
#             "time_of_check": [dataset_metadata_df["time_of_check"].max()],
#         }
#     )
#     task_logger.info(f"metadata_check_df: {metadata_check_df.T}")
#     engine = get_pg_engine(conn_id=api_handler.conn_id)
#     dataset_metadata_table = get_reflected_db_table(
#         engine=engine, table_name="dataset_metadata", schema_name="metadata"
#     )
#     insert_statement = (
#         insert(dataset_metadata_table)
#         .values(metadata_check_df.to_dict(orient="records"))
#         .returning(dataset_metadata_table)
#     )
#     ingested_metadata_check_df = execute_result_returning_orm_query(
#         engine=engine, select_query=insert_statement
#     )
#     task_logger.info(f"ingested_api_datasets_df: {ingested_metadata_check_df.T}")
#     return ingested_metadata_check_df


# @task
# def download_and_ingest_dataset(
#     census_dataset: CensusAPIDataset,
#     conn_id: str,
#     metadata_check_df: pd.DataFrame,
#     task_logger: Logger,
# ) -> int:
#     dataset_df = census_dataset.make_api_call()
#     task_logger.info(f"Rows and columns in returned dataset: {dataset_df.shape}")
#     table_name = f"temp_{census_dataset.dataset_name}"
#     dataset_df["source_data_last_modified"] = metadata_check_df["source_data_last_modified"].max()
#     dataset_df["time_of_check"] = metadata_check_df["time_of_check"].max()
#     engine = get_pg_engine(conn_id=conn_id)
#     dataset_df.to_sql(
#         name=table_name,
#         schema="data_raw",
#         con=engine,
#         if_exists="replace",
#         chunksize=100000,
#     )
#     task_logger.info("Successfully ingested data using pd.to_sql()")
#     task_logger.info("Dataset metadata record id: {metadata_check_df['id'].max()}")
#     return metadata_check_df["id"].max()


# @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
# def update_result_of_check_in_metadata_table(conn_id: str, task_logger=task_logger):
#     engine = get_pg_engine(conn_id=conn_id)
#     metadata_table = get_reflected_db_table(
#         engine=engine, table_name="dataset_metadata", schema_name="metadata"
#     )
#     update_query = (
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
def dev_update_cc_housing_units_by_census_tract():
    freshness_check_1 = check_freshness(
        census_dataset=CENSUS_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        freshness_check=freshness_check_1, task_logger=task_logger
    )
    update_local_1 = update_local_data(conn_id=POSTGRES_CONN_ID, task_logger=task_logger)
    local_is_fresh_1 = local_data_is_fresh(task_logger=task_logger)
    chain(freshness_check_1, fresh_source_data_available_1, [update_local_1, local_is_fresh_1])
    # local_dataset_freshness = get_latest_local_freshness_check(
    #     census_dataset=CENSUS_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    # )
    # dataset_source = get_source_dataset_metadata(
    #     census_dataset=CENSUS_DATASET, task_logger=task_logger
    # )
    # source_dataset_freshness = record_source_freshness_check(
    #     dataset_source=dataset_source, census_dataset=CENSUS_DATASET,
    #     conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    # )


dev_update_cc_housing_units_by_census_tract()
