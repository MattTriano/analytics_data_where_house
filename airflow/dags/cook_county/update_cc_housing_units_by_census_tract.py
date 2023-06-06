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
    chain(source_dataset_freshness, freshness_check)
    return freshness_check


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def fresher_source_data_available(
    freshness_check: CensusDatasetFreshnessCheck, task_logger: Logger
) -> str:
    dataset_in_local_dwh = len(freshness_check.local_freshness) > 0

    task_logger.info(f"Dataset in local dwh: {dataset_in_local_dwh}")
    task_logger.info(f"freshness_check.local_freshness: {freshness_check.local_freshness}")
    if dataset_in_local_dwh:
        local_last_modified = freshness_check.local_freshness["source_data_last_modified"].max()
        task_logger.info(f"Local dataset last modified: {local_last_modified}")
        source_last_modified = freshness_check.source_freshness["source_data_last_modified"].max()
        task_logger.info(f"Source dataset last modified: {source_last_modified}")
        local_dataset_is_fresh = local_last_modified >= source_last_modified
        if local_dataset_is_fresh:
            return "local_data_is_fresh"
    return "update_local_metadata.get_freshness_check_results"


@task
def get_freshness_check_results(task_logger: Logger, **kwargs) -> CensusDatasetFreshnessCheck:
    ti = kwargs["ti"]
    freshness_check = ti.xcom_pull(task_ids="check_freshness.organize_freshness_check_results")
    return freshness_check


@task
def ingest_dataset_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    metadata_df = freshness_check.dataset_source.metadata_catalog_df.copy()
    metadata_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    task_logger.info(f"Dataset metadata columns:")
    for col in metadata_df.columns:
        task_logger.info(f"  {col}")
    task_logger.info(f"{metadata_df.T}")
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table)
        .values(metadata_df.to_dict(orient="records"))
        .returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    return "success"


@task
def ingest_dataset_variables_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    variables_df = freshness_check.dataset_source.variables_df.copy()
    variables_df["dataset_base_url"] = freshness_check.dataset_source.base_url
    variables_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    variables_df["dataset_last_modified"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    variables_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_variables_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table)
        .values(variables_df.to_dict(orient="records"))
        .returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    task_logger.info(f"Variables ingested: {len(ingested_df)}")
    return "success"


@task
def ingest_dataset_geographies_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    geographies_df = freshness_check.dataset_source.geographies_df.copy()
    geographies_df["dataset_base_url"] = freshness_check.dataset_source.base_url
    geographies_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    geographies_df["dataset_last_modified"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    geographies_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_geographies_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table)
        .values(geographies_df.to_dict(orient="records"))
        .returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    task_logger.info(f"Geographies ingested: {len(ingested_df)}")
    return "success"


@task
def ingest_dataset_groups_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    groups_df = freshness_check.dataset_source.groups_df.copy()
    groups_df["dataset_base_url"] = freshness_check.dataset_source.base_url
    groups_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    groups_df["dataset_last_modified"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    groups_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_groups_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table).values(groups_df.to_dict(orient="records")).returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    task_logger.info(f"Groups ingested: {len(ingested_df)}")
    return "success"


@task
def ingest_dataset_tags_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    tags_df = freshness_check.dataset_source.tags_df.copy()
    tags_df["dataset_base_url"] = freshness_check.dataset_source.base_url
    tags_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    tags_df["dataset_last_modified"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    tags_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_tags_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table).values(tags_df.to_dict(orient="records")).returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    task_logger.info(f"Tags ingested: {len(ingested_df)}")
    return "success"


@task
def local_metadata_endpoint() -> str:
    return "Making the DAG flow cleaner"


@task_group
def update_local_metadata(conn_id: str, task_logger: Logger):
    freshness_check = get_freshness_check_results(task_logger=task_logger)
    update_dataset_metadata = ingest_dataset_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )
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
    local_metadata_endpoint_1 = local_metadata_endpoint()

    chain(
        freshness_check,
        [
            update_dataset_metadata,
            update_dataset_variables,
            update_dataset_geographies,
            update_dataset_groups,
            update_dataset_tags,
        ],
        local_metadata_endpoint_1,
    )


@task
def local_data_is_fresh(task_logger: Logger):
    return "hi"


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
    update_local_1 = update_local_metadata(conn_id=POSTGRES_CONN_ID, task_logger=task_logger)
    local_is_fresh_1 = local_data_is_fresh(task_logger=task_logger)

    chain(freshness_check_1, fresh_source_data_available_1, [update_local_1, local_is_fresh_1])


dev_update_cc_housing_units_by_census_tract()
