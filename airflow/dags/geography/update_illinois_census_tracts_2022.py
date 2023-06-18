from dataclasses import dataclass
import datetime as dt
from logging import Logger
import logging
from typing import Optional, Union, List

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from sqlalchemy import insert, update

from cc_utils.census.api import CensusGeography, CensusDatasetFreshnessCheck
from cc_utils.census.tiger import TIGERCatalog, TIGERGeographicEntityVintage
from cc_utils.db import (
    get_pg_engine,
    get_reflected_db_table,
    execute_dml_orm_query,
    execute_result_returning_query,
    execute_result_returning_orm_query,
)

from sources.geographies import ILLINOIS_CENSUS_TRACTS

task_logger = logging.getLogger("airflow.task")
POSTGRES_CONN_ID = "dwh_db_conn"
VINTAGE_YEAR = 2022
DATASET_NAME = "illinois_census_tracts"


@dataclass
class TIGERDatasetFreshnessCheck:
    def __init__(
        self,
        source_freshness: pd.DataFrame,
        local_freshness: pd.DataFrame,
    ):
        self.source_freshness = source_freshness
        self.local_freshness = local_freshness


@dataclass
class TIGERDataset:
    base_dataset_name: str
    vintage_year: Union[str, int, List[str], List[int]]
    entity_name: str
    geography: CensusGeography
    schedule: Optional[str] = None

    @property
    def dataset_name(self):
        if isinstance(self.vintage_year, list):
            year_str = f"{min(self.vintage_year)}_to_{max(self.vintage_year)}"
        else:
            year_str = str(self.vintage_year)
        return f"{self.base_dataset_name}_{year_str}"


TIGER_DATASET = TIGERDataset(
    base_dataset_name="illinois_census_tracts",
    vintage_year=2022,
    entity_name="TRACT",
    geography=ILLINOIS_CENSUS_TRACTS,
    schedule="5 4 3 * *",
)


@task
def get_tiger_catalog(task_logger: Logger) -> TIGERCatalog:
    tiger_catalog = TIGERCatalog()
    task_logger.info(f"Available TIGER vintages:")
    for vintage_row in tiger_catalog.dataset_vintages.iterrows():
        task_logger.info(
            f"  - {vintage_row[1]['clean_name']} (last modified {vintage_row[1]['last_modified']})"
        )
    return tiger_catalog


@task
def get_entity_vintage_metadata(
    tiger_dataset: TIGERDataset, tiger_catalog: TIGERCatalog, task_logger: Logger
) -> TIGERGeographicEntityVintage:
    vintages = TIGERGeographicEntityVintage(
        entity_name=tiger_dataset.entity_name,
        year=tiger_dataset.vintage_year,
        catalog=tiger_catalog,
    )
    year = tiger_dataset.vintage_year
    entity_files = vintages.entity_files_metadata.copy()
    task_logger.info(
        f"Entity vintage metadata for vintage {year} for {tiger_dataset.entity_name} entities:"
        + f"\n  last_modified: {entity_files['last_modified'].values[0]}"
        + f"\n  Files: {entity_files['is_file'].sum()}"
    )
    entity_vintage = vintages.get_entity_file_metadata(geography=tiger_dataset.geography)
    task_logger.info(f"entities after filtering: {len(entity_vintage)}")
    return vintages


@task
def record_source_freshness_check(
    tiger_dataset: TIGERDataset,
    vintages: TIGERGeographicEntityVintage,
    conn_id: str,
    task_logger: Logger,
) -> pd.DataFrame:
    entity_vintage = vintages.get_entity_file_metadata(geography=tiger_dataset.geography)
    freshness_check_record = pd.DataFrame(
        {
            "dataset_name": [tiger_dataset.dataset_name],
            "source_data_last_modified": [entity_vintage["last_modified"].max()],
            "time_of_check": [dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")],
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
    tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger
) -> pd.DataFrame:
    dataset_name = tiger_dataset.dataset_name
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
def organize_freshness_check_results(task_logger: Logger, **kwargs) -> TIGERDatasetFreshnessCheck:
    ti = kwargs["ti"]
    freshness_check = TIGERDatasetFreshnessCheck(
        source_freshness=ti.xcom_pull(task_ids="check_freshness.record_source_freshness_check"),
        local_freshness=ti.xcom_pull(task_ids="check_freshness.get_latest_local_freshness_check"),
    )
    return freshness_check


@task_group
def check_freshness(
    tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger
) -> TIGERDatasetFreshnessCheck:
    tiger_catalog = get_tiger_catalog(task_logger=task_logger)
    local_dataset_freshness = get_latest_local_freshness_check(
        tiger_dataset=tiger_dataset, conn_id=conn_id, task_logger=task_logger
    )
    entity_vintages = get_entity_vintage_metadata(
        tiger_dataset=tiger_dataset, tiger_catalog=tiger_catalog, task_logger=task_logger
    )
    source_dataset_freshness = record_source_freshness_check(
        tiger_dataset=tiger_dataset,
        vintages=entity_vintages,
        conn_id=conn_id,
        task_logger=task_logger,
    )
    freshness_check = organize_freshness_check_results(task_logger=task_logger)
    chain(local_dataset_freshness, freshness_check)
    chain(source_dataset_freshness, freshness_check)
    return freshness_check


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def fresher_source_data_available(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    freshness_check = ti.xcom_pull(task_ids="check_freshness.organize_freshness_check_results")
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
    return "request_and_ingest_fresh_data"


@task
def local_data_is_fresh(task_logger: Logger):
    return "hi"


@task
def request_and_ingest_fresh_data(
    tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger, **kwargs
):
    ti = kwargs["ti"]
    vintages = ti.xcom_pull(task_ids="check_freshness.get_entity_vintage_metadata")
    engine = get_pg_engine(conn_id=conn_id)
    full_gdf = vintages.get_entity_data(geography=tiger_dataset.geography)
    task_logger.info(f"Rows in returned TIGER dataset:    {len(full_gdf)}")
    task_logger.info(f"Columns in returned TIGER dataset: {full_gdf.columns}")
    full_gdf["vintage_year"] = tiger_dataset.vintage_year
    full_gdf.to_postgis(
        name=f"temp_{tiger_dataset.dataset_name}",
        schema="data_raw",
        con=engine,
        index=False,
        if_exists="replace",
        chunksize=100000,
    )
    return "ingested"


@task(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def record_data_update(conn_id: str, task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    freshness_check = ti.xcom_pull(task_ids="check_freshness.organize_freshness_check_results")
    engine = get_pg_engine(conn_id=conn_id)

    dataset_id = freshness_check.source_freshness["id"].max()
    pre_update_record = execute_result_returning_query(
        engine=engine,
        query=f"""SELECT * FROM metadata.dataset_metadata WHERE id = {dataset_id}""",
    )
    task_logger.info(f"General metadata record pre-update: {pre_update_record}")
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="dataset_metadata", schema_name="metadata"
    )
    update_query = (
        update(metadata_table)
        .where(metadata_table.c.id == int(dataset_id))
        .values(local_data_updated=True)
    )
    execute_dml_orm_query(engine=engine, dml_stmt=update_query, logger=task_logger)
    task_logger.info(f"dataset_id: {dataset_id}")
    post_update_record = execute_result_returning_query(
        engine=engine,
        query=f"""SELECT * FROM metadata.dataset_metadata WHERE id = {dataset_id}""",
    )
    task_logger.info(f"General metadata record post-update: {post_update_record}")
    return "success"


@dag(
    schedule=TIGER_DATASET.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["illinois", "census", "geospatial"],
)
def update_illinois_census_tracts():
    freshness_check_1 = check_freshness(
        tiger_dataset=TIGER_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    update_available_1 = fresher_source_data_available(
        freshness_check=freshness_check_1, task_logger=task_logger
    )
    local_data_is_fresh_1 = local_data_is_fresh(task_logger=task_logger)
    update_data_1 = request_and_ingest_fresh_data(
        tiger_dataset=TIGER_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )

    record_update_1 = record_data_update(conn_id=POSTGRES_CONN_ID, task_logger=task_logger)

    chain(freshness_check_1, update_available_1, [update_data_1, local_data_is_fresh_1])
    chain(update_data_1, record_update_1)


update_illinois_census_tracts()
