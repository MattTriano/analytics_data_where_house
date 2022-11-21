import datetime as dt
import logging
from pathlib import Path
from typing import Tuple
from urllib.request import urlretrieve

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label
import pandas as pd
import geopandas as gpd

from utils.db import get_pg_engine, get_data_table_names_in_schema
from utils.socrata import SocrataTable, SocrataTableMetadata

task_logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "dwh_db_conn"

chicago_cta_stations_table = SocrataTable(table_id="8pix-ypme", table_name="chicago_cta_stations")


def get_local_file_path(socrata_metadata: SocrataTableMetadata) -> Path:
    output_dir = Path("data_raw").resolve()
    output_dir.mkdir(exist_ok=True)
    local_file_path = output_dir.joinpath(socrata_metadata.format_file_name())
    return local_file_path


def ingest_into_table(
    socrata_metadata: SocrataTableMetadata, conn_id: str, temp_table: bool = False
) -> None:
    local_file_path = get_local_file_path(socrata_metadata=socrata_metadata)

    if temp_table:
        table_name = f"temp_{socrata_metadata.table_name}"
        if_exists = "replace"
    else:
        table_name = f"temp_{socrata_metadata.table_name}"
        if_exists = "fail"
    task_logger.info(f"Ingesting data to database table 'data_raw.{table_name}'")
    time_of_check = socrata_metadata.data_freshness_check["time_of_check"]
    engine = get_pg_engine(conn_id=conn_id)
    if socrata_metadata.is_geospatial:
        gdf = gpd.read_file(local_file_path)
        gdf["ingestion_check_time"] = time_of_check
        gdf.to_postgis(
            name=table_name,
            schema="data_raw",
            con=engine,
            if_exists=if_exists,
        )
        task_logger.info("Successfully ingested data using gpd.to_postgis()")
    else:
        df = pd.read_csv(local_file_path)
        df["ingestion_check_time"] = time_of_check
        df.to_sql(
            name=table_name,
            schema="data_raw",
            con=engine,
            if_exists=if_exists,
        )
        task_logger.info("Successfully ingested data using pd.to_sql()")


@task
def get_socrata_table_metadata(socrata_table: SocrataTable) -> SocrataTableMetadata:
    socrata_metadata = SocrataTableMetadata(socrata_table=socrata_table)
    task_logger.info(
        f"Retrieved metadata for socrata table {socrata_metadata.table_name} and table_id",
        f" {socrata_metadata.table_id}.",
    )
    return socrata_metadata


@task
def extract_table_freshness_info(
    socrata_metadata: SocrataTableMetadata, conn_id: str
) -> pd.DataFrame:
    engine = get_pg_engine(conn_id=conn_id)
    socrata_metadata.check_warehouse_data_freshness(engine=engine)
    task_logger.info(
        f"Extracted table freshness information.",
        f"Fresh source data available: {socrata_metadata.data_freshness_check['updated_data_available']}",
        f"Fresh source metadata available: {socrata_metadata.data_freshness_check['updated_metadata_available']}",
    )
    return socrata_metadata


@task
def ingest_table_freshness_check_metadata(
    socrata_metadata: SocrataTableMetadata, conn_id: str
) -> None:
    engine = get_pg_engine(conn_id=conn_id)
    socrata_metadata.insert_current_freshness_check_to_db(engine=engine)
    task_logger.info(
        f"Ingested table freshness check results into metadata table.  ",
        f"Freshness check id: {socrata_metadata.freshness_check_id}",
    )
    return socrata_metadata


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def fresher_source_data_available(socrata_metadata: SocrataTableMetadata, **kwargs) -> str:
    task_logger.info(f"In fresher_source_data_available, here's what kwargs looks like {kwargs}")
    if socrata_metadata.data_freshness_check["updated_data_available"]:
        task_logger.info(f"Fresh data available, entering extract-load branch")
        return "extract_load_task_group.download_fresh_data"
    else:
        return "end"


@task
def download_fresh_data(**kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="ingest_table_freshness_check_metadata")
    output_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    urlretrieve(url=socrata_metadata.get_data_download_url(), filename=output_file_path)
    return socrata_metadata


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def table_exists_in_warehouse(socrata_metadata: SocrataTableMetadata, conn_id: str) -> str:
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    if socrata_metadata.table_name not in tables_in_data_raw_schema:
        return "extract_load_task_group.ingest_into_new_table_in_data_raw"
    else:
        return "extract_load_task_group.ingest_into_temporary_table"


@task
def ingest_into_new_table_in_data_raw(conn_id: str, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="extract_load_task_group.download_fresh_data")
    ingest_into_table(socrata_metadata=socrata_metadata, conn_id=conn_id, temp_table=False)
    return socrata_metadata


@task
def ingest_into_temporary_table(conn_id: str, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="extract_load_task_group.download_fresh_data")
    ingest_into_table(socrata_metadata=socrata_metadata, conn_id=conn_id, temp_table=True)
    return socrata_metadata


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def update_socrata_data_table():
    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    @task_group
    def extract_load_task_group(conn_id: str) -> None:
        task_logger.info(f"In extract_load_task_group")
        metadata_4 = download_fresh_data()
        table_exists_1 = table_exists_in_warehouse(socrata_metadata=metadata_4, conn_id=conn_id)
        ingest_to_new_1 = ingest_into_new_table_in_data_raw(conn_id=conn_id)
        ingest_to_temp_1 = ingest_into_new_table_in_data_raw(conn_id=conn_id)

        metadata_4 >> table_exists_1 >> Label("Adding Table") >> ingest_to_new_1
        metadata_4 >> table_exists_1 >> Label("Updating Table") >> ingest_to_temp_1

    metadata_1 = get_socrata_table_metadata(socrata_table=chicago_cta_stations_table)
    metadata_2 = extract_table_freshness_info(metadata_1, POSTGRES_CONN_ID)
    metadata_3 = ingest_table_freshness_check_metadata(metadata_2, POSTGRES_CONN_ID)
    fresh_source_data_available_1 = fresher_source_data_available(socrata_metadata=metadata_3)
    extract_load_task_group_1 = extract_load_task_group(POSTGRES_CONN_ID)

    fresh_source_data_available_1 >> [extract_load_task_group_1, end_1]


check_table_freshness_dag = update_socrata_data_table()
