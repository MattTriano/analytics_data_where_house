from logging import Logger
from pathlib import Path
from urllib.request import urlretrieve

from airflow.decorators import task, task_group
from airflow.utils.trigger_rule import TriggerRule

from utils.db import get_pg_engine, get_data_table_names_in_schema
from utils.socrata import SocrataTable, SocrataTableMetadata
from utils.utils import get_local_data_raw_dir


def get_local_file_path(socrata_metadata: SocrataTableMetadata) -> Path:
    output_dir = get_local_data_raw_dir()
    local_file_path = output_dir.joinpath(socrata_metadata.format_file_name())
    return local_file_path


def ingest_into_table(
    socrata_metadata: SocrataTableMetadata,
    conn_id: str,
    task_logger: Logger,
    temp_table: bool = False,
) -> None:
    local_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    if temp_table:
        table_name = f"temp_{socrata_metadata.table_name}"
        if_exists = "replace"
    else:
        table_name = f"{socrata_metadata.table_name}"
        if_exists = "fail"
    task_logger.info(f"Ingesting data to database table 'data_raw.{table_name}'")
    source_data_updated = socrata_metadata.data_freshness_check["source_data_last_updated"]
    time_of_check = socrata_metadata.data_freshness_check["time_of_check"]
    engine = get_pg_engine(conn_id=conn_id)
    if socrata_metadata.is_geospatial:
        import geopandas as gpd

        gdf = gpd.read_file(local_file_path)
        gdf["source_data_updated"] = source_data_updated
        gdf["ingestion_check_time"] = time_of_check
        gdf.to_postgis(
            name=table_name,
            schema="data_raw",
            con=engine,
            if_exists=if_exists,
            chunksize=100000,
        )
        task_logger.info("Successfully ingested data using gpd.to_postgis()")
    else:
        import pandas as pd

        df = pd.read_csv(local_file_path)
        df["source_data_updated"] = source_data_updated
        df["ingestion_check_time"] = time_of_check
        df.to_sql(
            name=table_name,
            schema="data_raw",
            con=engine,
            if_exists=if_exists,
            chunksize=100000,
        )
        task_logger.info("Successfully ingested data using pd.to_sql()")


def create_data_raw_table(
    socrata_metadata: SocrataTableMetadata,
    conn_id: str,
    task_logger: Logger,
    temp_table: bool = False,
) -> None:
    if temp_table:
        table_name = f"temp_{socrata_metadata.table_name}"
    else:
        table_name = socrata_metadata.table_name
    local_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    if local_file_path.is_file():
        engine = get_pg_engine(conn_id=conn_id)
        from pandas.io.sql import SQLTable

        if socrata_metadata.download_format == "csv":
            import pandas as pd

            df_subset = pd.read_csv(local_file_path, nrows=2000000)
        elif socrata_metadata.is_geospatial:
            import geopandas as gpd

            df_subset = gpd.read_file(local_file_path, rows=2000000)
        a_table = SQLTable(
            frame=df_subset, name=table_name, schema="data_raw", pandas_sql_engine=engine
        )
        table_create_obj = a_table._create_table_setup()
        table_create_obj.create(bind=engine)
        task_logger.info(f"Successfully created table 'data_raw.{table_name}'")
    else:
        raise Exception(f"File not found in expected location.")


@task
def get_socrata_table_metadata(
    socrata_table: SocrataTable, task_logger: Logger
) -> SocrataTableMetadata:
    socrata_metadata = SocrataTableMetadata(socrata_table=socrata_table)
    task_logger.info(
        f"Retrieved metadata for socrata table {socrata_metadata.table_name} and table_id",
        f" {socrata_metadata.table_id}.",
    )
    return socrata_metadata


@task
def extract_table_freshness_info(
    socrata_metadata: SocrataTableMetadata,
    conn_id: str,
    task_logger: Logger,
) -> SocrataTableMetadata:
    engine = get_pg_engine(conn_id=conn_id)
    socrata_metadata.check_warehouse_data_freshness(engine=engine)
    task_logger.info(
        f"Extracted table freshness information. "
        + f"Fresh source data available: {socrata_metadata.data_freshness_check['updated_data_available']} "
        + f"Fresh source metadata available: {socrata_metadata.data_freshness_check['updated_metadata_available']}"
    )
    return socrata_metadata


@task
def ingest_table_freshness_check_metadata(
    socrata_metadata: SocrataTableMetadata,
    conn_id: str,
    task_logger: Logger,
) -> None:
    engine = get_pg_engine(conn_id=conn_id)
    socrata_metadata.insert_current_freshness_check_to_db(engine=engine)
    task_logger.info(
        f"Ingested table freshness check results into metadata table.  "
        + f"Freshness check id: {socrata_metadata.freshness_check_id}",
    )
    return socrata_metadata


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def fresher_source_data_available(
    socrata_metadata: SocrataTableMetadata, task_logger: Logger, **kwargs
) -> str:
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


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def table_exists_in_warehouse(socrata_metadata: SocrataTableMetadata, conn_id: str) -> str:
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    if socrata_metadata.table_name not in tables_in_data_raw_schema:
        return "extract_load_task_group.ingest_into_new_table_in_data_raw"
    else:
        return "extract_load_task_group.ingest_into_temporary_table"


@task
def ingest_into_new_table_in_data_raw(
    conn_id: str, task_logger: Logger, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="extract_load_task_group.download_fresh_data")
    ingest_into_table(
        socrata_metadata=socrata_metadata,
        conn_id=conn_id,
        task_logger=task_logger,
        temp_table=False,
    )
    return socrata_metadata


@task
def ingest_into_temporary_table(
    conn_id: str, task_logger: Logger, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="extract_load_task_group.download_fresh_data")
    ingest_into_table(
        socrata_metadata=socrata_metadata, conn_id=conn_id, task_logger=task_logger, temp_table=True
    )
    return socrata_metadata


@task(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def update_table_metadata_in_db(
    conn_id: str, task_logger: Logger, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="extract_load_task_group.download_fresh_data")
    task_logger.info(f"Updating table_metadata record id #{socrata_metadata.freshness_check_id}.")
    socrata_metadata.update_current_freshness_check_in_db(
        engine=get_pg_engine(conn_id=conn_id), update_payload={"data_pulled_this_check": True}
    )
    return socrata_metadata


@task_group
def check_table_metadata(
    socrata_table: SocrataTable, conn_id: str, task_logger: Logger
) -> SocrataTableMetadata:
    metadata_1 = get_socrata_table_metadata(socrata_table=socrata_table, task_logger=task_logger)
    metadata_2 = extract_table_freshness_info(metadata_1, conn_id=conn_id, task_logger=task_logger)
    metadata_3 = ingest_table_freshness_check_metadata(
        metadata_2, conn_id=conn_id, task_logger=task_logger
    )

    metadata_1 >> metadata_2 >> metadata_3
    return metadata_3
