import datetime as dt
import logging
from logging import Logger
from pathlib import Path
from urllib.request import urlretrieve

from airflow.models.baseoperator import chain
from airflow.decorators import dag, task_group, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.socrata import SocrataTable, SocrataTableMetadata
from utils.db import execute_structural_command
from tasks.socrata_tasks import (
    get_local_file_path,
    get_socrata_table_metadata,
    extract_table_freshness_info,
    ingest_table_freshness_check_metadata,
    table_exists_in_warehouse,
    ingest_into_new_table_in_data_raw,
    ingest_into_temporary_table,
    update_table_metadata_in_db,
    check_table_metadata,
    create_data_raw_table,
)

from utils.db import get_pg_engine, get_data_table_names_in_schema
from utils.utils import get_lines_in_geojson_file, produce_slice_indices_for_gpd_read_file

task_logger = logging.getLogger("airflow.task")

SOCRATA_TABLE = SocrataTable(table_id="hvnx-qtky", table_name="chicago_cta_bus_stops")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def fresher_source_data_available(
    socrata_metadata: SocrataTableMetadata, conn_id: str, task_logger: Logger
) -> str:
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    table_does_not_exist = socrata_metadata.table_name not in tables_in_data_raw_schema
    update_availble = socrata_metadata.data_freshness_check["updated_data_available"]
    task_logger.info(
        f"In fresher_source_data_available; table_does_not_exist: {table_does_not_exist}"
    )
    task_logger.info(f" --- table_does_not_exist: {table_does_not_exist}")
    task_logger.info(f" --- update_availble:      {update_availble}")
    if table_does_not_exist or update_availble:
        return "download_fresh_data"
    else:
        return "end"


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def table_exists_in_data_raw(conn_id: str, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="download_fresh_data")
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    if socrata_metadata.table_name not in tables_in_data_raw_schema:
        return "load_data_tg.create_table_in_data_raw"
    else:
        return "load_data_tg.file_ext_branch_router"


@task
def download_fresh_data(task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="check_table_metadata.ingest_table_freshness_check_metadata"
    )
    output_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    task_logger.info(f"Started downloading data at {dt.datetime.utcnow()} UTC")
    urlretrieve(url=socrata_metadata.get_data_download_url(), filename=output_file_path)
    task_logger.info(f"Finished downloading data at {dt.datetime.utcnow()} UTC")
    return socrata_metadata


@task
def create_table_in_data_raw(conn_id: str, task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="download_fresh_data")
    create_data_raw_table(
        socrata_metadata=socrata_metadata,
        conn_id=conn_id,
        task_logger=task_logger,
        temp_table=False,
    )
    return socrata_metadata


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def file_ext_branch_router(socrata_metadata: SocrataTableMetadata) -> str:
    dl_format = socrata_metadata.download_format
    if dl_format.lower() == "geojson":
        return "load_data_tg.load_geojson_data.drop_temp_table"
    elif dl_format.lower() == "csv":
        return "load_data_tg.load_csv_data.drop_temp_table"
    else:
        raise Exception(f"Download format '{dl_format}' not supported yet. CSV or GeoJSON for now")


@task
def drop_temp_table(
    route_str: str, conn_id: str, task_logger: Logger, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="download_fresh_data")

    task_logger.info(f"inside drop_temp_table, from {route_str}")
    engine = get_pg_engine(conn_id=conn_id)
    try:
        full_temp_table_name = f"data_raw.temp_{socrata_metadata.table_name}"
        execute_structural_command(
            query=f"DROP TABLE IF EXISTS {full_temp_table_name};",
            engine=engine,
        )
        return socrata_metadata
    except Exception as e:
        print(f"Failed to create temp table {full_temp_table_name}. Error: {e}, {type(e)}")


@task
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
            task_logger.info(f"df_subset: {df_subset.head(2)}")
            task_logger.info(f"df_subset.columns: {df_subset.columns}")
        elif socrata_metadata.is_geospatial:
            import geopandas as gpd

            df_subset = gpd.read_file(local_file_path, rows=2000000)
        a_table = SQLTable(
            frame=df_subset,
            name=table_name,
            schema="data_raw",
            pandas_sql_engine=engine,
            index=False,
        )
        table_create_obj = a_table._create_table_setup()
        table_create_obj.create(bind=engine)
        task_logger.info(f"Successfully created table 'data_raw.{table_name}'")
    else:
        raise Exception(f"File not found in expected location.")


@task
def ingest_csv_data(
    socrata_metadata: SocrataTableMetadata, conn_id: str, task_logger: Logger
) -> SocrataTableMetadata:
    try:
        full_temp_table_name = f"data_raw.temp_{socrata_metadata.table_name}"
        file_path = get_local_file_path(socrata_metadata=socrata_metadata)
        task_logger.info(f"file_path: {file_path}, is_file: {file_path.is_file()}")

        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        with conn:
            with conn.cursor() as cur:
                with open(file_path, "r") as f:
                    cur.copy_expert(
                        sql=f"""
                            COPY {full_temp_table_name}
                            FROM STDIN
                            WITH (FORMAT CSV, HEADER, DELIMITER ',');
                        """,
                        file=f,
                    )
        conn.close()
        task_logger.info(f"Successfully ingested csv data into {full_temp_table_name} via COPY.")
        return socrata_metadata
    except Exception as e:
        task_logger.info(f"Failed to ingest flat file to temp table. Error: {e}, {type(e)}")


@task_group
def load_csv_data(route_str: str, conn_id: str, task_logger: Logger) -> None:
    drop_temp_csv_1 = drop_temp_table(route_str=route_str, conn_id=conn_id, task_logger=task_logger)
    create_temp_csv_1 = create_data_raw_table(
        socrata_metadata=drop_temp_csv_1, conn_id=conn_id, task_logger=task_logger, temp_table=True
    )
    ingest_temp_csv_1 = ingest_csv_data(
        socrata_metadata=create_temp_csv_1, conn_id=conn_id, task_logger=task_logger
    )

    chain(drop_temp_csv_1, create_temp_csv_1, ingest_temp_csv_1)


@task
def get_geospatial_load_indices(
    socrata_metadata: SocrataTableMetadata, task_logger: Logger, rows_per_batch: int = 500000
):
    file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    assert file_path.name.lower().endswith(
        ".geojson"
    ), "Geojson is the only supported geospatial type at the moment."
    n_rows = get_lines_in_geojson_file(file_path=file_path)
    indexes = produce_slice_indices_for_gpd_read_file(n_rows=n_rows, rows_per_batch=rows_per_batch)
    task_logger.info(f"slices spanning data: {indexes}")
    return indexes


@task
def ingest_geojson_data(
    start_index: int, end_index: int, conn_id: str, task_logger: Logger, **kwargs
) -> None:
    try:
        ti = kwargs["ti"]
        socrata_metadata = ti.xcom_pull(task_ids="load_data_tg.load_geojson_data.drop_temp_table")

        engine = get_pg_engine(conn_id=conn_id)
        temp_table_name = f"temp_{socrata_metadata.table_name}"
        file_path = get_local_file_path(socrata_metadata=socrata_metadata)
        task_logger.info(f"file_path: {file_path}, is_file: {file_path.is_file()}")
        source_data_updated = socrata_metadata.data_freshness_check["source_data_last_updated"]
        time_of_check = socrata_metadata.data_freshness_check["time_of_check"]

        import geopandas as gpd

        gdf = gpd.read_file(file_path, rows=slice(start_index, end_index))
        gdf["source_data_updated"] = source_data_updated
        gdf["ingestion_check_time"] = time_of_check
        task_logger.info(f"Shape of gdf: {gdf.shape}, columns: {gdf.columns}")

        gdf.to_postgis(
            name=temp_table_name,
            schema="data_raw",
            con=engine,
            if_exists="append",
        )
        task_logger.info(
            f"Successfully ingested records {start_index} to {end_index} using gpd.to_postgis()"
        )
    except Exception as e:
        task_logger.info(f"Failed to ingest geojson file to temp table. Error: {e}, {type(e)}")


@task_group
def load_geojson_data(route_str: str, conn_id: str, task_logger: Logger) -> SocrataTableMetadata:
    drop_temp_geojson_1 = drop_temp_table(
        route_str=route_str, conn_id=conn_id, task_logger=task_logger
    )
    slice_indices_1 = get_geospatial_load_indices(
        socrata_metadata=drop_temp_geojson_1, task_logger=task_logger, rows_per_batch=500000
    )
    ingest_temp_geojson_1 = ingest_geojson_data.partial(
        conn_id=conn_id, task_logger=task_logger
    ).expand_kwargs(slice_indices_1)

    chain(drop_temp_geojson_1, slice_indices_1, ingest_temp_geojson_1)


@task_group
def load_data_tg(
    socrata_metadata: SocrataTableMetadata, conn_id: str, task_logger: Logger
) -> SocrataTableMetadata:
    task_logger.info(f"Entered load_data_tg task_group")
    file_ext_route_1 = file_ext_branch_router(socrata_metadata=socrata_metadata)

    geojson_route_1 = load_geojson_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )
    csv_route_1 = load_csv_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )
    table_exists_1 = table_exists_in_data_raw(conn_id=conn_id)
    create_staging_table_1 = create_table_in_data_raw(conn_id=conn_id, task_logger=task_logger)

    data_load_end_1 = EmptyOperator(task_id="data_load_end", trigger_rule=TriggerRule.NONE_FAILED)

    chain(
        file_ext_route_1,
        [geojson_route_1, csv_route_1],
        table_exists_1,
        Label("Table Exists"),
        data_load_end_1,
    )
    chain(
        file_ext_route_1,
        [geojson_route_1, csv_route_1],
        table_exists_1,
        Label("Creating Table"),
        create_staging_table_1,
        data_load_end_1,
    )


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
    # default_args={"conn_id": "dwh_db_conn", "task_logger": task_logger},
)
def update_cta_bus_stops_table():
    POSTGRES_CONN_ID = "dwh_db_conn"

    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    metadata_1 = check_table_metadata(
        socrata_table=SOCRATA_TABLE, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        socrata_metadata=metadata_1, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    extract_data_1 = download_fresh_data(task_logger=task_logger)
    load_data_tg_1 = load_data_tg(
        socrata_metadata=extract_data_1, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )

    chain(metadata_1, fresh_source_data_available_1, extract_data_1, load_data_tg_1)
    chain(metadata_1, fresh_source_data_available_1, end_1)


chicago_cta_bus_stop_dag = update_cta_bus_stops_table()
