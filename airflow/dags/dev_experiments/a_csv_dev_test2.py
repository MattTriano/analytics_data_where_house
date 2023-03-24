import datetime as dt
import logging
from logging import Logger
from pathlib import Path
from urllib.request import urlretrieve

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import get_pg_engine, get_data_table_names_in_schema, execute_structural_command
from cc_utils.socrata import SocrataTable, SocrataTableMetadata
from tasks.socrata_tasks import (
    get_socrata_table_metadata,
    # download_fresh_data,
    fresher_source_data_available,
    check_table_metadata,
    load_data_tg,
)
from cc_utils.utils import (
    get_local_data_raw_dir,
    get_lines_in_geojson_file,
    produce_slice_indices_for_gpd_read_file,
)

task_logger = logging.getLogger("airflow.task")

SOCRATA_TABLE = SocrataTable(table_id="wvhk-k5uv", table_name="cook_county_parcel_sales")


def get_local_file_path(socrata_metadata: SocrataTableMetadata) -> Path:
    output_dir = get_local_data_raw_dir()
    local_file_path = output_dir.joinpath("wvhk-k5uv_2022-11-29T17:59:02.212736Z.csv")
    return local_file_path


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
        return socrata_metadata
    else:
        raise Exception(f"File not found in expected location.")


@task
def download_fresh_data(socrata_metadata: SocrataTableMetadata) -> SocrataTableMetadata:
    return socrata_metadata


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


@dag(schedule=None, start_date=dt.datetime(2022, 11, 1), catchup=False, tags=["metadata"])
def a_csv_ingest_2_dag():
    POSTGRES_CONN_ID = "dwh_db_conn"

    metadata_1 = get_socrata_table_metadata(socrata_table=SOCRATA_TABLE, task_logger=task_logger)
    dl_meta_1 = download_fresh_data(socrata_metadata=metadata_1)

    load_csv_1 = load_csv_data(
        route_str="placeholder", conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    chain(metadata_1, dl_meta_1, load_csv_1)


# a_csv_ingest_2_dag()
