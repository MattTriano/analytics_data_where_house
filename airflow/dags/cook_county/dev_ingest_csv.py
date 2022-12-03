import datetime as dt
import logging
from logging import Logger
from pathlib import Path
import subprocess
from typing import Tuple

from airflow.models.baseoperator import chain
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from geoalchemy2.types import Geometry, Geography

from cc_utils.db import get_pg_engine, execute_structural_command
from cc_utils.utils import (
    get_lines_in_file,
    produce_offset_and_nrows_counts_for_pd_read_csv,
)
from tasks.socrata_tasks import create_data_raw_table

task_logger = logging.getLogger("airflow.task")


@task
def create_data_raw_table(
    # socrata_metadata: SocrataTableMetadata,
    conn_id: str,
    task_logger: Logger,
    temp_table: bool = False,
) -> None:
    table_name = "cook_county_parcel_value_assessments"
    if temp_table:
        table_name = f"temp_{table_name}"
    else:
        table_name = table_name
    # local_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    file_name = "uzyt-m557_2022-11-26T23:58:09.460579Z.csv"
    local_file_path = Path("/opt/airflow/data_raw").joinpath(file_name)
    if local_file_path.is_file():
        engine = get_pg_engine(conn_id=conn_id)
        from pandas.io.sql import SQLTable

        # if socrata_metadata.download_format == "csv":
        if str(local_file_path).lower().endswith(".csv"):
            import pandas as pd

            df_subset = pd.read_csv(local_file_path, nrows=2000000)
            task_logger.info(f"df_subset: {df_subset.head(2)}")
            task_logger.info(f"df_subset.columns: {df_subset.columns}")
        else:
            raise Exception("ToDo: scrub this func; it's modified for csv ingest dev work.")
        # elif socrata_metadata.is_geospatial:
        #     import geopandas as gpd

        #     df_subset = gpd.read_file(local_file_path, rows=2000000)
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
def drop_temp_table(conn_id: str, task_logger: Logger) -> None:
    # , **kwargs
    # ti = kwargs["ti"]
    # socrata_metadata = ti.xcom_pull(task_ids="download_fresh_data")

    temp_table_name = "temp_cook_county_parcel_value_assessments"

    task_logger.info(f"inside drop_temp_table, from")
    engine = get_pg_engine(conn_id=conn_id)
    try:
        full_temp_table_name = f"data_raw.{temp_table_name}"
        execute_structural_command(
            query=f"DROP TABLE IF EXISTS {full_temp_table_name};",
            engine=engine,
        )
        # return socrata_metadata
    except Exception as e:
        print(f"Failed to create temp table {full_temp_table_name}. Error: {e}, {type(e)}")


# @task
# def ingest_csv_data(offset: int, nrows: int, conn_id: str, task_logger: Logger) -> str:
#     try:
#         temp_table_name = f"temp_cook_county_parcel_value_assessments"
#         file_name = "uzyt-m557_2022-11-26T23:58:09.460579Z.csv"
#         # file_path = Path("/home").joinpath(socrata_metadata.format_file_name())
#         file_path = Path("/opt/airflow/data_raw").joinpath(file_name)
#         engine = get_pg_engine(conn_id=conn_id)
#         task_logger.info(f"file_path: {file_path}, is_file: {file_path.is_file()}")

#         import pandas as pd

#         subset_df = pd.read_csv(file_path, nrows=1)
#         column_names = list(subset_df.columns)
#         df = pd.read_csv(file_path, nrows=nrows, skiprows=offset, names=column_names, header=None)
#         task_logger.info(f"Shape of df: {df.shape}, columns: {df.columns}")
#         df.to_sql(
#             name=temp_table_name,
#             schema="data_raw",
#             con=engine,
#             if_exists="append",
#             index=False,
#         )
#         task_logger.info(
#             f"Successfully ingested records {offset} to {offset + nrows} using pd.to_sql()"
#         )
#     except Exception as e:
#         task_logger.info(f"Failed to ingest flat file to temp table. Error: {e}, {type(e)}")


@task
def get_flat_file_load_indices(file_path: Path, task_logger: Logger, rows_per_batch: int = 3000000):
    # file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    assert file_path.name.lower().endswith(
        ".csv"
    ), "CSV is the only supported flat file type at the moment."
    # n_rows = get_lines_in_file(file_path=file_path)
    offsets_and_nrows = produce_offset_and_nrows_counts_for_pd_read_csv(
        file_path=file_path, rows_per_batch=rows_per_batch
    )
    task_logger.info(f"Offsets and chunk-sizes spanning data: {offsets_and_nrows}")
    return offsets_and_nrows


# @task_group
# def load_csv_data(conn_id: str, task_logger: Logger) -> None:
#     temp_table_name = f"temp_cook_county_parcel_value_assessments"
#     file_name = "uzyt-m557_2022-11-26T23:58:09.460579Z.csv"
#     # file_path = Path("/home").joinpath(socrata_metadata.format_file_name())
#     file_path = Path("/opt/airflow/data_raw").joinpath(file_name)

#     drop_temp_csv_1 = drop_temp_table(conn_id=conn_id, task_logger=task_logger)
#     offsets_and_nrows_1 = get_flat_file_load_indices(file_path=file_path, task_logger=task_logger)
#     ingest_temp_csv_1 = ingest_csv_data.partial(
#         conn_id=conn_id, task_logger=task_logger
#     ).expand_kwargs(offsets_and_nrows_1)

#     chain(drop_temp_csv_1, offsets_and_nrows_1, ingest_temp_csv_1)


@task
def ingest_csv_data(conn_id: str, task_logger: Logger) -> str:
    try:
        temp_table_name = f"temp_cook_county_parcel_value_assessments"
        file_name = "uzyt-m557_2022-11-26T23:58:09.460579Z.csv"
        # file_path = Path("/home").joinpath(socrata_metadata.format_file_name())
        file_path = Path("/opt/airflow/data_raw").joinpath(file_name)
        task_logger.info(f"file_path: {file_path}, is_file: {file_path.is_file()}")

        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        with conn:
            with conn.cursor() as cur:
                with open(file_path, "r") as f:
                    cur.copy_expert(
                        sql=f"""
                            COPY data_raw.{temp_table_name}
                            FROM STDIN
                            WITH (FORMAT CSV, HEADER, DELIMITER ',');
                        """,
                        file=f,
                    )

                    # cur.execute(
                    #     f"""COPY data_raw.{temp_table_name}
                    #         FROM '{file_path}'
                    #         WITH (FORMAT CSV, HEADER, DELIMITER ',')"""
                    # )
                # cur.copy_from(f, f"data_raw.{temp_table_name}", sep=",")
        conn.close()
        task_logger.info(f"Successfully ingested csv data into {temp_table_name} via COPY.")
    except Exception as e:
        task_logger.info(f"Failed to ingest flat file to temp table. Error: {e}, {type(e)}")


@task_group
def load_csv_data(conn_id: str, task_logger: Logger) -> None:
    """Try 2, this time with COPY"""
    temp_table_name = f"temp_cook_county_parcel_value_assessments"
    file_name = "uzyt-m557_2022-11-26T23:58:09.460579Z.csv"
    # file_path = Path("/home").joinpath(socrata_metadata.format_file_name())
    file_path = Path("/opt/airflow/data_raw").joinpath(file_name)

    drop_temp_csv_1 = drop_temp_table(conn_id=conn_id, task_logger=task_logger)
    create_temp_csv_1 = create_data_raw_table(
        conn_id=conn_id, task_logger=task_logger, temp_table=True
    )
    ingest_temp_csv_1 = ingest_csv_data(conn_id=conn_id, task_logger=task_logger)

    chain(drop_temp_csv_1, create_temp_csv_1, ingest_temp_csv_1)


@dag(schedule=None, start_date=dt.datetime(2022, 11, 1), catchup=False, tags=["metadata"])
def a_csv_ingest_dag():
    POSTGRES_CONN_ID = "dwh_db_conn"

    load_csv_1 = load_csv_data(conn_id=POSTGRES_CONN_ID, task_logger=task_logger)
    load_csv_1


a_csv_ingest_dag()
