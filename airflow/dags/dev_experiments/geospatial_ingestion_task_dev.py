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
    produce_ingest_slices_for_gpd_read_file,
    produce_slice_indices_for_gpd_read_file,
)

task_logger = logging.getLogger("airflow.task")

# @task
# def create_temp_table_for_geojson_data(conn_id: str, task_logger: Logger) -> str:
#     task_logger.info(f"inside create_temp_table_for_geojson_data")
#     engine = get_pg_engine(conn_id=conn_id)
#     try:
#         full_temp_table_name = f"data_raw.temp_chicago_cta_bus_stops"
#         execute_structural_command(
#             query=f"""
#                 CREATE TABLE {full_temp_table_name} (
#                     data JSONB
#                 );
#                 """,
#             engine=engine,
#         )
#         task_logger.info(f"Created temp_table {full_temp_table_name}")
#         return full_temp_table_name
#     except Exception as e:
#         print(f"Failed to create temp table {full_temp_table_name}. Error: {e}, {type(e)}")


def preprocess_geojson_file(input_file_path: Path) -> Path:
    input_file_name = input_file_path.name
    assert input_file_path.is_file()
    file_dir = input_file_path.parent
    output_file_name = input_file_name.replace(".GeoJSON", "_jq_output.GeoJSON")
    output_file_path = file_dir.joinpath(output_file_name)
    jq_cmd = f"jq -c -r '.features[]' < {input_file_path} > {output_file_path}"
    subprocess.run(jq_cmd, shell=True)
    return output_file_path


@task
def ingest_geojson_data_test(conn_id: str, task_logger: Logger) -> str:
    try:
        full_temp_table_name = f"data_raw.temp_chicago_cta_bus_stops"
        file_name = "hvnx-qtky_2022-11-27T14:59:04.975359Z.GeoJSON"
        # file_path = Path("/home").joinpath(socrata_metadata.format_file_name())
        airflow_file_path = Path("/opt/airflow/data_raw").joinpath(file_name)
        task_logger.info(
            f"airflow_file_path: {airflow_file_path}, is_file: {airflow_file_path.is_file()}"
        )
        pg_file_path = Path("/home").joinpath(file_name)
        # That's not a file in this container, so it's executing in an airflow container (the scheduler)
        # Still, `COPY` is a server-side command, so I'll have to figure out a way to pipe the file to stdin
        # and use `jq` to process that stream
        # task_logger.info(f"pg_file_path: {pg_file_path}, is_file: {pg_file_path.is_file()}")

        processed_geojson_file_path = preprocess_geojson_file(input_file_path=airflow_file_path)

        # ingest_cmd = f"COPY {full_temp_table_name} FROM PROGRAM 'jq -c -r .[] < {file_path}';"
        # task_logger.info(f"ingest_cmd: {ingest_cmd}")

        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        # task_logger.info(f"dir(postgres_hook):  {dir(postgres_hook)}")

        # postgres_hook.copy_expert(ingest_cmd, file_path)
        conn = postgres_hook.get_conn()
        task_logger.info(f"dir(postgres_hook.get_conn):  {dir(conn)}")
        cur = conn.cursor()
        task_logger.info(f"dir(postgres_hook.get_conn().cursor):  {dir(cur)}")
        with open(processed_geojson_file_path, "r") as file:
            cur.copy_expert(f"COPY {full_temp_table_name} (data) FROM STDIN;", file)
        conn.commit()
        return pg_file_path
    except Exception as e:
        task_logger.info(f"Failed to ingest geojson file to temp table. Error: {e}, {type(e)}")


def get_lines_in_geojson_file(file_path) -> int:
    if file_path.name.lower().endswith(".geojson"):
        jq_cmd = f"jq -c -r '.features[]' < {file_path} | wc -l"
        n_rows = subprocess.check_output(jq_cmd, shell=True)
        return int(n_rows)
    else:
        raise Exception(
            "Not a geojson, or maybe it's formatted differently than this jq cmd can handle"
        )


# @task
# def get_geospatial_load_slices(file_path: Path, task_logger: Logger):
#     n_rows = get_lines_in_geojson_file(file_path=file_path)
#     slices = produce_ingest_slices_for_gpd_read_file(n_rows=n_rows, rows_per_batch=2000)

#     print(slices)
#     task_logger.info(f"slices spanning data: {slices}")
#     return slices


@task
def get_geospatial_load_indices(file_path: Path, task_logger: Logger):
    n_rows = get_lines_in_geojson_file(file_path=file_path)
    indexes = produce_slice_indices_for_gpd_read_file(n_rows=n_rows, rows_per_batch=2000)

    task_logger.info(f"slices spanning data: {indexes}")
    return indexes


# @task
# def ingest_geojson_data_in_slices(
#     ingest_slice, conn_id: str, task_logger: Logger
# ) -> str:
#     try:
#         full_temp_table_name = f"data_raw.temp_chicago_cta_bus_stops"
#         file_name = "hvnx-qtky_2022-11-27T14:59:04.975359Z.GeoJSON"
#         airflow_file_path = Path("/opt/airflow/data_raw").joinpath(file_name)
#         task_logger.info(f"airflow_file_path: {airflow_file_path}, is_file: {airflow_file_path.is_file()}")
#         task_logger.info(f"ingest_slice: {ingest_slice}")
#         # import geopandas as gpd

#         # gdf = gpd.read_file(airflow_file_path, rows=ingest_slice)

#         # n_rows = get_lines_in_geojson_file(file_path=airflow_file_path)
#         # task_logger.info(f"n_rows in file: {n_rows}")
#         # slices = get_geospatial_load_slices(file_path=airflow_file_path)
#         # task_logger.info(f"slices spanning data: {slices}")

#         # postgres_hook = PostgresHook(postgres_conn_id=conn_id)
#         # # task_logger.info(f"dir(postgres_hook):  {dir(postgres_hook)}")

#         # # postgres_hook.copy_expert(ingest_cmd, file_path)
#         # conn = postgres_hook.get_conn()
#         # task_logger.info(f"dir(postgres_hook.get_conn):  {dir(conn)}")
#         # cur = conn.cursor()
#         # task_logger.info(f"dir(postgres_hook.get_conn().cursor):  {dir(cur)}")
#         # with open(processed_geojson_file_path, "r") as file:
#         #     cur.copy_expert(
#         #         f"COPY {full_temp_table_name} (data) FROM STDIN;", file
#         #     )
#         # conn.commit()
#         # return pg_file_path
#     except Exception as e:
#         task_logger.info(f"Failed to ingest geojson file to temp table. Error: {e}, {type(e)}")


@task
def ingest_geojson_data_in_indexes(
    start_index: int, end_index: int, conn_id: str, task_logger: Logger
) -> str:
    try:
        table_name = "temp_chicago_cta_bus_stops"
        full_temp_table_name = f"data_raw.{table_name}"
        file_name = "hvnx-qtky_2022-11-27T14:59:04.975359Z.GeoJSON"
        airflow_file_path = Path("/opt/airflow/data_raw").joinpath(file_name)
        task_logger.info(
            f"airflow_file_path: {airflow_file_path}, is_file: {airflow_file_path.is_file()}"
        )
        task_logger.info(f"start_index: {start_index}, end_index: {end_index}")

        import geopandas as gpd

        gdf = gpd.read_file(airflow_file_path, rows=slice(start_index, end_index))
        task_logger.info(f"Shape of gdf: {gdf.shape}, columns: {gdf.columns}")
        engine = get_pg_engine(conn_id=conn_id)
        gdf.to_postgis(
            name=table_name,
            schema="data_raw",
            con=engine,
            if_exists="append",
        )
        task_logger.info(
            f"Successfully ingested records {start_index} to {end_index} using gpd.to_postgis()"
        )
    except Exception as e:
        task_logger.info(f"Failed to ingest geojson file to temp table. Error: {e}, {type(e)}")


# @task
# def print_file_path(file_path: str) -> None:
#     print(file_path)


@dag(schedule=None, start_date=dt.datetime(2022, 11, 1), catchup=False, tags=["dev_experiment"])
def an_ingest_dag():
    ingest_indexes_1 = get_geospatial_load_indices(
        file_path=Path("/opt/airflow/data_raw/hvnx-qtky_2022-11-27T14:59:04.975359Z.GeoJSON"),
        task_logger=task_logger,
    )
    test_2 = ingest_geojson_data_in_indexes.partial(
        conn_id="dwh_db_conn", task_logger=task_logger
    ).expand_kwargs(ingest_indexes_1)
    ingest_indexes_1 >> test_2


an_ingest_dag_instance = an_ingest_dag()
