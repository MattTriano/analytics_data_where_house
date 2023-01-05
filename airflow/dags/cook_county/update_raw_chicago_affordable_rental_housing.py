import datetime as dt
import logging

from airflow.decorators import dag

# from tasks.socrata_tasks import update_socrata_table
from sources.tables import CHICAGO_AFFORDABLE_RENTAL_HOUSING as SOCRATA_TABLE

task_logger = logging.getLogger("airflow.task")


import datetime as dt
from logging import Logger
from pathlib import Path
from urllib.request import urlretrieve

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import (
    get_pg_engine,
    get_data_table_names_in_schema,
    execute_structural_command,
)
from cc_utils.file_factory import make_dbt_data_raw_table_staging_model
from cc_utils.socrata import SocrataTable, SocrataTableMetadata
from cc_utils.utils import (
    get_local_data_raw_dir,
    get_lines_in_geojson_file,
    produce_slice_indices_for_gpd_read_file,
)


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


@task
def get_socrata_table_metadata(
    socrata_table: SocrataTable, task_logger: Logger
) -> SocrataTableMetadata:
    socrata_metadata = SocrataTableMetadata(socrata_table=socrata_table)
    task_logger.info(
        f"Retrieved metadata for socrata table {socrata_metadata.table_name} and table_id"
        + f" {socrata_metadata.table_id}."
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
        return "update_socrata_table.extract_load_task_group.download_fresh_data"
    else:
        return "update_socrata_table.update_result_of_check_in_metadata_table"


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def table_exists_in_warehouse(socrata_metadata: SocrataTableMetadata, conn_id: str) -> str:
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    if socrata_metadata.table_name not in tables_in_data_raw_schema:
        return "update_socrata_table.extract_load_task_group.ingest_into_new_table_in_data_raw"
    else:
        return "update_socrata_table.extract_load_task_group.ingest_into_temporary_table"


@task
def ingest_into_new_table_in_data_raw(
    conn_id: str, task_logger: Logger, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.extract_load_task_group.download_fresh_data"
    )
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
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.extract_load_task_group.download_fresh_data"
    )
    ingest_into_table(
        socrata_metadata=socrata_metadata,
        conn_id=conn_id,
        task_logger=task_logger,
        temp_table=True,
    )
    return socrata_metadata


@task(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def update_table_metadata_in_db(
    conn_id: str, task_logger: Logger, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.extract_load_task_group.download_fresh_data"
    )
    task_logger.info(f"Updating table_metadata record id #{socrata_metadata.freshness_check_id}.")
    socrata_metadata.update_current_freshness_check_in_db(
        engine=get_pg_engine(conn_id=conn_id),
        update_payload={"data_pulled_this_check": True},
        logger=task_logger,
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


@task
def download_fresh_data(task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.check_table_metadata.ingest_table_freshness_check_metadata"
    )
    output_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    task_logger.info(f"Started downloading data at {dt.datetime.utcnow()} UTC")
    urlretrieve(url=socrata_metadata.data_download_url, filename=output_file_path)
    task_logger.info(f"Finished downloading data at {dt.datetime.utcnow()} UTC")
    return socrata_metadata


@task
def drop_temp_table(
    route_str: str, conn_id: str, task_logger: Logger, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")

    task_logger.info(f"inside drop_temp_table, from {route_str}")
    engine = get_pg_engine(conn_id=conn_id)
    try:
        full_temp_table_name = f"data_raw.temp_{socrata_metadata.table_name}"
        execute_structural_command(
            query=f"DROP TABLE IF EXISTS {full_temp_table_name} CASCADE;",
            engine=engine,
        )
        return socrata_metadata
    except Exception as e:
        print(f"Failed to drop temp table {full_temp_table_name}. Error: {e}, {type(e)}")


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
                conn.commit()
                cur.execute(
                    f"""
                ALTER TABLE {full_temp_table_name} ADD COLUMN source_data_updated TEXT;
                ALTER TABLE {full_temp_table_name} ADD COLUMN ingestion_check_time TEXT;
                """
                )
                update_time = socrata_metadata.data_freshness_check["source_data_last_updated"]
                cur.execute(
                    f"""
                UPDATE {full_temp_table_name} SET source_data_updated = '{update_time}';
                """
                )
                conn.commit()
                check_time = socrata_metadata.data_freshness_check["time_of_check"]
                cur.execute(
                    f"""
                UPDATE {full_temp_table_name} SET ingestion_check_time = '{check_time}';
                """
                )
        conn.close()
        task_logger.info(f"Successfully ingested csv data into {full_temp_table_name} via COPY.")
        return socrata_metadata
    except Exception as e:
        task_logger.info(f"Failed to ingest flat file to temp table. Error: {e}, {type(e)}")


@task_group
def load_csv_data(route_str: str, conn_id: str, task_logger: Logger) -> None:
    drop_temp_csv_1 = drop_temp_table(route_str=route_str, conn_id=conn_id, task_logger=task_logger)
    create_temp_csv_1 = create_temp_data_raw_table(conn_id=conn_id, task_logger=task_logger)
    ingest_temp_csv_1 = ingest_csv_data(
        socrata_metadata=create_temp_csv_1, conn_id=conn_id, task_logger=task_logger
    )

    chain(drop_temp_csv_1, create_temp_csv_1, ingest_temp_csv_1)


@task
def get_geospatial_load_indices(
    socrata_metadata: SocrataTableMetadata,
    task_logger: Logger,
    rows_per_batch: int = 500000,
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
        socrata_metadata = ti.xcom_pull(
            task_ids="update_socrata_table.load_data_tg.load_geojson_data.drop_temp_table"
        )

        engine = get_pg_engine(conn_id=conn_id)
        temp_table_name = f"temp_{socrata_metadata.table_name}"
        file_path = get_local_file_path(socrata_metadata=socrata_metadata)
        task_logger.info(f"file_path: {file_path}, is_file: {file_path.is_file()}")
        source_data_updated = socrata_metadata.data_freshness_check["source_data_last_updated"]
        time_of_check = socrata_metadata.data_freshness_check["time_of_check"]

        import geopandas as gpd
        from cc_utils.geo import impute_empty_geometries_into_missing_geometries

        gdf = gpd.read_file(file_path, rows=slice(start_index, end_index))
        gdf = impute_empty_geometries_into_missing_geometries(gdf=gdf, logger=task_logger)
        gdf["source_data_updated"] = source_data_updated
        gdf["ingestion_check_time"] = time_of_check
        task_logger.info(f"Shape of gdf: {gdf.shape}, columns: {gdf.columns}")

        gdf.to_postgis(
            name=temp_table_name,
            schema="data_raw",
            con=engine,
            if_exists="append",
            index=False,
        )
        task_logger.info(
            f"Successfully ingested records {start_index} to {end_index} using gpd.to_postgis()"
        )
    except Exception as e:
        task_logger.error(
            f"Failed to ingest geojson file to temp table. Error: {e}, {type(e)}", exc_info=True
        )


@task_group
def load_geojson_data(route_str: str, conn_id: str, task_logger: Logger) -> SocrataTableMetadata:
    drop_temp_geojson_1 = drop_temp_table(
        route_str=route_str, conn_id=conn_id, task_logger=task_logger
    )
    slice_indices_1 = get_geospatial_load_indices(
        socrata_metadata=drop_temp_geojson_1,
        task_logger=task_logger,
        rows_per_batch=500000,
    )
    ingest_temp_geojson_1 = ingest_geojson_data.partial(
        conn_id=conn_id, task_logger=task_logger
    ).expand_kwargs(slice_indices_1)

    chain(drop_temp_geojson_1, slice_indices_1, ingest_temp_geojson_1)


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def file_ext_branch_router(socrata_metadata: SocrataTableMetadata) -> str:
    dl_format = socrata_metadata.download_format
    if dl_format.lower() == "geojson":
        return "update_socrata_table.load_data_tg.load_geojson_data.drop_temp_table"
    elif dl_format.lower() == "csv":
        return "update_socrata_table.load_data_tg.load_csv_data.drop_temp_table"
    else:
        raise Exception(f"Download format '{dl_format}' not supported yet. CSV or GeoJSON for now")


@task
def create_temp_data_raw_table(conn_id: str, task_logger: Logger, **kwargs) -> None:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    table_name = f"temp_{socrata_metadata.table_name}"
    local_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    task_logger.info(
        f"Attempting to create table 'data_raw.{table_name}, "
        + f"dtypes inferred from file {local_file_path}."
    )
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
def create_table_in_data_raw(
    conn_id: str, task_logger: Logger, temp_table: bool, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    try:
        table_name = socrata_metadata.table_name
        task_logger.info(f"Creating table data_raw.{table_name}")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE data_raw.{table_name} (LIKE data_raw.temp_{table_name} INCLUDING ALL);"
        )
        conn.commit()
    except Exception as e:
        print(
            f"Failed to create data_raw table {table_name} from temp_{table_name}. Error: {e}, {type(e)}"
        )
    return socrata_metadata


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_result_of_check_in_metadata_table(
    conn_id: str, task_logger: Logger, data_updated: bool, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.check_table_metadata.ingest_table_freshness_check_metadata"
    )
    task_logger.info(f"Updating table_metadata record id #{socrata_metadata.freshness_check_id}.")
    task_logger.info(f"Data_pulled_this_check: {data_updated}.")
    socrata_metadata.update_current_freshness_check_in_db(
        engine=get_pg_engine(conn_id=conn_id),
        update_payload={"data_pulled_this_check": data_updated},
    )
    return socrata_metadata


from cc_utils.validation import run_checkpoint, check_if_checkpoint_exists


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def socrata_table_checkpoint_exists(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    checkpoint_name = f"data_raw.temp_{socrata_metadata.socrata_table.table_name}"
    if check_if_checkpoint_exists(checkpoint_name=checkpoint_name, task_logger=task_logger):
        return "update_socrata_table.load_data_tg.run_socrata_checkpoint"
    else:
        return "update_socrata_table.load_data_tg.table_exists_in_data_raw"


@task
def run_socrata_checkpoint(task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    checkpoint_name = f"data_raw.temp_{socrata_metadata.socrata_table.table_name}"
    checkpoint_run_results = run_checkpoint(
        checkpoint_name=checkpoint_name, task_logger=task_logger
    )
    task_logger.info(
        f"list_validation_results:      {checkpoint_run_results.list_validation_results()}"
    )
    task_logger.info(f"validation success:      {checkpoint_run_results.success}")
    task_logger.info(f"dir(checkpoint_run_results): {dir(checkpoint_run_results)}")
    return socrata_metadata


@task_group
def load_data_tg(
    socrata_metadata: SocrataTableMetadata,
    socrata_table: SocrataTable,
    conn_id: str,
    task_logger: Logger,
) -> SocrataTableMetadata:
    task_logger.info(f"Entered load_data_tg task_group")
    file_ext_route_1 = file_ext_branch_router(socrata_metadata=socrata_metadata)

    geojson_route_1 = load_geojson_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )
    csv_route_1 = load_csv_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )
    checkpoint_exists_1 = socrata_table_checkpoint_exists(task_logger=task_logger)
    checkpoint_1 = run_socrata_checkpoint(
        socrata_table=SOCRATA_TABLE,
        task_logger=task_logger,
        schema_name="data_raw",
    )
    table_exists_1 = table_exists_in_data_raw(conn_id=conn_id, task_logger=task_logger)
    create_staging_table_1 = create_table_in_data_raw(
        conn_id=conn_id, task_logger=task_logger, temp_table=False
    )
    dbt_staging_model_exists_1 = dbt_staging_model_exists(task_logger=task_logger, temp_table=False)
    make_dbt_staging_model_1 = make_dbt_staging_model(conn_id=conn_id, task_logger=task_logger)
    update_data_raw_table_1 = BashOperator(
        task_id="update_data_raw_table",
        bash_command=f"""cd /opt/airflow/dbt && \
            dbt --warn-error run --select re_dbt.staging.{socrata_table.table_name}""",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    update_metadata_true_1 = update_result_of_check_in_metadata_table(
        conn_id=conn_id, task_logger=task_logger, data_updated=True
    )

    chain(
        file_ext_route_1,
        [geojson_route_1, csv_route_1],
        checkpoint_exists_1,
        [Label("Checkpoint Doesn't Exist"), checkpoint_1],
        table_exists_1,
        [Label("Table Exists"), create_staging_table_1],
        dbt_staging_model_exists_1,
        [Label("dbt Staging Model Exists"), make_dbt_staging_model_1],
        update_data_raw_table_1,
        update_metadata_true_1,
    )


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
        return "update_socrata_table.download_fresh_data"
    else:
        return "update_socrata_table.update_result_of_check_in_metadata_table"


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def table_exists_in_data_raw(conn_id: str, task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    task_logger.info(f"tables_in_data_raw_schema: {tables_in_data_raw_schema}")
    if socrata_metadata.table_name not in tables_in_data_raw_schema:
        task_logger.info(f"Table {socrata_metadata.table_name} not in data_raw; creating.")
        return "update_socrata_table.load_data_tg.create_table_in_data_raw"
    else:
        task_logger.info(f"Table {socrata_metadata.table_name} in data_raw; skipping.")
        return "update_socrata_table.load_data_tg.dbt_staging_model_exists"


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_staging_model_exists(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    dbt_staging_model_dir = Path(f"/opt/airflow/dbt/models/staging")
    task_logger.info(f"dbt staging model dir ('{dbt_staging_model_dir}')")
    task_logger.info(f"Dir exists? {dbt_staging_model_dir.is_dir()}")
    table_model_path = dbt_staging_model_dir.joinpath(f"{socrata_metadata.table_name}.sql")
    if table_model_path.is_file():
        return "update_socrata_table.load_data_tg.update_data_raw_table"
    else:
        return "update_socrata_table.load_data_tg.make_dbt_staging_model"


@task(retries=1)
def make_dbt_staging_model(conn_id: str, task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")

    make_dbt_data_raw_table_staging_model(
        table_name=socrata_metadata.table_name, engine=get_pg_engine(conn_id=conn_id)
    )
    task_logger.info(f"Leaving make_dbt_staging_model")
    return socrata_metadata


@task.short_circuit(ignore_downstream_trigger_rules=True)
def short_circuit_downstream():
    # airflow's short_circuit operator shorts downstream tasks by returning False
    # or proceeds if the short_circuit task returns True.
    return False


@task_group
def update_socrata_table(
    socrata_table: SocrataTable,
    conn_id: str,
    task_logger: Logger,
) -> SocrataTableMetadata:
    task_logger.info(f"Updating Socrata table {socrata_table.table_name}")

    metadata_1 = check_table_metadata(
        socrata_table=socrata_table, conn_id=conn_id, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        socrata_metadata=metadata_1, conn_id=conn_id, task_logger=task_logger
    )
    extract_data_1 = download_fresh_data(task_logger=task_logger)
    load_data_tg_1 = load_data_tg(
        socrata_metadata=extract_data_1,
        socrata_table=socrata_table,
        conn_id=conn_id,
        task_logger=task_logger,
    )
    update_metadata_false_1 = update_result_of_check_in_metadata_table(
        conn_id=conn_id, task_logger=task_logger, data_updated=False
    )
    short_circuit_update_1 = short_circuit_downstream()

    chain(
        metadata_1,
        fresh_source_data_available_1,
        Label("Fresher data available"),
        extract_data_1,
        load_data_tg_1,
    )
    chain(
        metadata_1,
        fresh_source_data_available_1,
        Label("Local data is fresh"),
        update_metadata_false_1,
        short_circuit_update_1,
    )


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["public_housing", "chicago", "cook_county", "geospatial", "data_raw"],
)
def update_data_raw_chicago_affordable_rental_housing():
    update_1 = update_socrata_table(
        socrata_table=SOCRATA_TABLE,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    update_1


update_data_raw_chicago_affordable_rental_housing()
