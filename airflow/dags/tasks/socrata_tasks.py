import datetime as dt
from logging import Logger
import os
from pathlib import Path
import subprocess
from urllib.request import urlretrieve

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.cleanup import standardize_column_names
from cc_utils.db import (
    get_pg_engine,
    get_data_table_names_in_schema,
    execute_structural_command,
)
from cc_utils.file_factory import (
    make_dbt_data_raw_model_file,
    write_lines_to_file,
    format_dbt_stub_for_standardized_stage,
    format_dbt_stub_for_clean_stage,
)
from cc_utils.socrata import SocrataTable, SocrataTableMetadata
from cc_utils.transform import run_dbt_dataset_transformations
from cc_utils.utils import (
    get_local_data_raw_dir,
    get_lines_in_geojson_file,
    produce_slice_indices_for_gpd_read_file,
    log_as_info,
)
from cc_utils.validation import (
    run_checkpoint,
    check_if_checkpoint_exists,
    get_datasource,
    register_data_asset,
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
    log_as_info(task_logger, f"Ingesting data to database table 'data_raw.{table_name}'")
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
        log_as_info(task_logger, "Successfully ingested data using gpd.to_postgis()")
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
        log_as_info(task_logger, "Successfully ingested data using pd.to_sql()")


@task
def get_socrata_table_metadata(
    socrata_table: SocrataTable, task_logger: Logger
) -> SocrataTableMetadata:
    socrata_metadata = SocrataTableMetadata(socrata_table=socrata_table)
    log_as_info(
        task_logger,
        f"Retrieved metadata for socrata table {socrata_metadata.table_name} and table_id"
        + f" {socrata_metadata.table_id}.",
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
    log_as_info(
        task_logger,
        f"Extracted table freshness information. "
        + f"Fresh source data available: {socrata_metadata.data_freshness_check['updated_data_available']} "
        + f"Fresh source metadata available: {socrata_metadata.data_freshness_check['updated_metadata_available']}",
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
    log_as_info(
        task_logger,
        f"Ingested table freshness check results into metadata table.  "
        + f"Freshness check id: {socrata_metadata.freshness_check_id}",
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

    chain(metadata_1, metadata_2, metadata_3)
    return metadata_3


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def fresher_source_data_available(
    socrata_metadata: SocrataTableMetadata, conn_id: str, task_logger: Logger
) -> str:
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    table_does_not_exist = socrata_metadata.table_name not in tables_in_data_raw_schema
    update_availble = socrata_metadata.data_freshness_check["updated_data_available"]
    log_as_info(
        task_logger,
        f"In fresher_source_data_available; table_does_not_exist: {table_does_not_exist}",
    )
    log_as_info(task_logger, f" --- table_does_not_exist: {table_does_not_exist}")
    log_as_info(task_logger, f" --- update_availble:      {update_availble}")
    if table_does_not_exist or update_availble:
        return "update_socrata_table.download_fresh_data"
    else:
        return "update_socrata_table.update_result_of_check_in_metadata_table"


@task
def download_fresh_data(task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.check_table_metadata.ingest_table_freshness_check_metadata"
    )
    output_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    log_as_info(task_logger, f"Started downloading data at {dt.datetime.utcnow()} UTC")
    urlretrieve(url=socrata_metadata.data_download_url, filename=output_file_path)
    log_as_info(task_logger, f"Finished downloading data at {dt.datetime.utcnow()} UTC")
    return socrata_metadata


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def file_ext_branch_router(socrata_metadata: SocrataTableMetadata) -> str:
    dl_format = socrata_metadata.download_format
    if dl_format.lower() == "geojson":
        return "update_socrata_table.load_data_tg.load_geojson_data.drop_temp_table"
    elif dl_format.lower() == "csv":
        return "update_socrata_table.load_data_tg.load_csv_data.drop_temp_table"
    else:
        raise Exception(f"Download format '{dl_format}' not supported yet. CSV or GeoJSON for now")


@task
def drop_temp_table(
    route_str: str, conn_id: str, task_logger: Logger, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")

    log_as_info(task_logger, f"inside drop_temp_table, from {route_str}")
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
def create_temp_data_raw_table(conn_id: str, task_logger: Logger, **kwargs) -> None:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    table_name = f"temp_{socrata_metadata.table_name}"
    local_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    log_as_info(
        task_logger,
        f"Attempting to create table 'data_raw.{table_name}, "
        + f"dtypes inferred from file {local_file_path}.",
    )
    if local_file_path.is_file():
        engine = get_pg_engine(conn_id=conn_id)
        from pandas.io.sql import SQLTable

        if socrata_metadata.download_format == "csv":
            import pandas as pd

            df_subset = pd.read_csv(local_file_path, nrows=2500000)
        elif socrata_metadata.is_geospatial:
            import geopandas as gpd

            df_subset = gpd.read_file(local_file_path, rows=2500000)
        df_subset = standardize_column_names(df=df_subset)
        a_table = SQLTable(
            frame=df_subset,
            name=table_name,
            schema="data_raw",
            pandas_sql_engine=engine,
            index=False,
        )
        table_create_obj = a_table._create_table_setup()
        table_create_obj.create(bind=engine)
        log_as_info(task_logger, f"Successfully created table 'data_raw.{table_name}'")
        return socrata_metadata
    else:
        raise Exception(f"File not found in expected location.")


@task
def ingest_csv_data(
    socrata_metadata: SocrataTableMetadata, conn_id: str, task_logger: Logger
) -> SocrataTableMetadata:
    try:
        full_temp_table_name = f"data_raw.temp_{socrata_metadata.table_name}"
        file_path = get_local_file_path(socrata_metadata=socrata_metadata)
        log_as_info(task_logger, f"file_path: {file_path}, is_file: {file_path.is_file()}")

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
        log_as_info(
            task_logger, f"Successfully ingested csv data into {full_temp_table_name} via COPY."
        )
        return socrata_metadata
    except Exception as e:
        log_as_info(task_logger, f"Failed to ingest flat file to temp table. Error: {e}, {type(e)}")


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
    log_as_info(task_logger, f"slices spanning data: {indexes}")
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
        log_as_info(task_logger, f"file_path: {file_path}, is_file: {file_path.is_file()}")
        source_data_updated = socrata_metadata.data_freshness_check["source_data_last_updated"]
        time_of_check = socrata_metadata.data_freshness_check["time_of_check"]

        import geopandas as gpd
        from cc_utils.geo import impute_empty_geometries_into_missing_geometries

        gdf = gpd.read_file(file_path, rows=slice(start_index, end_index))
        gdf = impute_empty_geometries_into_missing_geometries(gdf=gdf, logger=task_logger)
        gdf["source_data_updated"] = source_data_updated
        gdf["ingestion_check_time"] = time_of_check
        log_as_info(task_logger, f"Shape of gdf: {gdf.shape}, columns: {gdf.columns}")

        gdf.to_postgis(
            name=temp_table_name,
            schema="data_raw",
            con=engine,
            if_exists="append",
            index=False,
        )
        log_as_info(
            task_logger,
            f"Successfully ingested records {start_index} to {end_index} using gpd.to_postgis()",
        )
    except Exception as e:
        task_logger.error(
            f"Failed to ingest geojson file to temp table. Error: {e}, {type(e)}",
            exc_info=True,
        )


@task_group
def load_geojson_data(route_str: str, conn_id: str, task_logger: Logger) -> SocrataTableMetadata:
    drop_temp_geojson_1 = drop_temp_table(
        route_str=route_str, conn_id=conn_id, task_logger=task_logger
    )
    slice_indices_1 = get_geospatial_load_indices(
        socrata_metadata=drop_temp_geojson_1,
        task_logger=task_logger,
        rows_per_batch=250000,
    )
    ingest_temp_geojson_1 = ingest_geojson_data.partial(
        conn_id=conn_id, task_logger=task_logger
    ).expand_kwargs(slice_indices_1)

    chain(drop_temp_geojson_1, slice_indices_1, ingest_temp_geojson_1)


@task_group
def load_data_tg(
    socrata_metadata: SocrataTableMetadata,
    conn_id: str,
    task_logger: Logger,
) -> SocrataTableMetadata:
    log_as_info(task_logger, f"Entered load_data_tg task_group")
    file_ext_route_1 = file_ext_branch_router(socrata_metadata=socrata_metadata)

    geojson_route_1 = load_geojson_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )
    csv_route_1 = load_csv_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )

    chain(
        file_ext_route_1,
        [geojson_route_1, csv_route_1],
    )


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def register_temp_table_asset(datasource_name: str, task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    datasource = get_datasource(datasource_name=datasource_name, task_logger=task_logger)
    register_data_asset(
        schema_name="data_raw",
        table_name=f"temp_{socrata_metadata.socrata_table.table_name}",
        datasource=datasource,
        task_logger=task_logger,
    )
    return True


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def socrata_table_checkpoint_exists(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    checkpoint_name = f"data_raw.temp_{socrata_metadata.socrata_table.table_name}"
    if check_if_checkpoint_exists(checkpoint_name=checkpoint_name, task_logger=task_logger):
        log_as_info(task_logger, f"GE checkpoint for {checkpoint_name} exists")
        return "update_socrata_table.raw_data_validation_tg.run_socrata_checkpoint"
    else:
        log_as_info(
            task_logger, f"GE checkpoint for {checkpoint_name} doesn't exist yet. Make it maybe?"
        )
        return "update_socrata_table.raw_data_validation_tg.validation_endpoint"


@task
def run_socrata_checkpoint(task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    checkpoint_name = f"data_raw.temp_{socrata_metadata.socrata_table.table_name}"
    checkpoint_run_results = run_checkpoint(
        checkpoint_name=checkpoint_name, task_logger=task_logger
    )
    log_as_info(
        task_logger,
        f"list_validation_results:      {checkpoint_run_results.list_validation_results()}",
    )
    log_as_info(task_logger, f"validation success:      {checkpoint_run_results.success}")
    log_as_info(task_logger, f"dir(checkpoint_run_results): {dir(checkpoint_run_results)}")
    return socrata_metadata


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def validation_endpoint(**kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    return socrata_metadata


@task_group
def raw_data_validation_tg(
    datasource_name: str,
    task_logger: Logger,
) -> SocrataTableMetadata:
    log_as_info(task_logger, f"Entered raw_data_validation_tg task_group")

    register_temp_table_1 = register_temp_table_asset(
        datasource_name=datasource_name, task_logger=task_logger
    )
    checkpoint_exists_1 = socrata_table_checkpoint_exists(task_logger=task_logger)
    checkpoint_1 = run_socrata_checkpoint(task_logger=task_logger)
    end_validation_1 = validation_endpoint()

    chain(
        register_temp_table_1,
        checkpoint_exists_1,
        [Label("Checkpoint Doesn't Exist"), checkpoint_1],
        end_validation_1,
    )


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def table_exists_in_data_raw(conn_id: str, task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.raw_data_validation_tg.validation_endpoint"
    )
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    log_as_info(task_logger, f"tables_in_data_raw_schema: {tables_in_data_raw_schema}")
    if socrata_metadata.table_name not in tables_in_data_raw_schema:
        log_as_info(task_logger, f"Table {socrata_metadata.table_name} not in data_raw; creating.")
        return "update_socrata_table.persist_new_raw_data_tg.create_table_in_data_raw"
    else:
        log_as_info(task_logger, f"Table {socrata_metadata.table_name} in data_raw; skipping.")
        return "update_socrata_table.persist_new_raw_data_tg.dbt_data_raw_model_exists"


@task
def create_table_in_data_raw(conn_id: str, task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.raw_data_validation_tg.validation_endpoint"
    )
    try:
        table_name = socrata_metadata.table_name
        log_as_info(task_logger, f"Creating table data_raw.{table_name}")
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


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_data_raw_model_exists(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.raw_data_validation_tg.validation_endpoint"
    )
    dbt_data_raw_model_dir = Path(f"/opt/airflow/dbt/models/data_raw")
    log_as_info(task_logger, f"dbt data_raw model dir ('{dbt_data_raw_model_dir}')")
    log_as_info(task_logger, f"Dir exists? {dbt_data_raw_model_dir.is_dir()}")
    table_model_path = dbt_data_raw_model_dir.joinpath(f"{socrata_metadata.table_name}.sql")
    if table_model_path.is_file():
        return "update_socrata_table.persist_new_raw_data_tg.update_data_raw_table"
    else:
        return "update_socrata_table.persist_new_raw_data_tg.make_dbt_data_raw_model"


@task(retries=1)
def make_dbt_data_raw_model(conn_id: str, task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.raw_data_validation_tg.validation_endpoint"
    )

    make_dbt_data_raw_model_file(
        table_name=socrata_metadata.table_name, engine=get_pg_engine(conn_id=conn_id)
    )
    log_as_info(task_logger, f"Leaving make_dbt_data_raw_model")
    return socrata_metadata


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_data_raw_table(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.raw_data_validation_tg.validation_endpoint"
    )
    result = run_dbt_dataset_transformations(
        dataset_name=socrata_metadata.table_name,
        task_logger=task_logger,
        schema="data_raw",
    )
    log_as_info(task_logger, f"dbt transform result: {result}")
    return "data_raw_updated"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def register_data_raw_table_asset(datasource_name: str, task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    datasource = get_datasource(datasource_name=datasource_name, task_logger=task_logger)
    register_data_asset(
        schema_name="data_raw",
        table_name=f"{socrata_metadata.socrata_table.table_name}",
        datasource=datasource,
        task_logger=task_logger,
    )
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_result_of_check_in_metadata_table(
    conn_id: str, task_logger: Logger, data_updated: bool, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(
        task_ids="update_socrata_table.check_table_metadata.ingest_table_freshness_check_metadata"
    )
    log_as_info(
        task_logger, f"Updating table_metadata record id #{socrata_metadata.freshness_check_id}."
    )
    log_as_info(task_logger, f"Data_pulled_this_check: {data_updated}.")
    socrata_metadata.update_current_freshness_check_in_db(
        engine=get_pg_engine(conn_id=conn_id),
        update_payload={"data_pulled_this_check": data_updated},
    )
    return socrata_metadata


@task_group
def persist_new_raw_data_tg(conn_id: str, datasource_name: str, task_logger: Logger) -> None:
    log_as_info(task_logger, f"Entered persist_new_raw_data_tg task_group")
    table_exists_1 = table_exists_in_data_raw(conn_id=conn_id, task_logger=task_logger)
    create_data_raw_table_1 = create_table_in_data_raw(conn_id=conn_id, task_logger=task_logger)
    dbt_data_raw_model_exists_1 = dbt_data_raw_model_exists(
        task_logger=task_logger, temp_table=False
    )
    make_dbt_data_raw_model_1 = make_dbt_data_raw_model(conn_id=conn_id, task_logger=task_logger)
    update_data_raw_table_1 = update_data_raw_table(task_logger=task_logger)
    register_data_raw_asset_1 = register_data_raw_table_asset(
        datasource_name=datasource_name, task_logger=task_logger
    )
    update_metadata_true_1 = update_result_of_check_in_metadata_table(
        conn_id=conn_id, task_logger=task_logger, data_updated=True
    )

    chain(
        table_exists_1,
        [Label("Table Exists"), create_data_raw_table_1],
        dbt_data_raw_model_exists_1,
        [Label("dbt data_raw Model Exists"), make_dbt_data_raw_model_1],
        update_data_raw_table_1,
        register_data_raw_asset_1,
        update_metadata_true_1,
    )


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_standardized_model_ready(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "standardized",
        f"{socrata_metadata.table_name}_standardized.sql",
    )
    host_file_path = str(file_path).replace(airflow_home, "/airflow")
    if file_path.is_file():
        with open(file_path, "r") as f:
            file_lines = f.readlines()
        for file_line in file_lines:
            if (
                "REPLACE_WITH_COMPOSITE_KEY_COLUMNS" in file_line
                or "REPLACE_WITH_BETTER_id" in file_line
            ):
                log_as_info(
                    task_logger,
                    f"Found unfinished stub for dbt _standardized model in {host_file_path}."
                    + " Please update that model before proceeding to feature engineering.",
                )
                return "update_socrata_table.transform_data_tg.highlight_unfinished_dbt_standardized_stub"
        log_as_info(
            task_logger, f"Found a _standardized stage dbt model that looks finished; Proceeding"
        )
        return "update_socrata_table.transform_data_tg.dbt_clean_model_ready"
    else:
        log_as_info(task_logger, f"No _standardized stage dbt model found.")
        log_as_info(task_logger, f"Creating a stub in loc: {host_file_path}")
        log_as_info(
            task_logger, f"Edit the stub before proceeding to generate _clean stage dbt models."
        )
        return "update_socrata_table.transform_data_tg.make_dbt_standardized_model"


@task
def highlight_unfinished_dbt_standardized_stub(task_logger: Logger) -> str:
    log_as_info(
        task_logger,
        f"Hey! Go finish the dbt _standardized model file indicated in the logs for the "
        + "dbt_standardized_model_ready task! It still contains at least one placeholder value"
        + "(REPLACE_WITH_COMPOSITE_KEY_COLUMNS or REPLACE_WITH_BETTER_id).",
    )
    return "Please and thank you!"


@task
def make_dbt_standardized_model(conn_id: str, task_logger: Logger, **kwargs) -> None:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    engine = get_pg_engine(conn_id=conn_id)
    std_file_lines = format_dbt_stub_for_standardized_stage(
        table_name=socrata_metadata.table_name, engine=engine
    )
    file_path = Path(
        f"/opt/airflow/dbt/models/standardized/{socrata_metadata.table_name}_standardized.sql"
    )
    write_lines_to_file(file_lines=std_file_lines, file_path=file_path)
    log_as_info(task_logger, f"file_lines for table {socrata_metadata.table_name}")
    for file_line in std_file_lines:
        log_as_info(task_logger, f"    {file_line}")

    log_as_info(task_logger, f"Leaving make_dbt_standardized_model")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_clean_model_ready(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "clean",
        f"{socrata_metadata.table_name}_clean.sql",
    )
    if file_path.is_file():
        log_as_info(task_logger, f"Found a _clean stage dbt model that looks finished; Ending")
        return "update_socrata_table.transform_data_tg.run_dbt_models__standardized_onward"
    else:
        return "update_socrata_table.transform_data_tg.dbt_make_clean_model"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_make_clean_model(task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    airflow_home = os.environ["AIRFLOW_HOME"]
    clean_file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "clean",
        f"{socrata_metadata.table_name}_clean.sql",
    )
    clean_file_lines = format_dbt_stub_for_clean_stage(table_name=socrata_metadata.table_name)
    log_as_info(task_logger, f"clean_file_lines: {clean_file_lines}")
    write_lines_to_file(file_lines=clean_file_lines, file_path=clean_file_path)
    return socrata_metadata


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def run_dbt_models__standardized_onward(task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    result = run_dbt_dataset_transformations(
        dataset_name=socrata_metadata.table_name,
        task_logger=task_logger,
        schema="standardized",
        run_downstream=True,
    )
    return socrata_metadata


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def endpoint(task_logger: Logger) -> None:
    log_as_info(task_logger, "Ending run")
    return "end"


@task_group
def transform_data_tg(conn_id: str, task_logger: Logger):
    std_model_ready_1 = dbt_standardized_model_ready(task_logger=task_logger)
    highlight_std_stub_1 = highlight_unfinished_dbt_standardized_stub(task_logger=task_logger)
    make_std_model_1 = make_dbt_standardized_model(conn_id=conn_id, task_logger=task_logger)
    clean_model_ready_1 = dbt_clean_model_ready(task_logger=task_logger)
    make_clean_model_1 = dbt_make_clean_model(task_logger=task_logger)
    run_dbt_models_1 = run_dbt_models__standardized_onward(task_logger=task_logger)
    endpoint_1 = endpoint(task_logger=task_logger)

    chain(
        std_model_ready_1,
        [
            Label("No dbt _standardized model found"),
            Label("dbt _standardized model needs review"),
            Label("dbt _standardized model looks good"),
        ],
        [make_std_model_1, highlight_std_stub_1, clean_model_ready_1],
    )
    chain([make_std_model_1, highlight_std_stub_1], endpoint_1)
    chain(
        clean_model_ready_1,
        [Label("dbt _clean model looks good!"), make_clean_model_1],
        run_dbt_models_1,
        endpoint_1,
    )


@task.short_circuit(ignore_downstream_trigger_rules=True)
def short_circuit_downstream():
    # airflow's short_circuit operator shorts downstream tasks by returning False
    # or proceeds if the short_circuit task returns True.
    return False


@task_group
def update_socrata_table(
    socrata_table: SocrataTable,
    conn_id: str,
    datasource_name: str,
    task_logger: Logger,
) -> SocrataTableMetadata:
    log_as_info(task_logger, f"Updating Socrata table {socrata_table.table_name}")

    metadata_1 = check_table_metadata(
        socrata_table=socrata_table, conn_id=conn_id, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        socrata_metadata=metadata_1, conn_id=conn_id, task_logger=task_logger
    )
    extract_data_1 = download_fresh_data(task_logger=task_logger)
    load_data_tg_1 = load_data_tg(
        socrata_metadata=extract_data_1,
        conn_id=conn_id,
        task_logger=task_logger,
    )
    raw_data_validation_tg_1 = raw_data_validation_tg(
        datasource_name=datasource_name, task_logger=task_logger
    )
    persist_new_raw_data_tg_1 = persist_new_raw_data_tg(
        conn_id=conn_id, datasource_name=datasource_name, task_logger=task_logger
    )
    transform_data_1 = transform_data_tg(conn_id=conn_id, task_logger=task_logger)
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
        raw_data_validation_tg_1,
        persist_new_raw_data_tg_1,
        transform_data_1,
    )
    chain(
        metadata_1,
        fresh_source_data_available_1,
        Label("Local data is fresh"),
        update_metadata_false_1,
        short_circuit_update_1,
    )
