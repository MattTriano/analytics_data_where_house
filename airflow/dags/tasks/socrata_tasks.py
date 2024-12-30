import datetime as dt
from logging import Logger
from pathlib import Path
from urllib.request import urlretrieve

from cc_utils.cleanup import standardize_column_names
from cc_utils.db import (
    execute_structural_command,
    get_data_table_names_in_schema,
    get_pg_engine,
)
from cc_utils.file_factory import make_dbt_data_raw_model_file
from cc_utils.socrata import SocrataTable, SocrataTableMetadata
from cc_utils.transform import execute_dbt_cmd, format_dbt_run_cmd
from cc_utils.utils import (
    get_lines_in_geojson_file,
    get_local_data_raw_dir,
    get_task_group_id_prefix,
    log_as_info,
    produce_slice_indices_for_gpd_read_file,
)
from cc_utils.validation import (
    check_if_checkpoint_exists,
    get_datasource,
    register_data_asset,
    run_checkpoint,
)

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from tasks.dbt_tasks import transform_data_tg


def get_local_file_path(socrata_metadata: SocrataTableMetadata) -> Path:
    output_dir = get_local_data_raw_dir()
    local_file_path = output_dir.joinpath(socrata_metadata.format_file_name())
    return local_file_path


@task
def get_socrata_table_metadata(socrata_table: SocrataTable, task_logger: Logger) -> bool:
    context = get_current_context()
    socrata_metadata = SocrataTableMetadata(socrata_table=socrata_table)
    log_as_info(
        task_logger,
        f"Retrieved metadata for socrata table {socrata_metadata.table_name} and table_id"
        + f" {socrata_metadata.table_id}.",
    )
    log_as_info(task_logger, f" --- socrata_metadata:      {socrata_metadata.metadata}")
    context["ti"].xcom_push(key="socrata_metadata_key", value=socrata_metadata)
    return True


@task
def extract_table_freshness_info(
    conn_id: str,
    task_logger: Logger,
) -> bool:
    engine = get_pg_engine(conn_id=conn_id)
    context = get_current_context()
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
    socrata_metadata.check_warehouse_data_freshness(engine=engine)
    log_as_info(
        task_logger,
        "Extracted table freshness information. Fresh source data available: "
        f"{socrata_metadata.data_freshness_check['updated_data_available']} "
        "Fresh source metadata available: "
        f"{socrata_metadata.data_freshness_check['updated_metadata_available']}",
    )
    context["ti"].xcom_push(key="socrata_metadata_key", value=socrata_metadata)
    return True


@task
def ingest_table_freshness_check_metadata(
    conn_id: str,
    task_logger: Logger,
) -> bool:
    engine = get_pg_engine(conn_id=conn_id)
    context = get_current_context()
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
    socrata_metadata.insert_current_freshness_check_to_db(engine=engine)
    log_as_info(
        task_logger,
        "Ingested table freshness check results into metadata table.  "
        + f"Freshness check id: {socrata_metadata.freshness_check_id}",
    )
    context["ti"].xcom_push(key="socrata_metadata_key", value=socrata_metadata)
    return True


@task_group
def check_table_metadata(socrata_table: SocrataTable, conn_id: str, task_logger: Logger) -> None:
    metadata_1 = get_socrata_table_metadata(socrata_table=socrata_table, task_logger=task_logger)
    metadata_2 = extract_table_freshness_info(conn_id=conn_id, task_logger=task_logger)
    metadata_3 = ingest_table_freshness_check_metadata(conn_id=conn_id, task_logger=task_logger)

    chain(metadata_1, metadata_2, metadata_3)


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def fresher_source_data_available(conn_id: str, task_logger: Logger) -> str:
    context = get_current_context()
    task_group_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
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
    log_as_info(task_logger, f" --- task_group_id_prefix: {task_group_id_prefix}")
    if table_does_not_exist or update_availble:
        return f"{task_group_id_prefix}download_fresh_data"
    else:
        return f"{task_group_id_prefix}update_result_of_check_in_metadata_table"


@task
def download_fresh_data(task_logger: Logger) -> bool:
    context = get_current_context()
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
    output_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
    log_as_info(task_logger, f"Started downloading data at {dt.datetime.now(dt.UTC)} UTC")
    urlretrieve(url=socrata_metadata.data_download_url, filename=output_file_path)
    log_as_info(task_logger, f"Finished downloading data at {dt.datetime.now(dt.UTC)} UTC")
    return True


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def file_ext_branch_router() -> str:
    context = get_current_context()
    task_group_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
    dl_format = socrata_metadata.download_format
    if dl_format.lower() == "geojson":
        return f"{task_group_id_prefix}load_geojson_data.drop_temp_table"
    elif dl_format.lower() == "csv":
        return f"{task_group_id_prefix}load_csv_data.drop_temp_table"
    else:
        raise Exception(f"Download format '{dl_format}' not supported yet. CSV or GeoJSON for now")


@task
def drop_temp_table(route_str: str, conn_id: str, task_logger: Logger) -> bool:
    context = get_current_context()
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")

    log_as_info(task_logger, f"inside drop_temp_table, from {route_str}")
    engine = get_pg_engine(conn_id=conn_id)
    try:
        full_temp_table_name = f"data_raw.temp_{socrata_metadata.table_name}"
        execute_structural_command(
            query=f"DROP TABLE IF EXISTS {full_temp_table_name} CASCADE;",
            engine=engine,
        )
    except Exception as e:
        task_logger.error(
            f"Failed to drop temp table {full_temp_table_name}. Error: {e}, {type(e)}"
        )
        raise
    return True


@task
def create_temp_data_raw_table(conn_id: str, task_logger: Logger) -> bool:
    context = get_current_context()
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
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
    else:
        raise FileNotFoundError("File not found in expected location.")
    return True


@task
def ingest_csv_data(conn_id: str, task_logger: Logger) -> bool:
    try:
        context = get_current_context()
        socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
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
    except Exception as e:
        log_as_info(task_logger, f"Failed to ingest flat file to temp table. Error: {e}, {type(e)}")
    return True


@task_group
def load_csv_data(route_str: str, conn_id: str, task_logger: Logger) -> None:
    drop_temp_csv_1 = drop_temp_table(route_str=route_str, conn_id=conn_id, task_logger=task_logger)
    create_temp_csv_1 = create_temp_data_raw_table(conn_id=conn_id, task_logger=task_logger)
    ingest_temp_csv_1 = ingest_csv_data(conn_id=conn_id, task_logger=task_logger)

    chain(drop_temp_csv_1, create_temp_csv_1, ingest_temp_csv_1)


@task
def get_geospatial_load_indices(task_logger: Logger, rows_per_batch: int) -> list[dict[str, int]]:
    context = get_current_context()
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
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
    start_index: int, end_index: int, conn_id: str, task_logger: Logger
) -> bool:
    try:
        context = get_current_context()
        socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")

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
    return True


@task_group
def load_geojson_data(route_str: str, conn_id: str, task_logger: Logger) -> None:
    drop_temp_geojson_1 = drop_temp_table(
        route_str=route_str, conn_id=conn_id, task_logger=task_logger
    )
    slice_indices_1 = get_geospatial_load_indices(task_logger=task_logger, rows_per_batch=250000)
    ingest_temp_geojson_1 = ingest_geojson_data.partial(
        conn_id=conn_id, task_logger=task_logger
    ).expand_kwargs(slice_indices_1)

    chain(drop_temp_geojson_1, slice_indices_1, ingest_temp_geojson_1)


@task_group
def load_data_tg(conn_id: str, task_logger: Logger) -> None:
    log_as_info(task_logger, "Entered load_data_tg task_group")
    file_ext_route_1 = file_ext_branch_router()
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
def register_temp_table_asset(dataset_name: str, datasource_name: str, task_logger: Logger) -> bool:
    datasource = get_datasource(datasource_name=datasource_name, task_logger=task_logger)
    register_data_asset(
        schema_name="data_raw",
        table_name=f"temp_{dataset_name}",
        datasource=datasource,
        task_logger=task_logger,
    )
    return True


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def socrata_table_checkpoint_exists(dataset_name: str, task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    checkpoint_name = f"data_raw.temp_{dataset_name}"
    if check_if_checkpoint_exists(checkpoint_name=checkpoint_name, task_logger=task_logger):
        log_as_info(task_logger, f"GE checkpoint for {checkpoint_name} exists")
        return f"{tg_id_prefix}run_socrata_checkpoint"
    else:
        log_as_info(
            task_logger, f"GE checkpoint for {checkpoint_name} doesn't exist yet. Make it maybe?"
        )
        return f"{tg_id_prefix}validation_endpoint"


@task
def run_socrata_checkpoint(dataset_name: str, task_logger: Logger) -> bool:
    checkpoint_name = f"data_raw.temp_{dataset_name}"
    checkpoint_run_results = run_checkpoint(
        checkpoint_name=checkpoint_name, task_logger=task_logger
    )
    log_as_info(
        task_logger,
        f"list_validation_results:      {checkpoint_run_results.list_validation_results()}",
    )
    log_as_info(task_logger, f"validation success:      {checkpoint_run_results.success}")
    log_as_info(task_logger, f"dir(checkpoint_run_results): {dir(checkpoint_run_results)}")
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def validation_endpoint() -> bool:
    return True


@task_group
def raw_data_validation_tg(dataset_name: str, datasource_name: str, task_logger: Logger) -> None:
    log_as_info(task_logger, "Entered raw_data_validation_tg task_group")
    register_temp_table_1 = register_temp_table_asset(dataset_name, datasource_name, task_logger)
    checkpoint_exists_1 = socrata_table_checkpoint_exists(dataset_name, task_logger)
    checkpoint_1 = run_socrata_checkpoint(dataset_name, task_logger)
    end_validation_1 = validation_endpoint()

    chain(
        register_temp_table_1,
        checkpoint_exists_1,
        [Label("Checkpoint Doesn't Exist"), checkpoint_1],
        end_validation_1,
    )


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def table_exists_in_data_raw(dataset_name: str, conn_id: str, task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    log_as_info(task_logger, f"tables_in_data_raw_schema: {tables_in_data_raw_schema}")
    if dataset_name not in tables_in_data_raw_schema:
        log_as_info(task_logger, f"Table {dataset_name} not in data_raw; creating.")
        return f"{tg_id_prefix}create_table_in_data_raw"
    else:
        log_as_info(task_logger, f"Table {dataset_name} in data_raw; skipping.")
        return f"{tg_id_prefix}dbt_data_raw_model_exists"


@task
def create_table_in_data_raw(dataset_name: str, conn_id: str, task_logger: Logger) -> bool:
    try:
        log_as_info(task_logger, f"Creating table data_raw.{dataset_name}")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE data_raw.{dataset_name}
            (LIKE data_raw.temp_{dataset_name} INCLUDING ALL);"""
        )
        conn.commit()
    except Exception as e:
        task_logger.error(
            f"Failed to create data_raw table {dataset_name} from temp_{dataset_name}. "
            f"Error: {e}, {type(e)}",
        )
        raise
    return True


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_data_raw_model_exists(dataset_name: str, task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    dbt_data_raw_model_dir = Path("/opt/airflow/dbt/models/data_raw")
    log_as_info(task_logger, f"dbt data_raw model dir ('{dbt_data_raw_model_dir}')")
    log_as_info(task_logger, f"Dir exists? {dbt_data_raw_model_dir.is_dir()}")
    table_model_path = dbt_data_raw_model_dir.joinpath(f"{dataset_name}.sql")
    if table_model_path.is_file():
        return f"{tg_id_prefix}update_data_raw_table"
    else:
        return f"{tg_id_prefix}make_dbt_data_raw_model"


@task(retries=1)
def make_dbt_data_raw_model(dataset_name: str, conn_id: str, task_logger: Logger) -> bool:
    make_dbt_data_raw_model_file(dataset_name=dataset_name, engine=get_pg_engine(conn_id=conn_id))
    log_as_info(task_logger, "Leaving make_dbt_data_raw_model")
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_data_raw_table(dataset_name: str, task_logger: Logger) -> str:
    dbt_cmd = format_dbt_run_cmd(
        dataset_name=dataset_name,
        schema="data_raw",
        run_downstream=False,
    )
    result = execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)
    log_as_info(task_logger, f"dbt transform result: {result}")
    return "data_raw_updated"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def register_data_raw_table_asset(
    dataset_name: str, datasource_name: str, task_logger: Logger
) -> bool:
    datasource = get_datasource(datasource_name=datasource_name, task_logger=task_logger)
    register_data_asset(
        schema_name="data_raw",
        table_name=dataset_name,
        datasource=datasource,
        task_logger=task_logger,
    )
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_result_of_check_in_metadata_table(
    conn_id: str, task_logger: Logger, data_updated: bool
) -> bool:
    context = get_current_context()
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
    log_as_info(
        task_logger, f"Updating table_metadata record id #{socrata_metadata.freshness_check_id}."
    )
    log_as_info(task_logger, f"Data_pulled_this_check: {data_updated}.")
    socrata_metadata.update_current_freshness_check_in_db(
        engine=get_pg_engine(conn_id=conn_id),
        update_payload={"data_pulled_this_check": data_updated},
    )
    context["ti"].xcom_push(key="socrata_metadata_key", value=socrata_metadata)
    return True


@task_group
def persist_new_raw_data_tg(
    dataset_name: str, conn_id: str, datasource_name: str, task_logger: Logger
) -> None:
    log_as_info(task_logger, "Entered persist_new_raw_data_tg task_group")
    table_exists_1 = table_exists_in_data_raw(dataset_name, conn_id, task_logger)
    create_data_raw_table_1 = create_table_in_data_raw(dataset_name, conn_id, task_logger)
    dbt_data_raw_model_exists_1 = dbt_data_raw_model_exists(dataset_name, task_logger)
    make_dbt_data_raw_model_1 = make_dbt_data_raw_model(dataset_name, conn_id, task_logger)
    update_data_raw_table_1 = update_data_raw_table(dataset_name, task_logger)
    register_data_raw_asset_1 = register_data_raw_table_asset(
        dataset_name, datasource_name, task_logger
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


@task_group
def update_socrata_table(
    socrata_table: SocrataTable,
    conn_id: str,
    datasource_name: str,
    task_logger: Logger,
) -> None:
    log_as_info(task_logger, f"Updating Socrata table {socrata_table.table_name}")

    metadata_1 = check_table_metadata(
        socrata_table=socrata_table, conn_id=conn_id, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        conn_id=conn_id, task_logger=task_logger
    )
    extract_data_1 = download_fresh_data(task_logger=task_logger)
    load_data_tg_1 = load_data_tg(conn_id=conn_id, task_logger=task_logger)
    raw_data_validation_tg_1 = raw_data_validation_tg(
        socrata_table.table_name, datasource_name, task_logger
    )
    persist_new_raw_data_tg_1 = persist_new_raw_data_tg(
        dataset_name=socrata_table.table_name,
        conn_id=conn_id,
        datasource_name=datasource_name,
        task_logger=task_logger,
    )
    transform_data_1 = transform_data_tg(
        dataset_name=socrata_table.table_name, conn_id=conn_id, task_logger=task_logger
    )
    update_metadata_false_1 = update_result_of_check_in_metadata_table(
        conn_id=conn_id, task_logger=task_logger, data_updated=False
    )

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
        transform_data_1,
    )
