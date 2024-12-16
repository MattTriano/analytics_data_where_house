import datetime as dt
from logging import Logger
from pathlib import Path
import subprocess

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from sqlalchemy import insert, update

from cc_utils.census.tiger import (
    TIGERCatalog,
    TIGERGeographicEntityVintage,
    TIGERDataset,
    TIGERDatasetFreshnessCheck,
)
from cc_utils.cleanup import standardize_column_names
from cc_utils.db import (
    get_pg_engine,
    get_data_table_names_in_schema,
    get_reflected_db_table,
    execute_dml_orm_query,
    execute_result_returning_query,
    execute_result_returning_orm_query,
)
from cc_utils.file_factory import (
    make_dbt_data_raw_model_file,
)
from cc_utils.transform import format_dbt_run_cmd, execute_dbt_cmd
from cc_utils.utils import get_task_group_id_prefix, log_as_info
from cc_utils.validation import (
    run_checkpoint,
    check_if_checkpoint_exists,
    get_datasource,
    register_data_asset,
)


@task
def get_tiger_catalog(task_logger: Logger) -> TIGERCatalog:
    tiger_catalog = TIGERCatalog()
    log_as_info(task_logger, f"Available TIGER vintages:")
    for vintage_row in tiger_catalog.dataset_vintages.iterrows():
        log_as_info(
            task_logger,
            f"  - {vintage_row[1]['clean_name']} (last modified {vintage_row[1]['last_modified']})",
        )
    return tiger_catalog


@task
def get_entity_vintage_metadata(
    tiger_dataset: TIGERDataset, tiger_catalog: TIGERCatalog, task_logger: Logger
) -> bool:
    context = get_current_context()
    vintages = TIGERGeographicEntityVintage(
        entity_name=tiger_dataset.entity_name,
        year=tiger_dataset.vintage_year,
        catalog=tiger_catalog,
    )
    year = tiger_dataset.vintage_year
    entity_files = vintages.entity_files_metadata.copy()
    log_as_info(
        task_logger,
        f"Entity vintage metadata for vintage {year} for {tiger_dataset.entity_name} entities:"
        + f"\n  last_modified: {entity_files['last_modified'].values[0]}"
        + f"\n  Files: {entity_files['is_file'].sum()}",
    )
    entity_vintage = vintages.get_entity_file_metadata(geography=tiger_dataset.geography)
    log_as_info(task_logger, f"entities after filtering: {len(entity_vintage)}")
    context["ti"].xcom_push(key="entity_vintage_key", value=vintages)
    return True


@task
def record_source_freshness_check(
    tiger_dataset: TIGERDataset,
    conn_id: str,
    task_logger: Logger,
) -> pd.DataFrame:
    context = get_current_context()
    vintages = context["ti"].xcom_pull(key="entity_vintage_key")
    entity_vintage = vintages.get_entity_file_metadata(geography=tiger_dataset.geography)
    freshness_check_record = pd.DataFrame(
        {
            "dataset_name": [tiger_dataset.dataset_name],
            "source_data_last_modified": [entity_vintage["last_modified"].max()],
            "time_of_check": [dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")],
        }
    )
    log_as_info(task_logger, f"dataset name:          {freshness_check_record['dataset_name']}")
    log_as_info(
        task_logger, f"dataset last_modified: {freshness_check_record['source_data_last_modified']}"
    )
    log_as_info(task_logger, f"dataset time_of_check: {freshness_check_record['time_of_check']}")

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
    log_as_info(
        task_logger,
        f"Last_modified datetime of latest local dataset update: {source_last_modified}.",
    )
    return latest_dataset_check_df


@task
def organize_freshness_check_results(task_logger: Logger) -> bool:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    freshness_check = TIGERDatasetFreshnessCheck(
        source_freshness=context["ti"].xcom_pull(
            task_ids=f"{tg_id_prefix}record_source_freshness_check"
        ),
        local_freshness=context["ti"].xcom_pull(
            task_ids=f"{tg_id_prefix}get_latest_local_freshness_check"
        ),
    )
    log_as_info(task_logger, f"Source_freshness records: {len(freshness_check.source_freshness)}")
    log_as_info(task_logger, f"local_freshness records: {len(freshness_check.local_freshness)}")
    context["ti"].xcom_push(key="freshness_check_key", value=freshness_check)
    return True


@task_group
def check_freshness(tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger) -> None:
    tiger_catalog = get_tiger_catalog(task_logger=task_logger)
    local_dataset_freshness = get_latest_local_freshness_check(
        tiger_dataset=tiger_dataset, conn_id=conn_id, task_logger=task_logger
    )
    entity_vintages = get_entity_vintage_metadata(
        tiger_dataset=tiger_dataset, tiger_catalog=tiger_catalog, task_logger=task_logger
    )
    source_dataset_freshness = record_source_freshness_check(
        tiger_dataset=tiger_dataset,
        conn_id=conn_id,
        task_logger=task_logger,
    )
    freshness_check = organize_freshness_check_results(task_logger=task_logger)
    chain(local_dataset_freshness, freshness_check)
    chain(entity_vintages, source_dataset_freshness, freshness_check)


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def fresher_source_data_available(task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    freshness_check = context["ti"].xcom_pull(key="freshness_check_key")
    dataset_in_local_dwh = len(freshness_check.local_freshness) > 0

    log_as_info(task_logger, f"Dataset in local dwh: {dataset_in_local_dwh}")
    log_as_info(task_logger, f"freshness_check.local_freshness: {freshness_check.local_freshness}")
    if dataset_in_local_dwh:
        local_last_modified = freshness_check.local_freshness["source_data_last_modified"].max()
        log_as_info(task_logger, f"Local dataset last modified: {local_last_modified}")
        source_last_modified = freshness_check.source_freshness["source_data_last_modified"].max()
        log_as_info(task_logger, f"Source dataset last modified: {source_last_modified}")
        local_dataset_is_fresh = local_last_modified >= source_last_modified
        if local_dataset_is_fresh:
            return f"{tg_id_prefix}local_data_is_fresh"
    return f"{tg_id_prefix}request_and_ingest_fresh_data"


@task
def local_data_is_fresh(task_logger: Logger) -> bool:
    return True


@task
def request_and_ingest_fresh_data(
    tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger
) -> bool:
    context = get_current_context()
    vintages = context["ti"].xcom_pull(key="entity_vintage_key")
    freshness_check = context["ti"].xcom_pull(key="freshness_check_key")
    source_freshness = freshness_check.source_freshness
    log_as_info(
        task_logger, f"source_freshness:    {source_freshness} (type: {type(source_freshness)})"
    )
    engine = get_pg_engine(conn_id=conn_id)
    full_gdf = vintages.get_entity_data(geography=tiger_dataset.geography)
    log_as_info(task_logger, f"Rows in returned TIGER dataset:    {len(full_gdf)}")
    log_as_info(task_logger, f"Columns in returned TIGER dataset: {full_gdf.columns}")
    full_gdf["vintage_year"] = tiger_dataset.vintage_year
    full_gdf["source_data_updated"] = source_freshness["source_data_last_modified"]
    full_gdf["ingestion_check_time"] = source_freshness["time_of_check"]
    full_gdf = standardize_column_names(df=full_gdf)
    full_gdf.to_postgis(
        name=f"temp_{tiger_dataset.dataset_name}",
        schema="data_raw",
        con=engine,
        index=False,
        if_exists="replace",
        chunksize=100000,
    )
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def record_data_update(conn_id: str, task_logger: Logger) -> bool:
    context = get_current_context()
    freshness_check = context["ti"].xcom_pull(key="freshness_check_key")
    engine = get_pg_engine(conn_id=conn_id)

    dataset_id = freshness_check.source_freshness["id"].max()
    pre_update_record = execute_result_returning_query(
        engine=engine,
        query=f"""SELECT * FROM metadata.dataset_metadata WHERE id = {dataset_id}""",
    )
    log_as_info(task_logger, f"General metadata record pre-update: {pre_update_record}")
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="dataset_metadata", schema_name="metadata"
    )
    update_query = (
        update(metadata_table)
        .where(metadata_table.c.id == int(dataset_id))
        .values(local_data_updated=True)
    )
    execute_dml_orm_query(engine=engine, dml_stmt=update_query, logger=task_logger)
    log_as_info(task_logger, f"dataset_id: {dataset_id}")
    post_update_record = execute_result_returning_query(
        engine=engine,
        query=f"""SELECT * FROM metadata.dataset_metadata WHERE id = {dataset_id}""",
    )
    log_as_info(task_logger, f"General metadata record post-update: {post_update_record}")
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def register_temp_table_asset(
    datasource_name: str, tiger_dataset: TIGERDataset, task_logger: Logger
) -> str:
    datasource = get_datasource(datasource_name=datasource_name, task_logger=task_logger)
    register_data_asset(
        schema_name="data_raw",
        table_name=f"temp_{tiger_dataset.dataset_name}",
        datasource=datasource,
        task_logger=task_logger,
    )
    return True


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def table_checkpoint_exists(tiger_dataset: TIGERDataset, task_logger: Logger) -> str:
    checkpoint_name = f"data_raw.temp_{tiger_dataset.dataset_name}"
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    if check_if_checkpoint_exists(checkpoint_name=checkpoint_name, task_logger=task_logger):
        log_as_info(task_logger, f"GE checkpoint for {checkpoint_name} exists")
        return f"{tg_id_prefix}run_temp_table_checkpoint"
    else:
        log_as_info(
            task_logger, f"GE checkpoint for {checkpoint_name} doesn't exist yet. Make it maybe?"
        )
        return f"{tg_id_prefix}validation_endpoint"


@task
def run_temp_table_checkpoint(tiger_dataset: TIGERDataset, task_logger: Logger) -> str:
    checkpoint_name = f"data_raw.temp_{tiger_dataset.dataset_name}"
    checkpoint_run_results = run_checkpoint(
        checkpoint_name=checkpoint_name, task_logger=task_logger
    )
    log_as_info(
        task_logger,
        f"list_validation_results:      {checkpoint_run_results.list_validation_results()}",
    )
    log_as_info(task_logger, f"validation success:      {checkpoint_run_results.success}")
    log_as_info(task_logger, f"dir(checkpoint_run_results): {dir(checkpoint_run_results)}")
    return checkpoint_run_results


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def validation_endpoint() -> str:
    return "just_for_DAG_aesthetics"


@task_group
def raw_data_validation_tg(
    datasource_name: str,
    tiger_dataset: TIGERDataset,
    task_logger: Logger,
):
    log_as_info(task_logger, f"Entered raw_data_validation_tg task_group")
    register_asset_1 = register_temp_table_asset(
        datasource_name=datasource_name, tiger_dataset=tiger_dataset, task_logger=task_logger
    )
    checkpoint_exists_1 = table_checkpoint_exists(
        tiger_dataset=tiger_dataset, task_logger=task_logger
    )
    checkpoint_1 = run_temp_table_checkpoint(tiger_dataset=tiger_dataset, task_logger=task_logger)
    end_validation_1 = validation_endpoint()

    chain(
        register_asset_1,
        checkpoint_exists_1,
        [Label("Checkpoint Doesn't Exist"), checkpoint_1],
        end_validation_1,
    )


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def table_exists_in_data_raw(tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    log_as_info(task_logger, f"tables_in_data_raw_schema: {tables_in_data_raw_schema}")
    if tiger_dataset.dataset_name not in tables_in_data_raw_schema:
        log_as_info(task_logger, f"Table {tiger_dataset.dataset_name} not in data_raw; creating.")
        return f"{tg_id_prefix}create_table_in_data_raw"
    else:
        log_as_info(task_logger, f"Table {tiger_dataset.dataset_name} in data_raw; skipping.")
        return f"{tg_id_prefix}dbt_data_raw_model_exists"


@task
def create_table_in_data_raw(tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger) -> str:
    try:
        table_name = tiger_dataset.dataset_name
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
    return "table_created"


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_data_raw_model_exists(tiger_dataset: TIGERDataset, task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    dbt_data_raw_model_dir = Path(f"/opt/airflow/dbt/models/data_raw")
    log_as_info(task_logger, f"dbt data_raw model dir ('{dbt_data_raw_model_dir}')")
    log_as_info(task_logger, f"Dir exists? {dbt_data_raw_model_dir.is_dir()}")
    table_model_path = dbt_data_raw_model_dir.joinpath(f"{tiger_dataset.dataset_name}.sql")
    if table_model_path.is_file():
        return f"{tg_id_prefix}update_data_raw_table"
    else:
        return f"{tg_id_prefix}make_dbt_data_raw_model"


@task(retries=1)
def make_dbt_data_raw_model(tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger) -> bool:
    make_dbt_data_raw_model_file(
        dataset_name=tiger_dataset.dataset_name, engine=get_pg_engine(conn_id=conn_id)
    )
    log_as_info(task_logger, f"Leaving make_dbt_data_raw_model")
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_data_raw_table(tiger_dataset: TIGERDataset, task_logger: Logger) -> str:
    dbt_cmd = format_dbt_run_cmd(
        dataset_name=tiger_dataset.dataset_name,
        schema="data_raw",
        run_downstream=False,
    )
    result = execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)
    log_as_info(task_logger, f"dbt transform result: {result}")
    return "data_raw_updated"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def register_data_raw_table_asset(
    datasource_name: str, tiger_dataset: TIGERDataset, task_logger: Logger
) -> bool:
    datasource = get_datasource(datasource_name=datasource_name, task_logger=task_logger)
    register_data_asset(
        schema_name="data_raw",
        table_name=f"{tiger_dataset.dataset_name}",
        datasource=datasource,
        task_logger=task_logger,
    )
    return True


@task_group
def persist_new_raw_data_tg(
    datasource_name: str, tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger
):
    table_exists_1 = table_exists_in_data_raw(
        tiger_dataset=tiger_dataset, conn_id=conn_id, task_logger=task_logger
    )
    create_data_raw_table_1 = create_table_in_data_raw(
        tiger_dataset=tiger_dataset, conn_id=conn_id, task_logger=task_logger
    )
    dbt_raw_exists_1 = dbt_data_raw_model_exists(
        tiger_dataset=tiger_dataset, task_logger=task_logger
    )
    make_dbt_raw_1 = make_dbt_data_raw_model(
        tiger_dataset=tiger_dataset, conn_id=conn_id, task_logger=task_logger
    )
    update_raw_1 = update_data_raw_table(tiger_dataset=tiger_dataset, task_logger=task_logger)
    register_asset_2 = register_data_raw_table_asset(
        datasource_name=datasource_name, tiger_dataset=tiger_dataset, task_logger=task_logger
    )
    record_update_1 = record_data_update(conn_id=conn_id, task_logger=task_logger)

    chain(
        table_exists_1,
        [Label("Table Exists"), create_data_raw_table_1],
        dbt_raw_exists_1,
        [Label("dbt data_raw Model Exists"), make_dbt_raw_1],
        update_raw_1,
        register_asset_2,
        record_update_1,
    )


@task_group
def update_tiger_table(
    tiger_dataset: TIGERDataset, datasource_name: str, conn_id: str, task_logger: Logger
):
    freshness_check_1 = check_freshness(
        tiger_dataset=tiger_dataset, conn_id=conn_id, task_logger=task_logger
    )
    update_available_1 = fresher_source_data_available(task_logger=task_logger)
    local_data_is_fresh_1 = local_data_is_fresh(task_logger=task_logger)
    update_data_1 = request_and_ingest_fresh_data(
        tiger_dataset=tiger_dataset, conn_id=conn_id, task_logger=task_logger
    )
    raw_validation_1 = raw_data_validation_tg(
        datasource_name=datasource_name, tiger_dataset=tiger_dataset, task_logger=task_logger
    )
    persist_raw_1 = persist_new_raw_data_tg(
        datasource_name=datasource_name,
        tiger_dataset=tiger_dataset,
        conn_id=conn_id,
        task_logger=task_logger,
    )

    chain(freshness_check_1, update_available_1, [update_data_1, local_data_is_fresh_1])
    chain(update_data_1, raw_validation_1, persist_raw_1)
