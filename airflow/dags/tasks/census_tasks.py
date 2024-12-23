import os
from logging import Logger
from pathlib import Path

import pandas as pd
from cc_utils.census.api import (
    CensusAPIDatasetSource,
    CensusDatasetFreshnessCheck,
    CensusVariableGroupDataset,
)
from cc_utils.cleanup import standardize_column_names
from cc_utils.db import (
    execute_dml_orm_query,
    execute_result_returning_orm_query,
    execute_result_returning_query,
    get_data_table_names_in_schema,
    get_pg_engine,
    get_reflected_db_table,
)
from cc_utils.file_factory import (
    format_dbt_stub_for_clean_stage,
    format_dbt_stub_for_standardized_stage,
    make_dbt_data_raw_model_file,
    write_lines_to_file,
)
from cc_utils.transform import execute_dbt_cmd, format_dbt_run_cmd
from cc_utils.utils import get_task_group_id_prefix, log_as_info
from cc_utils.validation import (
    check_if_checkpoint_exists,
    get_datasource,
    register_data_asset,
    run_checkpoint,
)
from sqlalchemy import insert, update

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule


@task
def ingest_api_dataset_freshness_check(
    freshness_df: pd.DataFrame, conn_id: str, task_logger: Logger
) -> pd.DataFrame:
    source_freshness_df = freshness_df.loc[freshness_df["modified_src"].notnull()].copy()
    source_freshness_df = source_freshness_df.reset_index(drop=True)
    source_freshness_df = source_freshness_df.drop(columns="modified_local")
    source_freshness_df = source_freshness_df.rename(columns={"modified_src": "modified"})
    engine = get_pg_engine(conn_id=conn_id)
    api_dataset_metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(api_dataset_metadata_table)
        .values(source_freshness_df.to_dict(orient="records"))
        .returning(api_dataset_metadata_table)
    )
    ingested_api_datasets_df = execute_result_returning_orm_query(
        engine=engine, select_query=insert_statement
    )
    log_as_info(
        task_logger,
        f"Max census_api_metadata id value after ingestion: {ingested_api_datasets_df['id'].max()}",
    )
    return ingested_api_datasets_df


@task
def get_source_dataset_metadata(
    census_dataset: CensusVariableGroupDataset, task_logger: Logger
) -> CensusAPIDatasetSource:
    dataset_base_url = census_dataset.api_call_obj.dataset_base_url
    dataset_source = CensusAPIDatasetSource(dataset_base_url=dataset_base_url)
    log_as_info(task_logger, f"Census dataset {dataset_base_url} metadata details:")
    log_as_info(task_logger, f"  - Dataset variables:   {len(dataset_source.variables_df)}")
    log_as_info(task_logger, f"  - Dataset geographies: {len(dataset_source.geographies_df)}")
    log_as_info(task_logger, f"  - Dataset groups:      {len(dataset_source.groups_df)}")
    log_as_info(task_logger, f"  - Dataset tags:        {len(dataset_source.tags_df)}")
    context = get_current_context()
    context["ti"].xcom_push(key="census_dataset_key", value=census_dataset)
    return dataset_source


@task
def record_source_freshness_check(
    dataset_source: CensusAPIDatasetSource,
    census_dataset: CensusVariableGroupDataset,
    conn_id: str,
    task_logger: Logger,
) -> pd.DataFrame:
    dataset_metadata_df = dataset_source.metadata_catalog_df.loc[
        dataset_source.metadata_catalog_df["dataset_base_url"]
        == census_dataset.api_call_obj.dataset_base_url
    ].copy()
    freshness_check_record = pd.DataFrame(
        {
            "dataset_name": [census_dataset.dataset_name],
            "source_data_last_modified": [dataset_metadata_df["modified"].max()],
            "time_of_check": [dataset_source.time_of_check],
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
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> pd.DataFrame:
    dataset_name = census_dataset.dataset_name
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
def organize_freshness_check_results(task_logger: Logger) -> CensusDatasetFreshnessCheck:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    freshness_check = CensusDatasetFreshnessCheck(
        dataset_source=context["ti"].xcom_pull(
            task_ids=f"{tg_id_prefix}get_source_dataset_metadata"
        ),
        source_freshness=context["ti"].xcom_pull(
            task_ids=f"{tg_id_prefix}record_source_freshness_check"
        ),
        local_freshness=context["ti"].xcom_pull(
            task_ids=f"{tg_id_prefix}get_latest_local_freshness_check"
        ),
    )
    context["ti"].xcom_push(key="freshness_check_key", value=freshness_check)
    return freshness_check


@task_group
def check_freshness(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> CensusDatasetFreshnessCheck:
    local_dataset_freshness = get_latest_local_freshness_check(
        census_dataset=census_dataset, conn_id=conn_id, task_logger=task_logger
    )
    dataset_source = get_source_dataset_metadata(
        census_dataset=census_dataset, task_logger=task_logger
    )
    source_dataset_freshness = record_source_freshness_check(
        dataset_source=dataset_source,
        census_dataset=census_dataset,
        conn_id=conn_id,
        task_logger=task_logger,
    )
    freshness_check = organize_freshness_check_results(task_logger=task_logger)
    chain(local_dataset_freshness, dataset_source)
    chain(source_dataset_freshness, freshness_check)
    return freshness_check


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def fresher_source_data_available(
    freshness_check: CensusDatasetFreshnessCheck, task_logger: Logger
) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
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
    return f"{tg_id_prefix}update_local_metadata.get_freshness_check_results"


@task
def get_freshness_check_results(task_logger: Logger) -> CensusDatasetFreshnessCheck:
    context = get_current_context()
    freshness_check = context["ti"].xcom_pull(key="freshness_check_key")
    return freshness_check


@task
def ingest_dataset_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> bool:
    metadata_df = freshness_check.dataset_source.metadata_catalog_df.copy()
    metadata_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    log_as_info(task_logger, "Dataset metadata columns:")
    for col in metadata_df.columns:
        log_as_info(task_logger, f"  {col}")
    log_as_info(task_logger, f"{metadata_df.T}")
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table)
        .values(metadata_df.to_dict(orient="records"))
        .returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    log_as_info(task_logger, f"ingested_df shape: {ingested_df.shape}, {ingested_df.head(2)}")
    return True


@task
def ingest_dataset_variables_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    variables_df = freshness_check.dataset_source.variables_df.copy()
    variables_df["dataset_base_url"] = freshness_check.dataset_source.base_url
    variables_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    variables_df["dataset_last_modified"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    variables_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_variables_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table)
        .values(variables_df.to_dict(orient="records"))
        .returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    log_as_info(task_logger, f"Variables ingested: {len(ingested_df)}")
    return "success"


@task
def ingest_dataset_geographies_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    geographies_df = freshness_check.dataset_source.geographies_df.copy()
    geographies_df["dataset_base_url"] = freshness_check.dataset_source.base_url
    geographies_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    geographies_df["dataset_last_modified"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    geographies_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_geographies_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table)
        .values(geographies_df.to_dict(orient="records"))
        .returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    log_as_info(task_logger, f"Geographies ingested: {len(ingested_df)}")
    return "success"


@task
def ingest_dataset_groups_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    groups_df = freshness_check.dataset_source.groups_df.copy()
    groups_df["dataset_base_url"] = freshness_check.dataset_source.base_url
    groups_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    groups_df["dataset_last_modified"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    groups_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_groups_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table).values(groups_df.to_dict(orient="records")).returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    log_as_info(task_logger, f"Groups ingested: {len(ingested_df)}")
    return "success"


@task
def ingest_dataset_tags_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    tags_df = freshness_check.dataset_source.tags_df.copy()
    tags_df["dataset_base_url"] = freshness_check.dataset_source.base_url
    tags_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    tags_df["dataset_last_modified"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    tags_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    engine = get_pg_engine(conn_id=conn_id)
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_tags_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(metadata_table).values(tags_df.to_dict(orient="records")).returning(metadata_table)
    )
    ingested_df = execute_result_returning_orm_query(engine=engine, select_query=insert_statement)
    log_as_info(task_logger, f"Tags ingested: {len(ingested_df)}")
    return "success"


@task
def local_metadata_endpoint() -> str:
    return "Making the DAG flow cleaner"


@task_group
def update_local_metadata(conn_id: str, task_logger: Logger):
    freshness_check = get_freshness_check_results(task_logger=task_logger)
    update_dataset_metadata = ingest_dataset_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )
    update_dataset_variables = ingest_dataset_variables_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )
    update_dataset_geographies = ingest_dataset_geographies_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )
    update_dataset_groups = ingest_dataset_groups_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )
    update_dataset_tags = ingest_dataset_tags_metadata(
        freshness_check=freshness_check, conn_id=conn_id, task_logger=task_logger
    )
    local_metadata_endpoint_1 = local_metadata_endpoint()

    chain(
        freshness_check,
        [
            update_dataset_metadata,
            update_dataset_variables,
            update_dataset_geographies,
            update_dataset_groups,
            update_dataset_tags,
        ],
        local_metadata_endpoint_1,
    )


@task
def local_data_is_fresh(task_logger: Logger) -> bool:
    return True


@task
def request_and_ingest_dataset(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> str:
    context = get_current_context()
    freshness_check = context["ti"].xcom_pull(key="freshness_check_key")
    engine = get_pg_engine(conn_id=conn_id)

    dataset_df = census_dataset.api_call_obj.make_api_call()
    log_as_info(task_logger, f"Rows in returned dataset: {len(dataset_df)}")
    log_as_info(task_logger, f"Columns in returned dataset: {dataset_df.columns}")
    dataset_df["dataset_base_url"] = census_dataset.api_call_obj.dataset_base_url
    dataset_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    dataset_df["source_data_updated"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    dataset_df["ingestion_check_time"] = freshness_check.source_freshness["time_of_check"].max()
    log_as_info(task_logger, f"""dataset_id: {dataset_df["dataset_id"]}""")
    log_as_info(task_logger, f"""dataset_base_url: {dataset_df["dataset_base_url"]}""")
    log_as_info(task_logger, f"""source_data_updated: {dataset_df["source_data_updated"]}""")
    log_as_info(task_logger, f"""ingestion_check_time: {dataset_df["ingestion_check_time"]}""")
    dataset_df = standardize_column_names(df=dataset_df)
    result = dataset_df.to_sql(
        name=f"temp_{census_dataset.dataset_name}",
        schema="data_raw",
        con=engine,
        index=False,
        if_exists="replace",
        chunksize=100000,
    )
    log_as_info(task_logger, f"Ingestion result: {result}")
    if result is not None:
        return "ingested"
    else:
        raise Exception("No rows ingested")


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
    census_dataset: CensusVariableGroupDataset, datasource_name: str, task_logger: Logger
) -> bool:
    datasource = get_datasource(datasource_name=datasource_name, task_logger=task_logger)
    register_data_asset(
        schema_name="data_raw",
        table_name=f"temp_{census_dataset.dataset_name}",
        datasource=datasource,
        task_logger=task_logger,
    )
    return True


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def table_checkpoint_exists(census_dataset: CensusVariableGroupDataset, task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    checkpoint_name = f"data_raw.temp_{census_dataset.dataset_name}"
    if check_if_checkpoint_exists(checkpoint_name=checkpoint_name, task_logger=task_logger):
        log_as_info(task_logger, f"GE checkpoint for {checkpoint_name} exists")
        return f"{tg_id_prefix}run_temp_table_checkpoint"
    else:
        log_as_info(
            task_logger, f"GE checkpoint for {checkpoint_name} doesn't exist yet. Make it maybe?"
        )
        return f"{tg_id_prefix}validation_endpoint"


@task
def run_temp_table_checkpoint(
    census_dataset: CensusVariableGroupDataset, task_logger: Logger
) -> str:
    checkpoint_name = f"data_raw.temp_{census_dataset.dataset_name}"
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
    census_dataset: CensusVariableGroupDataset,
    datasource_name: str,
    task_logger: Logger,
):
    log_as_info(task_logger, "Entered raw_data_validation_tg task_group")
    register_asset_1 = register_temp_table_asset(
        census_dataset=census_dataset, datasource_name=datasource_name, task_logger=task_logger
    )
    checkpoint_exists_1 = table_checkpoint_exists(
        census_dataset=census_dataset, task_logger=task_logger
    )
    checkpoint_1 = run_temp_table_checkpoint(census_dataset=census_dataset, task_logger=task_logger)
    end_validation_1 = validation_endpoint()

    chain(
        register_asset_1,
        checkpoint_exists_1,
        [Label("Checkpoint Doesn't Exist"), checkpoint_1],
        end_validation_1,
    )


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def table_exists_in_data_raw(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> str:
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    log_as_info(task_logger, f"tables_in_data_raw_schema: {tables_in_data_raw_schema}")
    if census_dataset.dataset_name not in tables_in_data_raw_schema:
        log_as_info(task_logger, f"Table {census_dataset.dataset_name} not in data_raw; creating.")
        return "update_census_table.persist_new_raw_data_tg.create_table_in_data_raw"
    else:
        log_as_info(task_logger, f"Table {census_dataset.dataset_name} in data_raw; skipping.")
        return "update_census_table.persist_new_raw_data_tg.dbt_data_raw_model_exists"


@task
def create_table_in_data_raw(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> str:
    try:
        table_name = census_dataset.dataset_name
        log_as_info(task_logger, f"Creating table data_raw.{table_name}")
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE data_raw.{table_name} (LIKE data_raw.temp_{table_name} INCLUDING ALL);"
        )
        conn.commit()
    except Exception as e:
        task_logger.error(
            f"Failed to create data_raw table {table_name} from temp_{table_name}. "
            f"Error: {e}, {type(e)}"
        )
        raise
    return "table_created"


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_data_raw_model_exists(
    census_dataset: CensusVariableGroupDataset, task_logger: Logger
) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    dbt_data_raw_model_dir = Path("/opt/airflow/dbt/models/data_raw")
    log_as_info(task_logger, f"dbt data_raw model dir ('{dbt_data_raw_model_dir}')")
    log_as_info(task_logger, f"Dir exists? {dbt_data_raw_model_dir.is_dir()}")
    table_model_path = dbt_data_raw_model_dir.joinpath(f"{census_dataset.dataset_name}.sql")
    if table_model_path.is_file():
        return f"{tg_id_prefix}update_data_raw_table"
    else:
        return f"{tg_id_prefix}make_dbt_data_raw_model"


@task(retries=1)
def make_dbt_data_raw_model(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> bool:
    make_dbt_data_raw_model_file(
        dataset_name=census_dataset.dataset_name, engine=get_pg_engine(conn_id=conn_id)
    )
    log_as_info(task_logger, "dbt model file made, leaving make_dbt_data_raw_model")
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_data_raw_table(census_dataset: CensusVariableGroupDataset, task_logger: Logger) -> bool:
    dbt_cmd = format_dbt_run_cmd(
        dataset_name=census_dataset.dataset_name,
        schema="data_raw",
        run_downstream=False,
    )
    result = execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)
    log_as_info(task_logger, f"dbt transform result: {result}")
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def register_data_raw_table_asset(
    census_dataset: CensusVariableGroupDataset, datasource_name: str, task_logger: Logger
) -> str:
    datasource = get_datasource(datasource_name=datasource_name, task_logger=task_logger)
    register_data_asset(
        schema_name="data_raw",
        table_name=f"{census_dataset.dataset_name}",
        datasource=datasource,
        task_logger=task_logger,
    )
    return True


@task_group
def persist_new_raw_data_tg(
    census_dataset: CensusVariableGroupDataset,
    datasource_name: str,
    conn_id: str,
    task_logger: Logger,
):
    table_exists_1 = table_exists_in_data_raw(
        census_dataset=census_dataset, conn_id=conn_id, task_logger=task_logger
    )
    create_data_raw_table_1 = create_table_in_data_raw(
        census_dataset=census_dataset, conn_id=conn_id, task_logger=task_logger
    )
    dbt_raw_exists_1 = dbt_data_raw_model_exists(
        census_dataset=census_dataset, task_logger=task_logger
    )
    make_dbt_raw_1 = make_dbt_data_raw_model(
        census_dataset=census_dataset, conn_id=conn_id, task_logger=task_logger
    )
    update_raw_1 = update_data_raw_table(census_dataset=census_dataset, task_logger=task_logger)
    register_asset_2 = register_data_raw_table_asset(
        census_dataset=census_dataset, datasource_name=datasource_name, task_logger=task_logger
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


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_standardized_model_ready(task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    census_dataset = context["ti"].xcom_pull(key="census_dataset_key")
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "standardized",
        f"{census_dataset.dataset_name}_standardized.sql",
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
                return f"{tg_id_prefix}highlight_unfinished_dbt_standardized_stub"
        log_as_info(
            task_logger, "Found a _standardized stage dbt model that looks finished; Proceeding"
        )
        return f"{tg_id_prefix}dbt_clean_model_ready"
    else:
        log_as_info(task_logger, "No _standardized stage dbt model found.")
        log_as_info(task_logger, f"Creating a stub in loc: {host_file_path}")
        log_as_info(
            task_logger, "Edit the stub before proceeding to generate _clean stage dbt models."
        )
        return f"{tg_id_prefix}make_dbt_standardized_model"


@task
def highlight_unfinished_dbt_standardized_stub(task_logger: Logger) -> str:
    log_as_info(
        task_logger,
        "Hey! Go finish the dbt _standardized model file indicated in the logs for the "
        + "dbt_standardized_model_ready task! It still contains at least one placeholder value"
        + "(REPLACE_WITH_COMPOSITE_KEY_COLUMNS or REPLACE_WITH_BETTER_id).",
    )
    return "Please and thank you!"


@task
def make_dbt_standardized_model(conn_id: str, task_logger: Logger) -> bool:
    context = get_current_context()
    census_dataset = context["ti"].xcom_pull(key="census_dataset_key")
    engine = get_pg_engine(conn_id=conn_id)
    std_file_lines = format_dbt_stub_for_standardized_stage(
        table_name=census_dataset.dataset_name, engine=engine
    )
    file_path = Path(
        f"/opt/airflow/dbt/models/standardized/{census_dataset.dataset_name}_standardized.sql"
    )
    write_lines_to_file(file_lines=std_file_lines, file_path=file_path)
    log_as_info(task_logger, f"file_lines for table {census_dataset.dataset_name}")
    for file_line in std_file_lines:
        log_as_info(task_logger, f"    {file_line}")

    log_as_info(task_logger, "Leaving make_dbt_standardized_model")
    return True


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_clean_model_ready(task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    census_dataset = context["ti"].xcom_pull(key="census_dataset_key")
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "clean",
        f"{census_dataset.dataset_name}_clean.sql",
    )
    if file_path.is_file():
        log_as_info(task_logger, "Found a _clean stage dbt model that looks finished; Ending")
        return f"{tg_id_prefix}run_dbt_models__standardized_onward"
    else:
        return f"{tg_id_prefix}dbt_make_clean_model"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_make_clean_model(task_logger: Logger) -> bool:
    context = get_current_context()
    census_dataset = context["ti"].xcom_pull(key="census_dataset_key")
    airflow_home = os.environ["AIRFLOW_HOME"]
    clean_file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "clean",
        f"{census_dataset.dataset_name}_clean.sql",
    )
    clean_file_lines = format_dbt_stub_for_clean_stage(table_name=census_dataset.dataset_name)
    log_as_info(task_logger, f"clean_file_lines: {clean_file_lines}")
    write_lines_to_file(file_lines=clean_file_lines, file_path=clean_file_path)
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def run_dbt_models__standardized_onward(task_logger: Logger) -> bool:
    context = get_current_context()
    census_dataset = context["ti"].xcom_pull(key="census_dataset_key")
    dbt_cmd = format_dbt_run_cmd(
        dataset_name=census_dataset.dataset_name,
        schema="standardized",
        run_downstream=True,
    )
    return execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def endpoint(task_logger: Logger) -> bool:
    log_as_info(task_logger, "Ending run")
    return True


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


@task_group
def update_census_table(
    census_dataset: CensusVariableGroupDataset,
    datasource_name: str,
    conn_id: str,
    task_logger: Logger,
):
    freshness_check_1 = check_freshness(
        census_dataset=census_dataset, conn_id=conn_id, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        freshness_check=freshness_check_1, task_logger=task_logger
    )
    update_local_metadata_1 = update_local_metadata(conn_id=conn_id, task_logger=task_logger)
    local_is_fresh_1 = local_data_is_fresh(task_logger=task_logger)

    get_and_ingest_data_1 = request_and_ingest_dataset(
        census_dataset=census_dataset, conn_id=conn_id, task_logger=task_logger
    )

    raw_validation_1 = raw_data_validation_tg(
        census_dataset=census_dataset, datasource_name=datasource_name, task_logger=task_logger
    )
    persist_raw_1 = persist_new_raw_data_tg(
        census_dataset=census_dataset,
        datasource_name=datasource_name,
        conn_id=conn_id,
        task_logger=task_logger,
    )
    transform_data_1 = transform_data_tg(conn_id=conn_id, task_logger=task_logger)

    chain(
        freshness_check_1,
        fresh_source_data_available_1,
        [update_local_metadata_1, local_is_fresh_1],
    )
    chain(
        update_local_metadata_1,
        get_and_ingest_data_1,
        raw_validation_1,
        persist_raw_1,
        transform_data_1,
    )
    chain(local_is_fresh_1, transform_data_1)
