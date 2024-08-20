from pathlib import Path
from logging import Logger
import subprocess
import re

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label

import pandas as pd
from sqlalchemy import insert, update

from cc_utils.census.api import (
    CensusVariableGroupDataset,
    CensusAPIDatasetSource,
    CensusDatasetFreshnessCheck,
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
from cc_utils.validation import (
    run_checkpoint,
    check_if_checkpoint_exists,
    get_datasource,
    register_data_asset,
)


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
    task_logger.info(
        f"Max census_api_metadata id value after ingestion: {ingested_api_datasets_df['id'].max()}"
    )
    return ingested_api_datasets_df


@task
def get_source_dataset_metadata(
    census_dataset: CensusVariableGroupDataset, task_logger: Logger
) -> CensusAPIDatasetSource:
    dataset_base_url = census_dataset.api_call_obj.dataset_base_url
    dataset_source = CensusAPIDatasetSource(dataset_base_url=dataset_base_url)
    task_logger.info(f"Census dataset {dataset_base_url} metadata details:")
    task_logger.info(f"  - Dataset variables:   {len(dataset_source.variables_df)}")
    task_logger.info(f"  - Dataset geographies: {len(dataset_source.geographies_df)}")
    task_logger.info(f"  - Dataset groups:      {len(dataset_source.groups_df)}")
    task_logger.info(f"  - Dataset tags:        {len(dataset_source.tags_df)}")
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
    task_logger.info(f"dataset name:          {freshness_check_record['dataset_name']}")
    task_logger.info(
        f"dataset last_modified: {freshness_check_record['source_data_last_modified']}"
    )
    task_logger.info(f"dataset time_of_check: {freshness_check_record['time_of_check']}")

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
    task_logger.info(
        f"Last_modified datetime of latest local dataset update: {source_last_modified}."
    )
    return latest_dataset_check_df


@task
def organize_freshness_check_results(task_logger: Logger, **kwargs) -> CensusDatasetFreshnessCheck:
    ti = kwargs["ti"]
    freshness_check = CensusDatasetFreshnessCheck(
        dataset_source=ti.xcom_pull(
            task_ids="update_census_table.check_freshness.get_source_dataset_metadata"
        ),
        source_freshness=ti.xcom_pull(
            task_ids="update_census_table.check_freshness.record_source_freshness_check"
        ),
        local_freshness=ti.xcom_pull(
            task_ids="update_census_table.check_freshness.get_latest_local_freshness_check"
        ),
    )
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


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def fresher_source_data_available(
    freshness_check: CensusDatasetFreshnessCheck, task_logger: Logger
) -> str:
    dataset_in_local_dwh = len(freshness_check.local_freshness) > 0

    task_logger.info(f"Dataset in local dwh: {dataset_in_local_dwh}")
    task_logger.info(f"freshness_check.local_freshness: {freshness_check.local_freshness}")
    if dataset_in_local_dwh:
        local_last_modified = freshness_check.local_freshness["source_data_last_modified"].max()
        task_logger.info(f"Local dataset last modified: {local_last_modified}")
        source_last_modified = freshness_check.source_freshness["source_data_last_modified"].max()
        task_logger.info(f"Source dataset last modified: {source_last_modified}")
        local_dataset_is_fresh = local_last_modified >= source_last_modified
        if local_dataset_is_fresh:
            return "update_census_table.local_data_is_fresh"
    return "update_census_table.update_local_metadata.get_freshness_check_results"


@task
def get_freshness_check_results(task_logger: Logger, **kwargs) -> CensusDatasetFreshnessCheck:
    ti = kwargs["ti"]
    freshness_check = ti.xcom_pull(
        task_ids="update_census_table.check_freshness.organize_freshness_check_results"
    )
    return freshness_check


@task
def ingest_dataset_metadata(
    freshness_check: CensusDatasetFreshnessCheck, conn_id: str, task_logger: Logger
) -> str:
    metadata_df = freshness_check.dataset_source.metadata_catalog_df.copy()
    metadata_df["time_of_check"] = freshness_check.source_freshness["time_of_check"].max()
    task_logger.info(f"Dataset metadata columns:")
    for col in metadata_df.columns:
        task_logger.info(f"  {col}")
    task_logger.info(f"{metadata_df.T}")
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
    return "success"


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
    task_logger.info(f"Variables ingested: {len(ingested_df)}")
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
    task_logger.info(f"Geographies ingested: {len(ingested_df)}")
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
    task_logger.info(f"Groups ingested: {len(ingested_df)}")
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
    task_logger.info(f"Tags ingested: {len(ingested_df)}")
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
def local_data_is_fresh(task_logger: Logger):
    return "hi"


@task
def request_and_ingest_dataset(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger, **kwargs
) -> str:
    ti = kwargs["ti"]
    freshness_check = ti.xcom_pull(
        task_ids="update_census_table.check_freshness.organize_freshness_check_results"
    )
    engine = get_pg_engine(conn_id=conn_id)

    dataset_df = census_dataset.api_call_obj.make_api_call()
    task_logger.info(f"Rows in returned dataset: {len(dataset_df)}")
    task_logger.info(f"Columns in returned dataset: {dataset_df.columns}")
    dataset_df["dataset_base_url"] = census_dataset.api_call_obj.dataset_base_url
    dataset_df["dataset_id"] = freshness_check.source_freshness["id"].max()
    dataset_df["source_data_updated"] = freshness_check.source_freshness[
        "source_data_last_modified"
    ].max()
    dataset_df["ingestion_check_time"] = freshness_check.source_freshness["time_of_check"].max()
    task_logger.info(f"""dataset_id: {dataset_df["dataset_id"]}""")
    task_logger.info(f"""dataset_base_url: {dataset_df["dataset_base_url"]}""")
    task_logger.info(f"""source_data_updated: {dataset_df["source_data_updated"]}""")
    task_logger.info(f"""ingestion_check_time: {dataset_df["ingestion_check_time"]}""")
    dataset_df = standardize_column_names(df=dataset_df)
    result = dataset_df.to_sql(
        name=f"temp_{census_dataset.dataset_name}",
        schema="data_raw",
        con=engine,
        index=False,
        if_exists="replace",
        chunksize=100000,
    )
    task_logger.info(f"Ingestion result: {result}")
    if result is not None:
        return "ingested"
    else:
        raise Exception(f"No rows ingested")


@task(trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
def record_data_update(conn_id: str, task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    freshness_check = ti.xcom_pull(
        task_ids="update_census_table.check_freshness.organize_freshness_check_results"
    )
    engine = get_pg_engine(conn_id=conn_id)

    dataset_id = freshness_check.source_freshness["id"].max()
    pre_update_record = execute_result_returning_query(
        engine=engine,
        query=f"""SELECT * FROM metadata.dataset_metadata WHERE id = {dataset_id}""",
    )
    task_logger.info(f"General metadata record pre-update: {pre_update_record}")
    metadata_table = get_reflected_db_table(
        engine=engine, table_name="dataset_metadata", schema_name="metadata"
    )
    update_query = (
        update(metadata_table)
        .where(metadata_table.c.id == int(dataset_id))
        .values(local_data_updated=True)
    )
    execute_dml_orm_query(engine=engine, dml_stmt=update_query, logger=task_logger)
    task_logger.info(f"dataset_id: {dataset_id}")
    post_update_record = execute_result_returning_query(
        engine=engine,
        query=f"""SELECT * FROM metadata.dataset_metadata WHERE id = {dataset_id}""",
    )
    task_logger.info(f"General metadata record post-update: {post_update_record}")
    return "success"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def register_temp_table_asset(
    census_dataset: CensusVariableGroupDataset, datasource_name: str, task_logger: Logger
) -> str:
    task_logger.info
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
    checkpoint_name = f"data_raw.temp_{census_dataset.dataset_name}"
    if check_if_checkpoint_exists(checkpoint_name=checkpoint_name, task_logger=task_logger):
        task_logger.info(f"GE checkpoint for {checkpoint_name} exists")
        return "update_census_table.raw_data_validation_tg.run_temp_table_checkpoint"
    else:
        task_logger.info(f"GE checkpoint for {checkpoint_name} doesn't exist yet. Make it maybe?")
        return "update_census_table.raw_data_validation_tg.validation_endpoint"


@task
def run_temp_table_checkpoint(
    census_dataset: CensusVariableGroupDataset, task_logger: Logger
) -> str:
    checkpoint_name = f"data_raw.temp_{census_dataset.dataset_name}"
    checkpoint_run_results = run_checkpoint(
        checkpoint_name=checkpoint_name, task_logger=task_logger
    )
    task_logger.info(
        f"list_validation_results:      {checkpoint_run_results.list_validation_results()}"
    )
    task_logger.info(f"validation success:      {checkpoint_run_results.success}")
    task_logger.info(f"dir(checkpoint_run_results): {dir(checkpoint_run_results)}")
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
    task_logger.info(f"Entered raw_data_validation_tg task_group")
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
    task_logger.info(f"tables_in_data_raw_schema: {tables_in_data_raw_schema}")
    if census_dataset.dataset_name not in tables_in_data_raw_schema:
        task_logger.info(f"Table {census_dataset.dataset_name} not in data_raw; creating.")
        return "update_census_table.persist_new_raw_data_tg.create_table_in_data_raw"
    else:
        task_logger.info(f"Table {census_dataset.dataset_name} in data_raw; skipping.")
        return "update_census_table.persist_new_raw_data_tg.dbt_data_raw_model_exists"


@task
def create_table_in_data_raw(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> str:
    try:
        table_name = census_dataset.dataset_name
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
    return "table_created"


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_data_raw_model_exists(
    census_dataset: CensusVariableGroupDataset, task_logger: Logger
) -> str:
    dbt_data_raw_model_dir = Path(f"/opt/airflow/dbt/models/data_raw")
    task_logger.info(f"dbt data_raw model dir ('{dbt_data_raw_model_dir}')")
    task_logger.info(f"Dir exists? {dbt_data_raw_model_dir.is_dir()}")
    table_model_path = dbt_data_raw_model_dir.joinpath(f"{census_dataset.dataset_name}.sql")
    if table_model_path.is_file():
        return "update_census_table.persist_new_raw_data_tg.update_data_raw_table"
    else:
        return "update_census_table.persist_new_raw_data_tg.make_dbt_data_raw_model"


@task(retries=1)
def make_dbt_data_raw_model(
    census_dataset: CensusVariableGroupDataset, conn_id: str, task_logger: Logger
) -> str:
    make_dbt_data_raw_model_file(
        table_name=census_dataset.dataset_name, engine=get_pg_engine(conn_id=conn_id)
    )
    task_logger.info(f"Leaving make_dbt_data_raw_model")
    return "dbt_file_made"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_data_raw_table(census_dataset: CensusVariableGroupDataset, task_logger: Logger) -> str:
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error-options \
                        '{{"include": "all", "exclude": [UnusedResourceConfigPath]}}' \
                  run --select re_dbt.data_raw.{census_dataset.dataset_name}"""
    task_logger.info(f"dbt run command: {dbt_cmd}")
    subproc_output = subprocess.run(dbt_cmd, shell=True, capture_output=True, text=True)
    raise_exception = False
    for el in subproc_output.stdout.split("\n"):
        task_logger.info(f"{el}")
        if re.search("(\\d* of \\d* ERROR)", el):
            raise_exception = True
    if raise_exception:
        raise Exception("dbt model failed. Review the above outputs")
    return "data_raw_updated"


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
    )
