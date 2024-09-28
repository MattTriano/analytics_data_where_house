import datetime as dt
from logging import Logger
from pathlib import Path
import subprocess

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.utils.edgemodifier import Label
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from sqlalchemy import insert, update

from cc_utils.cleanup import standardize_column_names
from cc_utils.census.tiger import (
    TIGERCatalog,
    TIGERGeographicEntityVintage,
    TIGERDataset,
    TIGERDatasetFreshnessCheck,
)
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
def get_tiger_catalog(task_logger: Logger) -> TIGERCatalog:
    tiger_catalog = TIGERCatalog()
    task_logger.info(f"Available TIGER vintages:")
    for vintage_row in tiger_catalog.dataset_vintages.iterrows():
        task_logger.info(
            f"  - {vintage_row[1]['clean_name']} (last modified {vintage_row[1]['last_modified']})"
        )
    return tiger_catalog


@task
def get_entity_vintage_metadata(
    tiger_dataset: TIGERDataset, tiger_catalog: TIGERCatalog, task_logger: Logger
) -> TIGERGeographicEntityVintage:
    vintages = TIGERGeographicEntityVintage(
        entity_name=tiger_dataset.entity_name,
        year=tiger_dataset.vintage_year,
        catalog=tiger_catalog,
    )
    year = tiger_dataset.vintage_year
    entity_files = vintages.entity_files_metadata.copy()
    task_logger.info(
        f"Entity vintage metadata for vintage {year} for {tiger_dataset.entity_name} entities:"
        + f"\n  last_modified: {entity_files['last_modified'].values[0]}"
        + f"\n  Files: {entity_files['is_file'].sum()}"
    )
    entity_vintage = vintages.get_entity_file_metadata(geography=tiger_dataset.geography)
    task_logger.info(f"entities after filtering: {len(entity_vintage)}")
    return vintages


@task
def record_source_freshness_check(
    tiger_dataset: TIGERDataset,
    vintages: TIGERGeographicEntityVintage,
    conn_id: str,
    task_logger: Logger,
) -> pd.DataFrame:
    entity_vintage = vintages.get_entity_file_metadata(geography=tiger_dataset.geography)
    freshness_check_record = pd.DataFrame(
        {
            "dataset_name": [tiger_dataset.dataset_name],
            "source_data_last_modified": [entity_vintage["last_modified"].max()],
            "time_of_check": [dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")],
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
    task_logger.info(
        f"Last_modified datetime of latest local dataset update: {source_last_modified}."
    )
    return latest_dataset_check_df


@task
def organize_freshness_check_results(task_logger: Logger, **kwargs) -> TIGERDatasetFreshnessCheck:
    ti = kwargs["ti"]
    freshness_check = TIGERDatasetFreshnessCheck(
        source_freshness=ti.xcom_pull(
            task_ids="update_tiger_table.check_freshness.record_source_freshness_check"
        ),
        local_freshness=ti.xcom_pull(
            task_ids="update_tiger_table.check_freshness.get_latest_local_freshness_check"
        ),
    )
    task_logger.info(f"Source_freshness records: {len(freshness_check.source_freshness)}")
    task_logger.info(f"local_freshness records: {len(freshness_check.local_freshness)}")
    return freshness_check


@task_group
def check_freshness(
    tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger
) -> TIGERDatasetFreshnessCheck:
    tiger_catalog = get_tiger_catalog(task_logger=task_logger)
    local_dataset_freshness = get_latest_local_freshness_check(
        tiger_dataset=tiger_dataset, conn_id=conn_id, task_logger=task_logger
    )
    entity_vintages = get_entity_vintage_metadata(
        tiger_dataset=tiger_dataset, tiger_catalog=tiger_catalog, task_logger=task_logger
    )
    source_dataset_freshness = record_source_freshness_check(
        tiger_dataset=tiger_dataset,
        vintages=entity_vintages,
        conn_id=conn_id,
        task_logger=task_logger,
    )
    freshness_check = organize_freshness_check_results(task_logger=task_logger)
    chain(local_dataset_freshness, freshness_check)
    chain(source_dataset_freshness, freshness_check)
    return freshness_check


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def fresher_source_data_available(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    freshness_check = ti.xcom_pull(
        task_ids="update_tiger_table.check_freshness.organize_freshness_check_results"
    )
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
            return "update_tiger_table.local_data_is_fresh"
    return "update_tiger_table.request_and_ingest_fresh_data"


@task
def local_data_is_fresh(task_logger: Logger):
    return "hi"


@task
def request_and_ingest_fresh_data(
    tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger, **kwargs
):
    ti = kwargs["ti"]
    vintages = ti.xcom_pull(
        task_ids="update_tiger_table.check_freshness.get_entity_vintage_metadata"
    )
    freshness_check = ti.xcom_pull(
        task_ids="update_tiger_table.check_freshness.organize_freshness_check_results"
    )
    source_freshness = freshness_check.source_freshness
    task_logger.info(f"source_freshness:    {source_freshness} (type: {type(source_freshness)})")
    engine = get_pg_engine(conn_id=conn_id)
    full_gdf = vintages.get_entity_data(geography=tiger_dataset.geography)
    task_logger.info(f"Rows in returned TIGER dataset:    {len(full_gdf)}")
    task_logger.info(f"Columns in returned TIGER dataset: {full_gdf.columns}")
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
    return "ingested"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def record_data_update(conn_id: str, task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    freshness_check = ti.xcom_pull(
        task_ids="update_tiger_table.check_freshness.organize_freshness_check_results"
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
    if check_if_checkpoint_exists(checkpoint_name=checkpoint_name, task_logger=task_logger):
        task_logger.info(f"GE checkpoint for {checkpoint_name} exists")
        return "update_tiger_table.raw_data_validation_tg.run_temp_table_checkpoint"
    else:
        task_logger.info(f"GE checkpoint for {checkpoint_name} doesn't exist yet. Make it maybe?")
        return "update_tiger_table.raw_data_validation_tg.validation_endpoint"


@task
def run_temp_table_checkpoint(tiger_dataset: TIGERDataset, task_logger: Logger) -> str:
    checkpoint_name = f"data_raw.temp_{tiger_dataset.dataset_name}"
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
    datasource_name: str,
    tiger_dataset: TIGERDataset,
    task_logger: Logger,
):
    task_logger.info(f"Entered raw_data_validation_tg task_group")
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
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    task_logger.info(f"tables_in_data_raw_schema: {tables_in_data_raw_schema}")
    if tiger_dataset.dataset_name not in tables_in_data_raw_schema:
        task_logger.info(f"Table {tiger_dataset.dataset_name} not in data_raw; creating.")
        return "update_tiger_table.persist_new_raw_data_tg.create_table_in_data_raw"
    else:
        task_logger.info(f"Table {tiger_dataset.dataset_name} in data_raw; skipping.")
        return "update_tiger_table.persist_new_raw_data_tg.dbt_data_raw_model_exists"


@task
def create_table_in_data_raw(tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger) -> str:
    try:
        table_name = tiger_dataset.dataset_name
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
def dbt_data_raw_model_exists(tiger_dataset: TIGERDataset, task_logger: Logger) -> str:
    dbt_data_raw_model_dir = Path(f"/opt/airflow/dbt/models/data_raw")
    task_logger.info(f"dbt data_raw model dir ('{dbt_data_raw_model_dir}')")
    task_logger.info(f"Dir exists? {dbt_data_raw_model_dir.is_dir()}")
    table_model_path = dbt_data_raw_model_dir.joinpath(f"{tiger_dataset.dataset_name}.sql")
    if table_model_path.is_file():
        return "update_tiger_table.persist_new_raw_data_tg.update_data_raw_table"
    else:
        return "update_tiger_table.persist_new_raw_data_tg.make_dbt_data_raw_model"


@task(retries=1)
def make_dbt_data_raw_model(tiger_dataset: TIGERDataset, conn_id: str, task_logger: Logger) -> str:
    make_dbt_data_raw_model_file(
        table_name=tiger_dataset.dataset_name, engine=get_pg_engine(conn_id=conn_id)
    )
    task_logger.info(f"Leaving make_dbt_data_raw_model")
    return "dbt_file_made"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def update_data_raw_table(tiger_dataset: TIGERDataset, task_logger: Logger) -> str:
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error-options \
                        '{{"include": "all", "exclude": [UnusedResourceConfigPath]}}' \
                  run --select re_dbt.data_raw.{tiger_dataset.dataset_name}"""
    task_logger.info(f"dbt run command: {dbt_cmd}")
    subproc_output = subprocess.run(dbt_cmd, shell=True, capture_output=True, text=True)
    for el in subproc_output.stdout.split("\n"):
        task_logger.info(f"{el}")
    return "data_raw_updated"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def register_data_raw_table_asset(
    datasource_name: str, tiger_dataset: TIGERDataset, task_logger: Logger
) -> str:
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
    update_available_1 = fresher_source_data_available(
        freshness_check=freshness_check_1, task_logger=task_logger
    )
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
