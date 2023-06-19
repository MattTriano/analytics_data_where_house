import datetime as dt
import logging
from logging import Logger
from pathlib import Path
import subprocess

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label


from cc_utils.db import (
    get_pg_engine,
    get_data_table_names_in_schema,
    execute_structural_command,
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
from cc_utils.census.tiger import (
    TIGERCatalog,
    TIGERGeographicEntityVintage,
    TIGERDataset,
)
from tasks.tiger_tasks import (
    fresher_source_data_available,
    request_and_ingest_fresh_data,
    local_data_is_fresh,
    record_data_update,
    check_freshness,
)

from sources.tiger_datasets import UNITED_STATES_COUNTIES_2022 as TIGER_DATASET

task_logger = logging.getLogger("airflow.task")
POSTGRES_CONN_ID = "dwh_db_conn"
DATASOURCE_NAME = "fluent_dwh_source"


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
        return "raw_data_validation_tg.run_temp_table_checkpoint"
    else:
        task_logger.info(f"GE checkpoint for {checkpoint_name} doesn't exist yet. Make it maybe?")
        return "raw_data_validation_tg.validation_endpoint"


@task
def run_temp_table_checkpoint(tiger_dataset: TIGERDataset, task_logger: Logger, **kwargs) -> str:
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
        return "persist_new_raw_data_tg.create_table_in_data_raw"
    else:
        task_logger.info(f"Table {tiger_dataset.dataset_name} in data_raw; skipping.")
        return "persist_new_raw_data_tg.dbt_data_raw_model_exists"


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
        return "persist_new_raw_data_tg.update_data_raw_table"
    else:
        return "persist_new_raw_data_tg.make_dbt_data_raw_model"


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


@dag(
    schedule=TIGER_DATASET.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["illinois", "census", "geospatial"],
)
def update_united_states_counties_2022():
    freshness_check_1 = check_freshness(
        tiger_dataset=TIGER_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    update_available_1 = fresher_source_data_available(
        freshness_check=freshness_check_1, task_logger=task_logger
    )
    local_data_is_fresh_1 = local_data_is_fresh(task_logger=task_logger)
    update_data_1 = request_and_ingest_fresh_data(
        tiger_dataset=TIGER_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    raw_validation_1 = raw_data_validation_tg(
        datasource_name=DATASOURCE_NAME, tiger_dataset=TIGER_DATASET, task_logger=task_logger
    )
    persist_raw_1 = persist_new_raw_data_tg(
        datasource_name=DATASOURCE_NAME,
        tiger_dataset=TIGER_DATASET,
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
    )

    chain(freshness_check_1, update_available_1, [update_data_1, local_data_is_fresh_1])
    chain(update_data_1, raw_validation_1, persist_raw_1)


update_united_states_counties_2022()
