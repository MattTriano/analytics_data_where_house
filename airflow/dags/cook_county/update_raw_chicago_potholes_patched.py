import datetime as dt
import logging

from pathlib import Path
import os

from airflow.decorators import dag

from tasks.socrata_tasks import (
    # update_socrata_table,
    download_fresh_data,
    check_table_metadata,
    fresher_source_data_available,
    load_data_tg,
    dbt_standardized_model_ready,
    highlight_unfinished_dbt_standardized_stub,
    make_dbt_staging_model,
    update_result_of_check_in_metadata_table,
    endpoint,
    short_circuit_downstream,
    make_dbt_standardized_model,
)
from sources.tables import CHICAGO_POTHOLES_PATCHED as SOCRATA_TABLE

task_logger = logging.getLogger("airflow.task")

from cc_utils.socrata import SocrataTable, SocrataTableMetadata
from logging import Logger
from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.file_factory import format_dbt_stub_for_intermediate_clean_stage, write_lines_to_file
from cc_utils.db import get_pg_engine

# @task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
# def dbt_staging_model_exists(task_logger: Logger, **kwargs) -> str:
#     ti = kwargs["ti"]
#     socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
#     dbt_staging_model_dir = Path(f"/opt/airflow/dbt/models/staging")
#     task_logger.info(f"dbt staging model dir ('{dbt_staging_model_dir}')")
#     task_logger.info(f"Dir exists? {dbt_staging_model_dir.is_dir()}")
#     table_model_path = dbt_staging_model_dir.joinpath(f"{socrata_metadata.table_name}.sql")
#     if table_model_path.is_file():
#         return "update_socrata_table.load_data_tg.update_data_raw_table"
#     else:
#         return "update_socrata_table.load_data_tg.make_dbt_staging_model"


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_clean_model_ready(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "intermediate",
        f"{socrata_metadata.table_name}_clean.sql",
    )
    host_file_path = str(file_path).replace(airflow_home, "/airflow")
    if file_path.is_file():
        task_logger.info(f"Found a _clean stage dbt model that looks finished; Ending")
        return "update_socrata_table.endpoint"
    else:
        return "update_socrata_table.dbt_make_clean_model"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_make_clean_model(conn_id: str, task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    engine = get_pg_engine(conn_id=conn_id)
    airflow_home = os.environ["AIRFLOW_HOME"]
    # std_file_path = Path(airflow_home).joinpath(
    #     "dbt",
    #     "models",
    #     "intermediate",
    #     f"{socrata_metadata.table_name}_standardized.sql",
    # )
    clean_file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "intermediate",
        f"{socrata_metadata.table_name}_clean.sql",
    )
    # with open(std_file_path, "r") as f:
    #     std_file_lines = f.readlines()
    # task_logger.info(f"std_file_lines: {std_file_lines}")
    clean_file_lines = format_dbt_stub_for_intermediate_clean_stage(
        table_name=socrata_metadata.table_name, engine=engine
    )
    task_logger.info(f"clean_file_lines: {clean_file_lines}")
    write_lines_to_file(file_lines=clean_file_lines, file_path=clean_file_path)
    return socrata_metadata


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
    std_model_exists_1 = dbt_standardized_model_ready(
        socrata_metadata=load_data_tg_1, task_logger=task_logger
    )
    std_model_unfinished_1 = highlight_unfinished_dbt_standardized_stub(task_logger=task_logger)
    make_standardized_stage_1 = make_dbt_standardized_model(
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    clean_model_ready_1 = dbt_clean_model_ready(task_logger=task_logger)
    make_clean_model_1 = dbt_make_clean_model(conn_id=conn_id, task_logger=task_logger)
    endpoint_1 = endpoint(task_logger=task_logger)
    update_metadata_false_1 = update_result_of_check_in_metadata_table(
        conn_id=conn_id, task_logger=task_logger, data_updated=False
    )
    short_circuit_update_1 = short_circuit_downstream()

    # chain(
    #     metadata_1,
    #     fresh_source_data_available_1,
    #     Label("Fresher data available"),
    #     extract_data_1,
    #     load_data_tg_1,
    #     std_model_exists_1,
    #     chain(
    #         make_standardized_stage_1,
    #         std_model_unfinished_1,
    #         clean_model_ready_1,
    #         [make_clean_model_1, Label("dbt _standardized model looks good!")]

    #     ),
    #     endpoint_1,
    # )
    metadata_1 >> fresh_source_data_available_1 >> Label("Fresher data available") >> extract_data_1
    extract_data_1 >> load_data_tg_1 >> std_model_exists_1
    std_model_exists_1 >> [
        Label("dbt _standardized model looks good!"),
        make_standardized_stage_1,
        std_model_unfinished_1,
    ]
    std_model_exists_1 >> Label("dbt _standardized model looks good!") >> clean_model_ready_1
    make_standardized_stage_1 >> clean_model_ready_1
    clean_model_ready_1 >> [make_clean_model_1, Label("dbt _clean model looks good!")] >> endpoint_1
    std_model_unfinished_1 >> endpoint_1

    # metadata_1 >> fresh_source_data_available_1 >> Label("Fresher data available") >> \
    #     extract_data_1 >> load_data_tg_1 >> std_model_exists_1 >> \
    #     [ make_standardized_stage_1 >> std_model_unfinished_1 >>
    #       clean_model_ready_1 >> [make_clean_model_1, Label("dbt _standardized model looks good!")]
    #     ] >> endpoint_1

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
    tags=["transit", "chicago", "cook_county", "geospatial", "data_raw"],
)
def update_data_raw_chicago_potholes_patched():
    update_1 = update_socrata_table(
        socrata_table=SOCRATA_TABLE,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    update_1


update_data_raw_chicago_potholes_patched()
