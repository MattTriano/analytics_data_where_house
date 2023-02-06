import datetime as dt
import logging

from airflow.decorators import dag, task

import subprocess
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from tasks.socrata_tasks import (
    # update_socrata_table,
    SocrataTable,
    Logger,
    SocrataTableMetadata,
    check_table_metadata,
    fresher_source_data_available,
    download_fresh_data,
    load_data_tg,
    dbt_standardized_model_ready,
    highlight_unfinished_dbt_standardized_stub,
    make_dbt_standardized_model,
    # dbt_clean_model_ready,
    dbt_make_clean_model,
    endpoint,
    update_result_of_check_in_metadata_table,
    short_circuit_downstream,
    chain,
    Label,
    task_group,
    Path,
    os,
)
from sources.tables import CHICAGO_POTHOLES_PATCHED as SOCRATA_TABLE

task_logger = logging.getLogger("airflow.task")

transform_raw_data_1 = BashOperator(
    task_id="transform_raw_data",
    bash_command=f"""cd /opt/airflow/dbt && \
            dbt --warn-error run --select \
                re_dbt.intermediate.{SOCRATA_TABLE.table_name}_standardized+""",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)

# @task
# def run_dbt_models__standardized_onward(task_logger: Logger, **kwargs) -> SocrataTableMetadata:
#     ti = kwargs["ti"]
#     socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
#     dbt_cmd = f"""cd /opt/airflow/dbt && \
#                   dbt --warn-error run --select \
#                   re_dbt.intermediate.{socrata_metadata.table_name}_standardized+"""
#     task_logger.info(f"dbt run command: {dbt_cmd}")
#     subproc_output = subprocess.check_output(dbt_cmd, shell=True)
#     task_logger.info(f"subproc_output: {subproc_output}")
#     return socrata_metadata


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def run_dbt_models__standardized_onward(task_logger: Logger, **kwargs) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error run --select \
                  re_dbt.intermediate.{socrata_metadata.table_name}_standardized+"""
    task_logger.info(f"dbt run command: {dbt_cmd}")
    subproc_output = subprocess.run(dbt_cmd, shell=True, capture_output=True, text=True)
    for el in subproc_output.stdout.split("\n"):
        task_logger.info(f"{el}")
    return socrata_metadata


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
        return "update_socrata_table.run_dbt_models__standardized_onward"
    else:
        return "update_socrata_table.dbt_make_clean_model"


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
    std_model_ready_1 = dbt_standardized_model_ready(
        socrata_metadata=load_data_tg_1, task_logger=task_logger
    )
    std_model_unfinished_1 = highlight_unfinished_dbt_standardized_stub(task_logger=task_logger)
    make_standardized_stage_1 = make_dbt_standardized_model(
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    clean_model_ready_1 = dbt_clean_model_ready(task_logger=task_logger)
    make_clean_model_1 = dbt_make_clean_model(conn_id=conn_id, task_logger=task_logger)
    run_dbt_models_1 = run_dbt_models__standardized_onward(task_logger=task_logger)
    endpoint_1 = endpoint(task_logger=task_logger)
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
        std_model_ready_1,
    )
    chain(
        std_model_ready_1,
        [
            Label("No dbt _standardized model found"),
            Label("dbt _standardized model needs review"),
        ],
        [
            make_standardized_stage_1,
            std_model_unfinished_1,
        ],
        endpoint_1,
    )
    chain(
        std_model_ready_1,
        Label("dbt _standardized model looks good"),
        clean_model_ready_1,
        [Label("dbt _clean model looks good!"), make_clean_model_1],
        run_dbt_models_1,
        endpoint_1,
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
    tags=["transit", "chicago", "cook_county", "geospatial", "data_raw"],
)
def update_data_raw_chicago_potholes_patched_dev():
    update_1 = update_socrata_table(
        socrata_table=SOCRATA_TABLE,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    update_1


update_data_raw_chicago_potholes_patched_dev()


# @dag(
#     schedule=SOCRATA_TABLE.schedule,
#     start_date=dt.datetime(2022, 11, 1),
#     catchup=False,
#     tags=["transit", "chicago", "cook_county", "geospatial", "data_raw"],
# )
# def update_data_raw_chicago_potholes_patched():
#     update_1 = update_socrata_table(
#         socrata_table=SOCRATA_TABLE,
#         conn_id="dwh_db_conn",
#         task_logger=task_logger,
#     )
#     update_1


# update_data_raw_chicago_potholes_patched()
