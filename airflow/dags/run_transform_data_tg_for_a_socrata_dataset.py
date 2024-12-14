import datetime as dt
import logging
import os
from logging import Logger
from pathlib import Path

from cc_utils.utils import get_task_group_id_prefix
from cc_utils.transform import execute_dbt_cmd
from sources.tables import CHICAGO_VACANT_AND_ABANDONED_BUILDINGS as SOCRATA_TABLE
from tasks.socrata_tasks import (
    dbt_make_clean_model,
    dbt_standardized_model_ready,
    endpoint,
    get_socrata_table_metadata,
    highlight_unfinished_dbt_standardized_stub,
    log_as_info,
    make_dbt_standardized_model,
)

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

task_logger = logging.getLogger("airflow.task")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_clean_model_ready(task_logger: Logger) -> str:
    context = get_current_context()
    task_group_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    socrata_metadata = context["ti"].xcom_pull(key="socrata_metadata_key")
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "clean",
        f"{socrata_metadata.table_name}_clean.sql",
    )
    if file_path.is_file():
        log_as_info(task_logger, f"headed to {task_group_id_prefix}run_dbt_std_model")
        return f"{task_group_id_prefix}run_dbt_std_model"
    else:
        return f"{task_group_id_prefix}dbt_make_clean_model"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def run_dbt_std_model(table_name: str, task_logger: Logger) -> bool:
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error run --select \
                  re_dbt.standardized.{table_name}_standardized"""
    result = execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)
    return result


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def run_dbt_clean_model(table_name: str, task_logger: Logger) -> bool:
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error run --select \
                  re_dbt.clean.{table_name}_clean"""
    result = execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)
    return result


@task_group
def transform_data_tg(socrata_table, conn_id: str, task_logger: Logger):
    std_model_ready_1 = dbt_standardized_model_ready(task_logger=task_logger)
    highlight_std_stub_1 = highlight_unfinished_dbt_standardized_stub(task_logger=task_logger)
    make_std_model_1 = make_dbt_standardized_model(conn_id=conn_id, task_logger=task_logger)
    clean_model_ready_1 = dbt_clean_model_ready(task_logger=task_logger)
    make_clean_model_1 = dbt_make_clean_model(task_logger=task_logger)
    # run_dbt_models_1 = run_dbt_models__standardized_onward(task_logger=task_logger)
    run_dbt_std_model_1 = run_dbt_std_model(
        table_name=socrata_table.table_name, task_logger=task_logger
    )
    run_dbt_clean_model_1 = run_dbt_clean_model(
        table_name=socrata_table.table_name, task_logger=task_logger
    )
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
        run_dbt_std_model_1,
        run_dbt_clean_model_1,
        endpoint_1,
    )


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["dbt", "utility"],
)
def run_socrata_transform_data_tg():
    metadata_1 = get_socrata_table_metadata(socrata_table=SOCRATA_TABLE, task_logger=task_logger)
    transform_data_1 = transform_data_tg(
        socrata_table=SOCRATA_TABLE, conn_id="dwh_db_conn", task_logger=task_logger
    )

    chain(metadata_1, transform_data_1)


run_socrata_transform_data_tg()
