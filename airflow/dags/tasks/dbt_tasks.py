from logging import Logger
import os
from pathlib import Path

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import get_pg_engine
from cc_utils.file_factory import (
    format_dbt_stub_for_standardized_stage,
    format_dbt_stub_for_clean_stage,
    write_lines_to_file,
)
from cc_utils.transform import execute_dbt_cmd, format_dbt_run_cmd
from cc_utils.utils import get_task_group_id_prefix, log_as_info


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_standardized_model_ready(dataset_name: str, task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "standardized",
        f"{dataset_name}_standardized.sql",
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
def make_dbt_standardized_model(dataset_name: str, conn_id: str, task_logger: Logger) -> None:
    engine = get_pg_engine(conn_id=conn_id)
    std_file_lines = format_dbt_stub_for_standardized_stage(table_name=dataset_name, engine=engine)
    file_path = Path(f"/opt/airflow/dbt/models/standardized/{dataset_name}_standardized.sql")
    write_lines_to_file(file_lines=std_file_lines, file_path=file_path)
    log_as_info(task_logger, f"file_lines for table {dataset_name}")
    for file_line in std_file_lines:
        log_as_info(task_logger, f"    {file_line}")

    log_as_info(task_logger, "Leaving make_dbt_standardized_model")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_clean_model_ready(dataset_name: str, task_logger: Logger) -> str:
    context = get_current_context()
    tg_id_prefix = get_task_group_id_prefix(task_instance=context["ti"])
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath("dbt", "models", "clean", f"{dataset_name}_clean.sql")
    if file_path.is_file():
        log_as_info(task_logger, "Found a _clean stage dbt model that looks finished; Ending")
        return f"{tg_id_prefix}run_dbt_models__standardized_onward"
    else:
        return f"{tg_id_prefix}dbt_make_clean_model"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def dbt_make_clean_model(dataset_name: str, task_logger: Logger) -> bool:
    airflow_home = os.environ["AIRFLOW_HOME"]
    clean_file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "clean",
        f"{dataset_name}_clean.sql",
    )
    clean_file_lines = format_dbt_stub_for_clean_stage(table_name=dataset_name)
    log_as_info(task_logger, f"clean_file_lines: {clean_file_lines}")
    write_lines_to_file(file_lines=clean_file_lines, file_path=clean_file_path)
    return True


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def run_dbt_models__standardized_onward(dataset_name: str, task_logger: Logger) -> bool:
    dbt_cmd = format_dbt_run_cmd(
        dataset_name=dataset_name,
        schema="standardized",
        run_downstream=True,
    )
    return execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def endpoint(task_logger: Logger) -> bool:
    log_as_info(task_logger, "Ending run")
    return True


@task_group
def transform_data_tg(dataset_name: str, conn_id: str, task_logger: Logger):
    std_model_ready_1 = dbt_standardized_model_ready(dataset_name, task_logger)
    highlight_std_stub_1 = highlight_unfinished_dbt_standardized_stub(task_logger)
    make_std_model_1 = make_dbt_standardized_model(dataset_name, conn_id, task_logger)
    clean_model_ready_1 = dbt_clean_model_ready(dataset_name, task_logger)
    make_clean_model_1 = dbt_make_clean_model(dataset_name, task_logger)
    run_dbt_models_1 = run_dbt_models__standardized_onward(dataset_name, task_logger)
    endpoint_1 = endpoint(task_logger)

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
