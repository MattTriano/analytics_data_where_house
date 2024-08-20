import datetime as dt
import logging
from logging import Logger
import os
from pathlib import Path
import re
import subprocess

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import get_data_table_names_in_schema, get_pg_engine
from cc_utils.file_factory import (
    write_lines_to_file,
    format_dbt_stub_for_standardized_stage,
    format_dbt_stub_for_clean_stage,
)
from tasks.socrata_tasks import highlight_unfinished_dbt_standardized_stub

from sources.tables import CHICAGO_MURALS as SOCRATA_TABLE

task_logger = logging.getLogger("airflow.task")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def table_in_warehouse(table_name: str, conn_id: str, task_logger: Logger) -> str:
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    task_logger.info(f"Tables in data_raw schema: {tables_in_data_raw_schema}")
    if table_name not in tables_in_data_raw_schema:
        task_logger.info(f"Table {table_name} isn't in the warehouse.")
        return "table_not_in_warehouse"
    else:
        task_logger.info(f"Table {table_name} is in the warehouse! Huzzah!")
        return "standardized_model_ready"


@task
def table_not_in_warehouse(table_name: str, task_logger: Logger) -> None:
    # this is a kludge to block this pathway in the DAG
    task_logger.info(f"Reiterating, table {table_name} isn't in the warehouse.")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def standardized_model_ready(table_name: str, task_logger: Logger) -> str:
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "standardized",
        f"{table_name}_standardized.sql",
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
                task_logger.info(
                    f"Found unfinished stub for dbt _standardized model in {host_file_path}."
                    + " Please update that model before proceeding to feature engineering."
                )
                return "highlight_unfinished_dbt_standardized_stub"
        task_logger.info(f"Found a _standardized stage dbt model that looks finished; Proceeding")
        return "clean_dbt_model_ready"
    else:
        task_logger.info(f"No _standardized stage dbt model found.")
        task_logger.info(
            f"Run this data set's update DAG when an update is available and edit the "
            + "generated stub before proceeding to generate _clean stage dbt models."
        )
        return "make_standardized_dbt_model"


@task
def make_standardized_dbt_model(table_name: str, conn_id: str, task_logger: Logger) -> None:
    engine = get_pg_engine(conn_id=conn_id)
    std_file_lines = format_dbt_stub_for_standardized_stage(table_name=table_name, engine=engine)
    file_path = Path(f"/opt/airflow/dbt/models/standardized/{table_name}_standardized.sql")
    write_lines_to_file(file_lines=std_file_lines, file_path=file_path)
    task_logger.info(f"file_lines for table {table_name}")
    for file_line in std_file_lines:
        task_logger.info(f"    {file_line}")

    task_logger.info(f"Leaving make_dbt_standardized_model")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def clean_dbt_model_ready(table_name: str, task_logger: Logger, **kwargs) -> str:
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "clean",
        f"{table_name}_clean.sql",
    )
    if file_path.is_file():
        task_logger.info(f"Found a _clean stage dbt model that looks finished")
        return "run_dbt_models_for_a_data_set"
    else:
        return "make_clean_dbt_model"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def make_clean_dbt_model(table_name: str, task_logger: Logger) -> str:
    airflow_home = os.environ["AIRFLOW_HOME"]
    clean_file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "clean",
        f"{table_name}_clean.sql",
    )
    clean_file_lines = format_dbt_stub_for_clean_stage(table_name=table_name)
    task_logger.info(f"clean_file_lines: {clean_file_lines}")
    write_lines_to_file(file_lines=clean_file_lines, file_path=clean_file_path)
    return table_name


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def run_dbt_models_for_a_data_set(table_name: str, task_logger: Logger) -> None:
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error run --select \
                  re_dbt.standardized.{table_name}_standardized+"""
    task_logger.info(f"dbt run command: {dbt_cmd}")
    try:
        subproc_output = subprocess.run(
            dbt_cmd, shell=True, capture_output=True, text=True, check=False
        )
        task_logger.info(f"subproc_output.stderr: {subproc_output.stderr}")
        task_logger.info(f"subproc_output.stdout: {subproc_output.stdout}")
        raise_exception = False
        for el in subproc_output.stdout.split("\n"):
            task_logger.info(f"{el}")
            if re.search("(\\d* of \\d* ERROR)", el):
                raise_exception = True
        if raise_exception:
            raise Exception("dbt model failed. Review the above outputs")
    except subprocess.CalledProcessError as err:
        task_logger.info(f"Error {err} while running dbt models. {type(err)}")
        raise


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def success_endpoint(task_logger: Logger) -> None:
    task_logger.info("Success! Clean and Standardized dbt models ran.")
    return "end"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def fail_endpoint(task_logger: Logger) -> None:
    task_logger.info("There's a bit of work still to be done before models can run.")
    raise Exception("There's a bit of work still to be done before models can run.")


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["dbt", "utility"],
)
def generate_and_or_run_dbt_models():
    table_in_warehouse_1 = table_in_warehouse(
        table_name=SOCRATA_TABLE.table_name,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    table_not_in_warehouse_1 = table_not_in_warehouse(
        table_name=SOCRATA_TABLE.table_name, task_logger=task_logger
    )
    std_model_ready_1 = standardized_model_ready(
        table_name=SOCRATA_TABLE.table_name, task_logger=task_logger
    )
    std_model_unfinished_1 = highlight_unfinished_dbt_standardized_stub(task_logger=task_logger)
    make_std_model_1 = make_standardized_dbt_model(
        table_name=SOCRATA_TABLE.table_name,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    clean_model_ready_1 = clean_dbt_model_ready(
        table_name=SOCRATA_TABLE.table_name, task_logger=task_logger
    )

    make_clean_model_1 = make_clean_dbt_model(
        table_name=SOCRATA_TABLE.table_name, task_logger=task_logger
    )

    run_dbt_models_1 = run_dbt_models_for_a_data_set(
        table_name=SOCRATA_TABLE.table_name, task_logger=task_logger
    )

    success_endpoint_1 = success_endpoint(task_logger=task_logger)
    fail_endpoint_1 = fail_endpoint(task_logger=task_logger)

    chain(
        table_in_warehouse_1,
        [
            Label("Table in warehouse, checking std model"),
            table_not_in_warehouse_1,
        ],
        [std_model_ready_1, fail_endpoint_1],
    )
    chain(
        std_model_ready_1,
        [
            Label("No dbt _standardized model found"),
            Label("dbt _standardized model needs review"),
            Label("Standardized model looks good, checking Clean model"),
        ],
        [make_std_model_1, std_model_unfinished_1, clean_model_ready_1],
    )
    chain(make_std_model_1, fail_endpoint_1)
    chain(std_model_unfinished_1, fail_endpoint_1)
    chain(
        clean_model_ready_1,
        [make_clean_model_1, Label("Clean model already set")],
        run_dbt_models_1,
        success_endpoint_1,
    )


generate_and_or_run_dbt_models()
