import datetime as dt
import logging
from logging import Logger
from pathlib import Path
import re
import subprocess

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from sources.tables import CHICAGO_DIVVY_STATIONS as SOCRATA_TABLE
from cc_utils.utils import log_as_info

task_logger = logging.getLogger("airflow.task")

# REFERENCE
# Running a model with all of its downstream models
# dbt_cmd = f"""cd /opt/airflow/dbt && \
#   dbt --warn-error run --select \
#   re_dbt.dwh.{table_name}_fact+"""
# Running all models in the `dwh` level
# dbt_cmd = f"""cd /opt/airflow/dbt &&
#                   dbt --warn-error run --select
#                   re_dbt.dwh.*"""


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def run_specific_dbt_model_for_a_data_set(table_name: str, task_logger: Logger) -> None:
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error run --full-refresh --select \
                  re_dbt.clean.chicago_towed_vehicles*+"""
    task_logger.info(f"dbt run command: {dbt_cmd}")
    try:
        subproc_output = subprocess.run(
            dbt_cmd, shell=True, capture_output=True, text=True, check=False
        )
        log_as_info(task_logger, f"subproc_output.stderr: {subproc_output.stderr}")
        log_as_info(task_logger, f"subproc_output.stdout: {subproc_output.stdout}")
        raise_exception = False
        for el in subproc_output.stdout.split("\n"):
            log_as_info(task_logger, f"{el}")
            if re.search("(\\d* of \\d* ERROR)", el):
                raise_exception = True
        if raise_exception:
            raise Exception("dbt model failed. Review the above outputs")
    except subprocess.CalledProcessError as err:
        log_as_info(task_logger, f"Error {err} while running dbt models. {type(err)}")
        raise


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["dbt", "utility"],
)
def run_a_specific_dbt_model():
    run_dbt_model_1 = run_specific_dbt_model_for_a_data_set(
        table_name=SOCRATA_TABLE.table_name, task_logger=task_logger
    )
    run_dbt_model_1


run_a_specific_dbt_model()
