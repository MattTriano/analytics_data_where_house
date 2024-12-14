import datetime as dt
import logging
from logging import Logger
from pathlib import Path
import re
import subprocess

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from sources.tables import CHICAGO_POLICE_DISTRICT_BOUNDARIES as SOCRATA_TABLE
from cc_utils.utils import log_as_info
from cc_utils.transform import execute_dbt_cmd

task_logger = logging.getLogger("airflow.task")


@task
def run_specific_dbt_model_for_a_data_set(table_name: str, task_logger: Logger) -> bool:
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error run --full-refresh --select \
                  re_dbt.standardized.{table_name}_standardized"""
    result = execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error run --full-refresh --select \
                  re_dbt.clean.{table_name}_clean"""
    result = execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)
    return result


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
