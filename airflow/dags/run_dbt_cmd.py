import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.transform import execute_dbt_cmd

task_logger = logging.getLogger("airflow.task")


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def run_dbt(task_logger: Logger) -> None:
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt test --select models/clean/test_incremental_dedupe_logic.yml --warn-error"""
    result = execute_dbt_cmd(dbt_cmd=dbt_cmd, task_logger=task_logger)
    return result


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["dbt", "utility", "test"],
)
def run_dbt_cmd():
    run_dbt_1 = run_dbt(task_logger=task_logger)
    run_dbt_1


run_dbt_cmd()
