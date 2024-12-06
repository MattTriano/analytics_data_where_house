from logging import Logger
import subprocess
import re

from cc_utils.utils import log_as_info


class DbtExecutionError(Exception):
    def __init__(self, msg):
        super().__init__(msg)


def format_dbt_run_cmd(
    dataset_name: str,
    schema: str = "data_raw",
    dbt_project: str = "re_dbt",
    run_downstream: bool = False,
) -> str:
    if run_downstream:
        suffix = "*+"
    else:
        suffix = ""
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error-options \
                        '{{"include": "all", "exclude": [UnusedResourceConfigPath]}}' \
                  run --select {dbt_project}.{schema}.{dataset_name}{suffix}"""
    return dbt_cmd


def execute_dbt_cmd(dbt_cmd: str, task_logger: Logger) -> bool:
    log_as_info(task_logger, f"dbt command: {dbt_cmd}")
    if "--warn-error" not in dbt_cmd:
        task_logger.warning(
            "The --warn-error flag isn't in the dbt_cmd, so warnings won't cause the task to fail."
        )
    result = subprocess.run(dbt_cmd, shell=True, capture_output=True, text=True)
    log_as_info(task_logger, f"dbt cmd returncode: {result.returncode}")
    raise_exception = False
    for el in result.stdout.split("\n"):
        log_as_info(task_logger, f"{el}")
    if result.returncode != 0:
        raise DbtExecutionError(
            msg=f"dbt model failed. Review the dbt output.\n{result.stdout}",
        )
    return True
