from logging import Logger
import subprocess
import re

from cc_utils.utils import log_as_info


class DbtExecutionError(Exception):
    def __init__(self, msg):
        super().__init__(msg)


def run_dbt_dataset_transformations(
    dataset_name: str, task_logger: Logger, schema: str = "data_raw", dbt_project: str = "re_dbt"
) -> bool:
    dbt_cmd = f"""cd /opt/airflow/dbt && \
                  dbt --warn-error-options \
                        '{{"include": "all", "exclude": [UnusedResourceConfigPath]}}' \
                  run --select {dbt_project}.{schema}.{dataset_name}"""
    log_as_info(task_logger, f"dbt run command: {dbt_cmd}")
    subproc_output = subprocess.run(dbt_cmd, shell=True, capture_output=True, text=True)
    raise_exception = False
    for el in subproc_output.stdout.split("\n"):
        log_as_info(task_logger, f"{el}")
        if re.search("(\\d* of \\d* ERROR)", el):
            raise DbtExecutionError(
                msg=f"dbt model failed. Review the dbt output.\n{subproc_output.stdout}",
            )
    return True
