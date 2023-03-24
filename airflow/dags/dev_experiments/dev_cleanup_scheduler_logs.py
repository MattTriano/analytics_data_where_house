from dataclasses import dataclass
import datetime as dt
import filecmp
import itertools
import logging
from logging import Logger
from pathlib import Path
import re
import shutil
from typing import List

from airflow.decorators import dag, task


task_logger = logging.getLogger("airflow.task")

KEEP_LAST_N_DAYS_OF_SCHEDULER_LOGS = 30
SCHEDULER_LOGS_DIR = Path("/opt/airflow/logs/scheduler").resolve()


@task
def get_paths_to_scheduler_logs_dirs_past_limit(
    scheduler_logs_dir: Path, keep_n_dirs: int, task_logger: Logger
) -> List:
    if scheduler_logs_dir.is_dir():
        scheduler_logs_dirs = [el.name for el in scheduler_logs_dir.iterdir() if el.is_dir()]
        scheduler_logs_dirs.sort()
        scheduler_logs_dirs_past_limit = [
            scheduler_logs_dir.joinpath(dn)
            for dn in scheduler_logs_dirs[:-KEEP_LAST_N_DAYS_OF_SCHEDULER_LOGS]
        ]
        task_logger.info(
            f"\nscheduler_logs_dirs_past_limit:\n - "
            + ",\n - ".join([str(dp) for dp in scheduler_logs_dirs_past_limit])
        )
        return scheduler_logs_dirs_past_limit
    else:
        raise Exception(
            f"The given scheduler_logs_dir path,"
            + f"\n {scheduler_logs_dir} \n"
            + "doesn't resolve to a directory."
        )


@task
def delete_directories(dirs_to_delete: List, task_logger: Logger) -> None:
    for dir_to_delete in dirs_to_delete:
        if dir_to_delete.is_dir():
            task_logger.info(f"Removing directory {dir_to_delete}")
            shutil.rmtree(dir_to_delete)
        else:
            task_logger.info(f"Path {dir_to_delete} isn't to a directory. Very curious...")


# @dag(
#     start_date=dt.datetime(2022, 11, 1),
#     catchup=False,
#     tags=["cleanup", "data_raw"],
# )
# # def delete_scheduler_logs_past_retention_limit():
#     dirs_past_limit_1 = get_paths_to_scheduler_logs_dirs_past_limit(
#         scheduler_logs_dir=SCHEDULER_LOGS_DIR,
#         keep_n_dirs=KEEP_LAST_N_DAYS_OF_SCHEDULER_LOGS,
#         task_logger=task_logger,
#     )
#     delete_dirs_1 = delete_directories(
#         dirs_to_delete=dirs_past_limit_1, task_logger=task_logger
#     )
#     dirs_past_limit_1 >> delete_dirs_1


# delete_scheduler_logs_past_retention_limit()
