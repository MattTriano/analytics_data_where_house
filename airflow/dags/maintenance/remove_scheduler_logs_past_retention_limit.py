import datetime as dt
import logging
from pathlib import Path

from airflow.decorators import dag

from tasks.cleanup_tasks import (
    get_paths_to_scheduler_logs_dirs_past_limit,
    delete_directories,
)

task_logger = logging.getLogger("airflow.task")

# I should probably extract these out into some "configs" file.
# Also, I might want to just define an "AIRFLOW_HOME" env_var and make SCHEDULER_LOGS_DIR
# relative to that.
KEEP_LAST_N_DAYS_OF_SCHEDULER_LOGS = 30
SCHEDULER_LOGS_DIR = Path("/opt/airflow/logs/scheduler").resolve()


@dag(
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cleanup", "data_raw"],
)
def delete_scheduler_logs_past_retention_limit():
    dirs_past_limit_1 = get_paths_to_scheduler_logs_dirs_past_limit(
        scheduler_logs_dir=SCHEDULER_LOGS_DIR,
        keep_n_dirs=KEEP_LAST_N_DAYS_OF_SCHEDULER_LOGS,
        task_logger=task_logger,
    )
    delete_dirs_1 = delete_directories(dirs_to_delete=dirs_past_limit_1, task_logger=task_logger)
    dirs_past_limit_1 >> delete_dirs_1


delete_scheduler_logs_past_retention_limit()
