import datetime as dt
import logging

from airflow.decorators import dag

from tasks.cleanup_tasks import (
    get_paths_to_files_identical_to_prior_files,
    delete_files_identical_to_prior_files,
)

task_logger = logging.getLogger("airflow.task")


@dag(
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cleanup", "data_raw"],
)
def delete_data_files_identical_to_earlier_pulls():
    dupe_paths_1 = get_paths_to_files_identical_to_prior_files(task_logger=task_logger)
    delete_dupes_1 = delete_files_identical_to_prior_files(
        dupe_file_paths=dupe_paths_1, task_logger=task_logger
    )
    dupe_paths_1 >> delete_dupes_1


delete_data_files_identical_to_earlier_pulls()
