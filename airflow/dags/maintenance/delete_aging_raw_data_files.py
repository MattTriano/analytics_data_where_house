import datetime as dt
import logging

from airflow.decorators import dag
from airflow.models.baseoperator import chain

from tasks.cleanup_tasks import (
    get_details_on_data_raw_files,
    get_paths_to_data_files_over_n_downloads_old,
    delete_files_over_n_downloads_old,
)

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["utility", "maintenance"],
)
def delete_raw_data_files_over_n_downloads_old():
    data_raw_files_1 = get_details_on_data_raw_files(task_logger=task_logger)
    get_paths_older_than_n_1 = get_paths_to_data_files_over_n_downloads_old(
        raw_file_objs=data_raw_files_1, task_logger=task_logger, keep_n_files=15
    )
    delete_old_files_1 = delete_files_over_n_downloads_old(
        files_to_drop=get_paths_older_than_n_1, task_logger=task_logger
    )
    chain(data_raw_files_1, get_paths_older_than_n_1, delete_old_files_1)


delete_raw_data_files_over_n_downloads_old()
