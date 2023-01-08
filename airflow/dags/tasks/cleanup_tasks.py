from logging import Logger
from pathlib import Path
import re
from typing import List

from airflow.decorators import task

from cc_utils.cleanup import (
    get_duplicated_file_sizes_for_a_table_id,
    get_paths_of_files_identical_to_prior_data_pulls,
    get_raw_socrata_data_file_objs,
    get_table_objs_with_a_given_file_size,
)


@task
def get_paths_to_files_identical_to_prior_files(task_logger: Logger) -> List:
    data_dir_path = Path("/opt/airflow/data_raw").resolve()
    raw_file_objs = get_raw_socrata_data_file_objs(
        data_dir_path=data_dir_path, table_id_pattern=re.compile("^(\S{4}-\S{4})")
    )
    distinct_table_ids = set(st.table_id for st in raw_file_objs)

    all_paths_for_duplicated_files = []
    for distinct_table_id in distinct_table_ids:
        table_file_objs = [tf for tf in raw_file_objs if tf.table_id == distinct_table_id]
        task_logger.info(f"Number of pulls of table-id {distinct_table_id}: {len(table_file_objs)}")
        duplicated_file_sizes = get_duplicated_file_sizes_for_a_table_id(
            table_file_objs=table_file_objs
        )
        if len(duplicated_file_sizes) > 0:
            task_logger.info(
                f" - Duplicated file sizes found for table_id {duplicated_file_sizes}."
            )
            for duplicated_file_size in duplicated_file_sizes:
                date_ordered = get_table_objs_with_a_given_file_size(
                    table_file_objs=table_file_objs, duplicated_file_size=duplicated_file_size
                )
                paths_for_duplicated_files = get_paths_of_files_identical_to_prior_data_pulls(
                    data_dir_path=data_dir_path, date_ordered=date_ordered
                )
                if len(paths_for_duplicated_files) > 0:
                    all_paths_for_duplicated_files.extend(paths_for_duplicated_files)
                    task_logger.info(f"Paths of files identical to earlier files")
                    for path_for_duplicated_files in paths_for_duplicated_files:
                        task_logger.info(f"    {path_for_duplicated_files}")
    return all_paths_for_duplicated_files


@task
def delete_files_identical_to_prior_files(dupe_file_paths: List, task_logger: Logger) -> None:
    task_logger.info(f"Paths of files identical to earlier files")
    for dupe_file_path in dupe_file_paths:
        if dupe_file_path.is_file():
            task_logger.info(f"Deleting file: {dupe_file_path}")
            dupe_file_path.unlink(missing_ok=False)
            task_logger.info(f"  - Successfully deleted? {not dupe_file_path.is_file()}")
