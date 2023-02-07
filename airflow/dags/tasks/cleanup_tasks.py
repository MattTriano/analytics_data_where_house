from logging import Logger
from pathlib import Path
import re
import shutil
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


@task
def get_paths_to_scheduler_logs_dirs_past_limit(
    scheduler_logs_dir: Path, keep_n_dirs: int, task_logger: Logger
) -> List:
    if scheduler_logs_dir.is_dir():
        scheduler_logs_dirs = [el.name for el in scheduler_logs_dir.iterdir() if el.is_dir()]
        scheduler_logs_dirs.sort()
        scheduler_logs_dirs_past_limit = [
            scheduler_logs_dir.joinpath(dn) for dn in scheduler_logs_dirs[:-keep_n_dirs]
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


@task
def get_details_on_data_raw_files(task_logger: Logger) -> List:
    data_dir_path = Path("/opt/airflow/data_raw").resolve()
    raw_file_objs = get_raw_socrata_data_file_objs(
        data_dir_path=data_dir_path, table_id_pattern=re.compile("^(\S{4}-\S{4})")
    )
    if len(raw_file_objs) > 0:
        for raw_file_obj in raw_file_objs:
            task_logger.info(f"raw_file_obj {raw_file_obj}.")
        return raw_file_objs
    else:
        task_logger.info(f"No files found in dir {data_dir_path}.")
        return list()


@task
def get_paths_to_data_files_over_n_downloads_old(
    raw_file_objs: List, task_logger: str, keep_n_files: int = 20
) -> List:
    sorted_objs = sorted(raw_file_objs, key=lambda x: x.file_name)

    file_dicts = {}
    for sorted_obj in sorted_objs:
        if sorted_obj.table_id not in file_dicts.keys():
            file_dicts[sorted_obj.table_id] = list()
        else:
            file_dicts[sorted_obj.table_id].append(sorted_obj)
    files_to_drop = []
    for table_id in file_dicts.keys():
        old_file_paths = [el.file_path for el in file_dicts[table_id][:-keep_n_files]]
        if len(old_file_paths) > 0:
            files_to_drop.extend(old_file_paths)
            task_logger.info(f"Table id: {table_id}, files to delete: {len(old_file_paths)}")
    return files_to_drop


@task
def delete_files_over_n_downloads_old(files_to_drop: List, task_logger: Logger) -> None:
    if len(files_to_drop) > 0:
        for file_to_drop in files_to_drop:
            task_logger.info(f"file: {file_to_drop}")
            task_logger.info(f"  Pre-drop.  File still exists? {file_to_drop.is_file()}")
            file_to_drop.unlink()
            task_logger.info(f"  Post-drop. File still exists? {file_to_drop.is_file()}")
