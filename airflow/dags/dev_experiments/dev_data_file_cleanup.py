from dataclasses import dataclass
import datetime as dt
import filecmp
import itertools
import logging
from logging import Logger
from pathlib import Path
import re
from typing import List

from airflow.decorators import dag, task


task_logger = logging.getLogger("airflow.task")


@dataclass
class RawSocrataDataFile:
    file_path: Path
    file_name: str
    table_id: str
    file_last_modified_time: int
    file_bytes: int


def get_raw_socrata_data_file_objs(
    data_dir_path: Path, table_id_pattern: re.Pattern = re.compile("^(\S{4}-\S{4})")
) -> List[RawSocrataDataFile]:
    if not data_dir_path.is_dir():
        raise Exception(
            f"data_dir_path must be a path to a directory. No directory found at {data_dir_path}"
        )

    raw_file_objs = [
        RawSocrataDataFile(
            file_path=dp,
            file_name=dp.name,
            table_id=dp.name[0:9],
            file_last_modified_time=dp.stat().st_mtime,
            file_bytes=dp.stat().st_size,
        )
        for dp in data_dir_path.iterdir()
        if re.match(table_id_pattern, dp.name)
    ]
    return raw_file_objs


def get_duplicated_file_sizes_for_a_table_id(
    table_file_objs: List[RawSocrataDataFile],
) -> List[int]:
    file_size_counts = {}
    for table_file in table_file_objs:
        if table_file.file_bytes not in file_size_counts.keys():
            file_size_counts[table_file.file_bytes] = 1
        else:
            file_size_counts[table_file.file_bytes] = file_size_counts[table_file.file_bytes] + 1
    duplicated_file_sizes = [k for k, v in file_size_counts.items() if v > 1]
    return duplicated_file_sizes


def get_table_objs_with_a_given_file_size(
    table_file_objs: List[RawSocrataDataFile], duplicated_file_size: int
) -> List[RawSocrataDataFile]:
    table_files_w_this_file_size = [
        tf for tf in table_file_objs if tf.file_bytes == duplicated_file_size
    ]
    table_files_w_this_file_size = sorted(
        table_files_w_this_file_size, key=lambda x: x.file_last_modified_time
    )
    return table_files_w_this_file_size


def get_paths_of_files_identical_to_prior_data_pulls(
    data_dir_path: Path, date_ordered: List[RawSocrataDataFile]
) -> List[Path]:
    paths_for_duplicated_files = []
    combos = itertools.combinations([do.file_name for do in date_ordered], 2)
    for earlier_file_name, later_file_name in combos:
        earlier_time = dt.datetime.strptime(earlier_file_name[10:37], "%Y-%m-%dT%H:%M:%S.%fZ")
        later_time = dt.datetime.strptime(later_file_name[10:37], "%Y-%m-%dT%H:%M:%S.%fZ")
        if earlier_time < later_time:
            earlier_file = data_dir_path.joinpath(earlier_file_name)
            later_file = data_dir_path.joinpath(later_file_name)
            if earlier_file.is_file() and later_file.is_file():
                files_are_identical = filecmp.cmp(earlier_file, later_file, shallow=False)
                if files_are_identical:
                    paths_for_duplicated_files.append(later_file)
    return paths_for_duplicated_files


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


# @dag(
#     start_date=dt.datetime(2022, 11, 1),
#     catchup=False,
#     tags=["cleanup", "data_raw"],
# )
# def delete_data_files_identical_to_earlier_pulls():
#     dupe_paths_1 = get_paths_to_files_identical_to_prior_files(task_logger=task_logger)
#     delete_dupes_1 = delete_files_identical_to_prior_files(
#         dupe_file_paths=dupe_paths_1, task_logger=task_logger
#     )
#     dupe_paths_1 >> delete_dupes_1


# delete_data_files_identical_to_earlier_pulls()
