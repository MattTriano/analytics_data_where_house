from copy import copy
from dataclasses import dataclass
import datetime as dt
import filecmp
import itertools
from pathlib import Path
import re
from typing import List


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


def standardize_columns_fill_spaces(df):
    initial_columns = copy(df.columns)
    fixed_columns = {c: c for c in initial_columns}
    print(fixed_columns)

    columns_with_spaces = [col for col in initial_columns if " " in col]
    columns_without_spaces = [col for col in initial_columns if " " not in col]
    underscores_imputed = ["_".join(col.split()) for col in copy(columns_with_spaces)]

    for column_with_spaces in columns_with_spaces:
        print(f"column_with_spaces: {column_with_spaces}")
        fixed_col_name = "_".join(column_with_spaces.split())
        print(f"fixed_col_name: {fixed_col_name}")
        print(f"column_with_spaces: {column_with_spaces}")
        if (fixed_col_name not in fixed_columns.keys()) and (
            fixed_col_name not in fixed_columns.values()
        ):
            fixed_columns[column_with_spaces] = fixed_col_name
        else:
            suffix = 1
            while (
                (f"{fixed_col_name}_{suffix}" in fixed_columns.keys())
                or (f"{fixed_col_name}_{suffix}" in fixed_columns.values())
                or (f"{fixed_col_name}_{suffix}" in underscores_imputed)
            ):
                suffix += 1
            fixed_columns[column_with_spaces] = f"{fixed_col_name}_{suffix}"
    fixed_columns = {k: v.lower() for k, v in fixed_columns.items()}
    df = df.rename(columns=fixed_columns)
    return df
