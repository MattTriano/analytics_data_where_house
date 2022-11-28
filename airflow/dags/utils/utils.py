import datetime as dt
from pathlib import Path
import re
import subprocess


def typeset_zulu_tz_datetime_str(datetime_str: str) -> dt.datetime:
    datetime_str = re.sub("Z$", " +0000", datetime_str)
    datetime_dt = dt.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S %z")
    return datetime_dt


def get_local_data_raw_dir() -> Path:
    output_dir = Path("/opt/airflow/data_raw").resolve()
    if not output_dir.is_dir():
        raise Exception("Couldn't find local (container) /opt/airflow/data_raw dir. Please fix.")
    return output_dir


def get_lines_in_file(file_path: Path) -> int:
    if file_path.is_file():
        lines_in_file = int(subprocess.getoutput(f'wc -l "{file_path}"').split()[0])
        return lines_in_file
    else:
        raise Exception(f"No file found at path '{file_path}'.")


def produce_ingest_slices_for_gpd_read_file(n_rows: int, rows_per_batch: int = 200000):
    start_inds = list(range(0, n_rows, rows_per_batch))
    end_inds = list(range(rows_per_batch - 1, n_rows + rows_per_batch - 1, rows_per_batch))
    range_list = list(zip(start_inds, end_inds))
    # return [slice(rng[0], rng[1]) for rng in range_list]
    return [{"ingest_slice": slice(rng[0], rng[1])} for rng in range_list]


def produce_slice_indices_for_gpd_read_file(n_rows: int, rows_per_batch: int = 200000):
    start_inds = list(range(0, n_rows, rows_per_batch))
    end_inds = list(range(rows_per_batch - 1, n_rows + rows_per_batch - 1, rows_per_batch))
    range_list = list(zip(start_inds, end_inds))
    inds = [{"start_index": el[0], "end_index": el[1]} for el in range_list]
    return inds


def get_lines_in_geojson_file(file_path) -> int:
    if file_path.name.lower().endswith(".geojson"):
        jq_cmd = f"jq -c -r '.features[]' < {file_path} | wc -l"
        n_rows = subprocess.check_output(jq_cmd, shell=True)
        return int(n_rows)
    else:
        raise Exception(
            "Not a geojson, or maybe it's formatted differently than this jq cmd can handle"
        )
