import datetime as dt
from pathlib import Path
import re


def typeset_zulu_tz_datetime_str(datetime_str: str) -> dt.datetime:
    datetime_str = re.sub("Z$", " +0000", datetime_str)
    datetime_dt = dt.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S %z")
    return datetime_dt


def get_local_data_raw_dir() -> Path:
    output_dir = Path("/opt/airflow/data_raw").resolve()
    if not output_dir.is_dir():
        raise Exception("Couldn't find local (container) /opt/airflow/data_raw dir. Please fix.")
    return output_dir
