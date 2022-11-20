import datetime as dt
import re


def typeset_zulu_tz_datetime_str(datetime_str: str) -> dt.datetime:
    datetime_str = re.sub("Z$", " +0000", datetime_str)
    datetime_dt = dt.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S %z")
    return datetime_dt
