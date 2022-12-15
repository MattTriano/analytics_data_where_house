import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag

from tasks.ddl_tasks import ensure_schema_exists


task_logger = logging.getLogger("airflow.task")


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["ddl"],
)
def setup_schemas():
    setup_data_raw_schema_1 = ensure_schema_exists(
        schema_name="data_raw", conn_id="dwh_db_conn", task_logger=task_logger
    )
    setup_clean_schema_1 = ensure_schema_exists(
        schema_name="clean", conn_id="dwh_db_conn", task_logger=task_logger
    )
    setup_marts_schema_1 = ensure_schema_exists(
        schema_name="marts", conn_id="dwh_db_conn", task_logger=task_logger
    )
    setup_data_raw_schema_1 >> setup_clean_schema_1 >> setup_marts_schema_1


setup_schemas()
