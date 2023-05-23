import datetime as dt
import logging

from airflow.decorators import dag

from tasks.census_tasks import check_api_dataset_freshness


task_logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "dwh_db_conn"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["census", "metadata"],
)
def refresh_census_api_metadata():
    check_api_dataset_freshness_1 = check_api_dataset_freshness(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )

    check_api_dataset_freshness_1


refresh_census_api_metadata()
