import datetime as dt
import logging

from airflow.decorators import dag
from airflow.models.baseoperator import chain

from tasks.tiger_tasks import (
    fresher_source_data_available,
    request_and_ingest_fresh_data,
    local_data_is_fresh,
    record_data_update,
    check_freshness,
)

from sources.tiger_datasets import UNITED_STATES_RAILS_2022 as TIGER_DATASET

task_logger = logging.getLogger("airflow.task")
POSTGRES_CONN_ID = "dwh_db_conn"


@dag(
    schedule=TIGER_DATASET.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["illinois", "census", "geospatial"],
)
def update_united_states_rails_2022():
    freshness_check_1 = check_freshness(
        tiger_dataset=TIGER_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    update_available_1 = fresher_source_data_available(
        freshness_check=freshness_check_1, task_logger=task_logger
    )
    local_data_is_fresh_1 = local_data_is_fresh(task_logger=task_logger)
    update_data_1 = request_and_ingest_fresh_data(
        tiger_dataset=TIGER_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )

    record_update_1 = record_data_update(conn_id=POSTGRES_CONN_ID, task_logger=task_logger)

    chain(freshness_check_1, update_available_1, [update_data_1, local_data_is_fresh_1])
    chain(update_data_1, record_update_1)


update_united_states_rails_2022()
