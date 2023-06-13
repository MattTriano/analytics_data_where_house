import datetime as dt
import logging

from airflow.decorators import dag
from airflow.models.baseoperator import chain
import pandas as pd

from tasks.census_tasks import (
    check_freshness,
    fresher_source_data_available,
    update_local_metadata,
    local_data_is_fresh,
    request_and_ingest_dataset,
    record_data_update,
)
from sources.census_api_datasets import CC_PLANNING_DB_HOUSING_AND_DEMOS_BY_BG as CENSUS_DATASET

task_logger = logging.getLogger("airflow.task")
pd.options.display.max_columns = None

POSTGRES_CONN_ID = "dwh_db_conn"


@dag(
    schedule=CENSUS_DATASET.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "census"],
)
def dev_cc_planning_db_housing_and_demos_by_bg():
    freshness_check_1 = check_freshness(
        census_dataset=CENSUS_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        freshness_check=freshness_check_1, task_logger=task_logger
    )
    update_local_metadata_1 = update_local_metadata(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    local_is_fresh_1 = local_data_is_fresh(task_logger=task_logger)

    get_and_ingest_data_1 = request_and_ingest_dataset(
        census_dataset=CENSUS_DATASET, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    record_update_1 = record_data_update(conn_id=POSTGRES_CONN_ID, task_logger=task_logger)

    chain(
        freshness_check_1,
        fresh_source_data_available_1,
        [update_local_metadata_1, local_is_fresh_1],
    )
    chain(update_local_metadata_1, get_and_ingest_data_1, record_update_1)


dev_cc_planning_db_housing_and_demos_by_bg()
