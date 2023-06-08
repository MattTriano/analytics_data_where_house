import datetime as dt
import logging

from airflow.decorators import dag
from airflow.models.baseoperator import chain

from tasks.census_tasks import (
    check_freshness,
    fresher_source_data_available,
    update_local_metadata,
    local_data_is_fresh,
    request_and_ingest_dataset,
    record_data_update,
)
from sources.census_api_datasets import GROSS_RENT_BY_COOK_COUNTY_IL_TRACT as CENSUS_DATASET

task_logger = logging.getLogger("airflow.task")
POSTGRES_CONN_ID = "dwh_db_conn"


@dag(
    schedule=CENSUS_DATASET.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "census"],
)
def update_cc_gross_rent_by_tract():
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


update_cc_gross_rent_by_tract()
