import datetime as dt
import logging

from airflow.models.baseoperator import chain
from airflow.decorators import dag
from airflow.utils.edgemodifier import Label

from cc_utils.socrata import SocrataTable
from tasks.socrata_tasks import (
    download_fresh_data,
    fresher_source_data_available,
    check_table_metadata,
    load_data_tg,
    update_result_of_check_in_metadata_table,
    short_circuit_downstream,
)

task_logger = logging.getLogger("airflow.task")

SOCRATA_TABLE = SocrataTable(table_id="wvhk-k5uv", table_name="cook_county_parcel_sales")


@dag(
    schedule="0 6 4 * *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "parcels", "fact_table", "data_raw"],
)
def update_data_raw_cook_county_parcel_sales():
    POSTGRES_CONN_ID = "dwh_db_conn"

    metadata_1 = check_table_metadata(
        socrata_table=SOCRATA_TABLE, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        socrata_metadata=metadata_1, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    extract_data_1 = download_fresh_data(task_logger=task_logger)
    load_data_tg_1 = load_data_tg(
        socrata_metadata=extract_data_1,
        socrata_table=SOCRATA_TABLE,
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
    )
    update_metadata_false_1 = update_result_of_check_in_metadata_table(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger, data_updated=False
    )
    short_circuit_update_1 = short_circuit_downstream()

    chain(
        metadata_1,
        fresh_source_data_available_1,
        Label("Fresher data available"),
        extract_data_1,
        load_data_tg_1,
    )
    chain(
        metadata_1,
        fresh_source_data_available_1,
        Label("Local data is fresh"),
        update_metadata_false_1,
        short_circuit_update_1,
    )


update_data_raw_cook_county_parcel_sales()
