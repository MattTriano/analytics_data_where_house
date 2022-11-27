import datetime as dt
import logging

from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label

from utils.socrata import SocrataTable
from tasks.socrata_tasks import (
    get_socrata_table_metadata,
    extract_table_freshness_info,
    ingest_table_freshness_check_metadata,
    fresher_source_data_available,
    download_fresh_data,
    table_exists_in_warehouse,
    ingest_into_new_table_in_data_raw,
    ingest_into_temporary_table,
    update_table_metadata_in_db,
)


task_logger = logging.getLogger("airflow.task")

SOCRATA_TABLE = SocrataTable(table_id="c49d-89sn", table_name="cook_county_parcel_locations")


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def update_cc_parcel_locations_table():
    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    @task_group
    def extract_load_task_group() -> None:
        task_logger.info(f"In extract_load_task_group")
        metadata_4 = download_fresh_data()
        table_exists_1 = table_exists_in_warehouse(socrata_metadata=metadata_4)
        ingest_to_new_1 = ingest_into_new_table_in_data_raw()
        ingest_to_temp_1 = ingest_into_temporary_table()

        metadata_4 >> table_exists_1 >> Label("Adding Table") >> ingest_to_new_1
        metadata_4 >> table_exists_1 >> Label("Updating Table") >> ingest_to_temp_1

    metadata_1 = get_socrata_table_metadata(socrata_table=SOCRATA_TABLE)
    metadata_2 = extract_table_freshness_info(metadata_1)
    metadata_3 = ingest_table_freshness_check_metadata(metadata_2)
    fresh_source_data_available_1 = fresher_source_data_available(socrata_metadata=metadata_3)
    extract_load_task_group_1 = extract_load_task_group()

    metadata_5 = update_table_metadata_in_db()

    fresh_source_data_available_1 >> extract_load_task_group_1 >> metadata_5
    fresh_source_data_available_1 >> end_1


update_cc_parcel_locations_table_dag = update_cc_parcel_locations_table()
