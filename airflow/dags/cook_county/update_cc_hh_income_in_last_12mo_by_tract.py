import datetime as dt
import logging

from airflow.decorators import dag

from tasks.census_tasks import update_census_table

from sources.census_api_datasets import CC_HH_INCOME_IN_LAST_12MO_BY_TRACT as CENSUS_DATASET

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule=CENSUS_DATASET.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "census"],
)
def update_cc_hh_income_in_last_12mo_by_tract():
    update_census_table(
        census_dataset=CENSUS_DATASET,
        datasource_name="fluent_dwh_source",
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )


update_cc_hh_income_in_last_12mo_by_tract()
