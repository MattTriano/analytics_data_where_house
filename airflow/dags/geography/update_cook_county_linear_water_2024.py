import datetime as dt
import logging

from airflow.decorators import dag

from tasks.tiger_tasks import update_tiger_table

from sources.tiger_datasets import COOK_COUNTY_LINEAR_WATER_2024 as TIGER_DATASET

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule=TIGER_DATASET.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["census", "geospatial", "TIGER"],
)
def update_cook_county_linear_water_2024_dev():
    update_tiger_table(
        tiger_dataset=TIGER_DATASET,
        datasource_name="fluent_dwh_source",
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )


update_cook_county_linear_water_2024_dev()
