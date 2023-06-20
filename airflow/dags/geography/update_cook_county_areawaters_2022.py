import datetime as dt
import logging

from airflow.decorators import dag

from tasks.tiger_tasks import update_tiger_table

from sources.tiger_datasets import COOK_COUNTY_AREAWATERS_2022 as TIGER_DATASET

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule=TIGER_DATASET.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "census", "geospatial", "TIGER"],
)
def update_cook_county_areawaters_2022():
    update_tiger_table(
        tiger_dataset=TIGER_DATASET,
        datasource_name="fluent_dwh_source",
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )


update_cook_county_areawaters_2022()
