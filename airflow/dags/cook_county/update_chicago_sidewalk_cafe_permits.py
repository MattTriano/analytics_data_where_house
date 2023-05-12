import datetime as dt
import logging

from airflow.decorators import dag

from tasks.socrata_tasks import update_socrata_table
from sources.tables import CHICAGO_SIDEWALK_CAFE_PERMITS as SOCRATA_TABLE

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["chicago", "walkability", "fact_table", "demo"],
)
def update_chicago_sidewalk_cafe_permits():
    update_1 = update_socrata_table(
        socrata_table=SOCRATA_TABLE,
        conn_id="dwh_db_conn",
        datasource_name="fluent_dwh_source",
        task_logger=task_logger,
    )
    update_1


update_chicago_sidewalk_cafe_permits()
