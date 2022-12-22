import datetime as dt
import logging

from airflow.decorators import dag

from tasks.socrata_tasks import update_socrata_table
from sources.tables import CHICAGO_SHOTSPOTTER_ALERTS as SOCRATA_TABLE

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["chicago", "violent_crime", "fact_table", "data_raw"],
)
def update_data_raw_chicago_homicide_and_shooting_victimizations():
    update_1 = update_socrata_table(
        socrata_table=SOCRATA_TABLE,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    update_1


update_data_raw_chicago_homicide_and_shooting_victimizations()
