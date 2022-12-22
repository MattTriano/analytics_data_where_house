import datetime as dt
import logging

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from sources.tables import COOK_COUNTY_PARCEL_SALES as SOCRATA_TABLE


@dag(
    schedule=SOCRATA_TABLE.clean_schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "parcels", "fact_table", "data_raw"],
)
def clean_cook_county_parcel_sales():
    transform_raw_data_1 = BashOperator(
        task_id="transform_raw_data",
        bash_command=f"""cd /opt/airflow/dbt && \
            dbt --warn-error run --select \
                re_dbt.intermediate.{SOCRATA_TABLE.table_name}_standardized+""",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    transform_raw_data_1


clean_cook_county_parcel_sales()
