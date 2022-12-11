import datetime as dt
import logging

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.socrata import SocrataTable

SOCRATA_TABLE = SocrataTable(table_id="wvhk-k5uv", table_name="cook_county_parcel_sales")


@dag(
    schedule="0 6 4 * *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "parcels", "fact_table", "data_raw"],
)
def clean_cook_county_parcel_sales():
    POSTGRES_CONN_ID = "dwh_db_conn"

    standardize_raw_data_1 = BashOperator(
        task_id="standardize_raw_data",
        bash_command=f"""cd /opt/airflow/dbt && \
            dbt run --select models/intermediate/{SOCRATA_TABLE.table_name}_standardized.sql""",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    clean_standardized_data_1 = BashOperator(
        task_id="clean_standardized_data",
        bash_command=f"""cd /opt/airflow/dbt && \
            dbt run --select models/intermediate/{SOCRATA_TABLE.table_name}_clean.sql""",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    standardize_raw_data_1 >> clean_standardized_data_1


clean_cook_county_parcel_sales()
