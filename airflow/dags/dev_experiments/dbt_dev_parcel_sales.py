import datetime as dt
import logging

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.socrata import SocrataTable


task_logger = logging.getLogger("airflow.task")

# SOCRATA_TABLE = SocrataTable(table_id="wvhk-k5uv", table_name="cook_county_parcel_sales")


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["dev_experiment"],
)
def dbt_dev_update_data_raw_cook_county_parcel_sales():
    # dbt_stage_table_1 = BashOperator(
    #     task_id="dbt_stage_table",
    #     bash_command="cd /opt/airflow/dbt && dbt run --select models/staging/stg_cc_parcel_sales.sql",
    # )
    dbt_update_data_raw_table_1 = BashOperator(
        task_id="dbt_update_data_raw_table",
        bash_command="""cd /opt/airflow/dbt && \
            dbt run --select models/staging/dev_cook_county_parcel_sales_dbt_macro.sql""",
    )
    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    # dbt_stage_table_1 >>
    dbt_update_data_raw_table_1 >> end_1


# dbt_dev_update_data_raw_cook_county_parcel_sales()
