import datetime as dt
import logging

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.socrata import SocrataTable


task_logger = logging.getLogger("airflow.task")

SOCRATA_TABLE = SocrataTable(table_id="wvhk-k5uv", table_name="cook_county_parcel_sales")


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["dev_experiment"],
)
def a_dbt_test():
    # dbt_run = BashOperator(
    #     task_id="dbt_run",
    #     bash_command="""
    #         cd ${AIRFLOW_HOME}; dbt run --models "aggregate_transaction_features"
    #         """,
    # )
    pwd_1 = BashOperator(task_id="pwd", bash_command="pwd")
    # dbt_create_proj_1 = BashOperator(
    #     task_id='dbt_create_proj',
    #     bash_command='cd /opt/airflow/dags && dbt init dbt_proj'
    # )

    dbt_debug_1 = BashOperator(task_id="dbt_debug", bash_command="cd /opt/airflow/dbt && dbt debug")
    # --project-dir /opt/airflow/dbt

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --select models/staging/stg_cc_neighborhoods.sql",
    )

    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    pwd_1 >> dbt_debug_1 >> dbt_run >> end_1
    # dbt_create_proj_1 >>


# a_dbt_test()
