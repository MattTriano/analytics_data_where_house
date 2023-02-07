import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task

from cc_utils.db import get_data_table_names_in_schema, get_pg_engine, execute_structural_command


task_logger = logging.getLogger("airflow.task")


@task
def drop_temp_tables(conn_id: str, task_logger: Logger) -> None:
    engine = get_pg_engine(conn_id=conn_id)
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=engine, schema_name="data_raw"
    )
    table_names = [f"data_raw.{tn}" for tn in tables_in_data_raw_schema if tn.startswith("temp_")]
    if len(table_names) > 0:
        for tn in table_names:
            task_logger.info(f"Dropping Table {tn}.")
            execute_structural_command(
                query=f"DROP TABLE IF EXISTS {tn} CASCADE;",
                engine=engine,
            )
        task_logger.info(f"Temp tables dropped.")
    else:
        task_logger.info(f"No 'temp_' tables in data_raw to drop.")


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["utility", "maintenance"],
)
def drop_data_raw_temp_tables():
    drop_temp_tables_1 = drop_temp_tables(conn_id="dwh_db_conn", task_logger=task_logger)
    drop_temp_tables_1


drop_data_raw_temp_tables()
