import datetime as dt
import logging
from logging import Logger

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import get_pg_engine, get_data_table_names_in_schema
from cc_utils.socrata import SocrataTable, SocrataTableMetadata
from tasks.socrata_tasks import (
    download_fresh_data,
    fresher_source_data_available,
    check_table_metadata,
    file_ext_branch_router,
    load_geojson_data,
    load_csv_data,
    # table_exists_in_data_raw,
    create_table_in_data_raw,
    # load_data_tg,
)

task_logger = logging.getLogger("airflow.task")

SOCRATA_TABLE = SocrataTable(table_id="wvhk-k5uv", table_name="cook_county_parcel_sales")


@task(trigger_rule=TriggerRule.NONE_FAILED)
def update_result_of_check_in_metadata_table(
    conn_id: str, task_logger: Logger, data_updated: bool, **kwargs
) -> SocrataTableMetadata:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="download_fresh_data")
    task_logger.info(f"Updating table_metadata record id #{socrata_metadata.freshness_check_id}.")
    socrata_metadata.update_current_freshness_check_in_db(
        engine=get_pg_engine(conn_id=conn_id),
        update_payload={"data_pulled_this_check": data_updated},
    )
    return socrata_metadata


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def table_exists_in_data_raw(conn_id: str, task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="download_fresh_data")
    tables_in_data_raw_schema = get_data_table_names_in_schema(
        engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
    )
    task_logger.info(f"tables_in_data_raw_schema: {tables_in_data_raw_schema}")
    if socrata_metadata.table_name not in tables_in_data_raw_schema:
        task_logger.info(f"Table {socrata_metadata.table_name} not in data_raw; creating.")
        return "load_data_tg.create_table_in_data_raw"
    else:
        task_logger.info(f"Table {socrata_metadata.table_name} in data_raw; skipping.")
        return "load_data_tg.update_data_raw_table"


@task_group
def load_data_tg(
    socrata_metadata: SocrataTableMetadata,
    socrata_table: SocrataTable,
    conn_id: str,
    task_logger: Logger,
) -> SocrataTableMetadata:
    task_logger.info(f"Entered load_data_tg task_group")
    file_ext_route_1 = file_ext_branch_router(socrata_metadata=socrata_metadata)

    geojson_route_1 = load_geojson_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )
    csv_route_1 = load_csv_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )
    table_exists_1 = table_exists_in_data_raw(conn_id=conn_id, task_logger=task_logger)
    create_staging_table_1 = create_table_in_data_raw(
        conn_id=conn_id, task_logger=task_logger, temp_table=False
    )
    dbt_update_data_raw_table_1 = BashOperator(
        task_id="update_data_raw_table",
        bash_command=f"""cd /opt/airflow/dbt && \
            dbt run --select models/staging/{socrata_table.table_name}.sql""",
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    update_metadata_true_1 = update_result_of_check_in_metadata_table(
        conn_id=conn_id, task_logger=task_logger, data_updated=True
    )

    # data_load_end_1 = EmptyOperator(task_id="data_load_end", trigger_rule=TriggerRule.NONE_FAILED)

    chain(
        file_ext_route_1,
        [geojson_route_1, csv_route_1],
        table_exists_1,
        Label("Table Exists"),
        dbt_update_data_raw_table_1,
        update_metadata_true_1,
    )
    chain(
        file_ext_route_1,
        [geojson_route_1, csv_route_1],
        table_exists_1,
        Label("Creating Table"),
        create_staging_table_1,
        dbt_update_data_raw_table_1,
        update_metadata_true_1,
    )


@dag(
    schedule="0 6 4 * *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "parcels", "fact_table"],
)
def extract_load_update_cc_parcel_sales_table():
    POSTGRES_CONN_ID = "dwh_db_conn"

    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    metadata_1 = check_table_metadata(
        socrata_table=SOCRATA_TABLE, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        socrata_metadata=metadata_1, conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )
    extract_data_1 = download_fresh_data(task_logger=task_logger)
    load_data_tg_1 = load_data_tg(
        socrata_metadata=extract_data_1,
        socrata_table=SOCRATA_TABLE,
        conn_id=POSTGRES_CONN_ID,
        task_logger=task_logger,
    )

    chain(metadata_1, fresh_source_data_available_1, extract_data_1, load_data_tg_1)
    chain(metadata_1, fresh_source_data_available_1, end_1)


extract_load_update_cc_parcel_sales_table()
