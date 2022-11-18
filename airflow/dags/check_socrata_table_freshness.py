import datetime as dt
import logging
from typing import Tuple

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy.engine.base import Engine

from utils.db import get_pg_engine
from utils.socrata import SocrataTable, SocrataTableMetadata

task_logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "dwh_db_conn"

chicago_cta_stations_table = SocrataTable(table_id="8pix-ypme", table_name="chicago_cta_stations")


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def check_table_freshness():
    @task
    def get_socrata_table_metadata(socrata_table: SocrataTable) -> SocrataTableMetadata:
        socrata_metadata = SocrataTableMetadata(socrata_table=socrata_table)
        task_logger.info(
            f"Retrieved metadata for socrata table {socrata_metadata.table_name} and table_id",
            f" {socrata_metadata.table_id}.",
        )
        return socrata_metadata

    @task
    def get_table_freshness_check_record(
        socrata_metadata: SocrataTableMetadata, conn_id: str
    ) -> Tuple:
        engine = get_pg_engine(conn_id=conn_id)
        insertable_record = socrata_metadata.get_insertable_check_table_metadata_record(
            engine=engine
        )
        task_logger.info(
            f"Produced insertable record of freshness check results. Record: {insertable_record}"
        )
        return insertable_record

    socrata_table_metadata_1 = get_socrata_table_metadata(socrata_table=chicago_cta_stations_table)
    table_freshness_check_record_1 = get_table_freshness_check_record(
        socrata_metadata=socrata_table_metadata_1, conn_id=POSTGRES_CONN_ID
    )
    ingest_table_freshness_check_metadata_1 = PostgresOperator(
        task_id="ingest_table_freshness_check_metadata",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            INSERT INTO metadata.table_metadata
            VALUES {table_freshness_check_record_1};
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        socrata_table_metadata_1
        >> table_freshness_check_record_1
        >> ingest_table_freshness_check_metadata_1
    )


check_table_freshness_dag = check_table_freshness()
