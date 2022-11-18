import datetime as dt
import logging
from typing import Tuple

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd

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
    def extract_table_freshness_info(
        socrata_metadata: SocrataTableMetadata, conn_id: str
    ) -> pd.DataFrame:
        engine = get_pg_engine(conn_id=conn_id)
        socrata_metadata.check_table_metadata(engine=engine)
        task_logger.info(
            f"Extracted table freshness information.",
            f"Fresh source data available: {socrata_metadata.table_check_metadata['updated_data_available']}",
            f"Fresh source metadata available: {socrata_metadata.table_check_metadata['updated_metadata_available']}",
        )
        return socrata_metadata

    @task
    def ingest_table_freshness_check_metadata(socrata_metadata: SocrataTableMetadata, conn_id: str):
        engine = get_pg_engine(conn_id=conn_id)
        insertable_record_df = socrata_metadata.get_insertable_check_table_metadata_record(
            engine=engine, output_type="DataFrame"
        )
        task_logger.info(f"type(insertable_record_df): {type(insertable_record_df)}")
        insertable_record_df.to_sql(
            name="table_metadata",
            schema="metadata",
            con=engine,
            if_exists="append",
        )
        task_logger.info(
            f"Ingested table freshness check results into metadata table. Record: {insertable_record_df}"
        )

    socrata_table_metadata_1 = get_socrata_table_metadata(socrata_table=chicago_cta_stations_table)
    socrata_table_metadata_2 = extract_table_freshness_info(
        socrata_metadata=socrata_table_metadata_1, conn_id=POSTGRES_CONN_ID
    )
    ingest_freshness_metadata_1 = ingest_table_freshness_check_metadata(
        socrata_metadata=socrata_table_metadata_2, conn_id=POSTGRES_CONN_ID
    )

    (socrata_table_metadata_1 >> socrata_table_metadata_2 >> ingest_freshness_metadata_1)


check_table_freshness_dag = check_table_freshness()
