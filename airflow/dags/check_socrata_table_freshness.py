import datetime as dt
import logging
from pathlib import Path
from typing import Tuple
from urllib.request import urlretrieve

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label
import pandas as pd
import geopandas as gpd

from utils.db import get_pg_engine, get_data_table_names_in_schema
from utils.socrata import SocrataTable, SocrataTableMetadata

task_logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "dwh_db_conn"

chicago_cta_stations_table = SocrataTable(table_id="8pix-ypme", table_name="chicago_cta_stations")


def get_local_file_path(socrata_metadata: SocrataTableMetadata) -> Path:
    output_dir = Path("data_raw").resolve()
    output_dir.mkdir(exist_ok=True)
    local_file_path = output_dir.joinpath(socrata_metadata.format_file_name())
    return local_file_path


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def check_table_freshness():
    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

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
        socrata_metadata.check_warehouse_data_freshness(engine=engine)
        task_logger.info(
            f"Extracted table freshness information.",
            f"Fresh source data available: {socrata_metadata.data_freshness_check['updated_data_available']}",
            f"Fresh source metadata available: {socrata_metadata.data_freshness_check['updated_metadata_available']}",
        )
        return socrata_metadata

    @task
    def ingest_table_freshness_check_metadata(
        socrata_metadata: SocrataTableMetadata, conn_id: str
    ) -> None:
        engine = get_pg_engine(conn_id=conn_id)
        socrata_metadata.insert_current_freshness_check_to_db(engine=engine)
        task_logger.info(
            f"Ingested table freshness check results into metadata table.  ",
            f"Freshness check id: {socrata_metadata.freshness_check_id}",
        )

    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    def fresher_source_data_available(
        socrata_metadata: SocrataTableMetadata,
    ) -> str:
        if socrata_metadata.data_freshness_check["updated_data_available"]:
            task_logger.info(f"Fresh data available, entering extract-load branch")
            return "extract_load_task_group"
        else:
            return "end"

    @task_group(group_id="EL_task_group")
    def extract_load_task_group(socrata_metadata: SocrataTableMetadata, conn_id: str) -> None:
        @task
        def download_fresh_data(socrata_metadata: SocrataTableMetadata) -> SocrataTableMetadata:
            output_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
            urlretrieve(url=socrata_metadata.get_data_download_url(), filename=output_file_path)
            return socrata_metadata

        @task.branch(trigger_rule=TriggerRule.ALL_DONE)
        def table_exists_in_warehouse(
            socrata_metadata: SocrataTableMetadata,
            conn_id: str,
        ) -> str:
            tables_in_data_raw_schema = get_data_table_names_in_schema(
                engine=get_pg_engine(conn_id=conn_id), schema_name="data_raw"
            )
            if socrata_metadata.table_name not in tables_in_data_raw_schema:
                return "ingest_straight_into_table_in_data_raw"
            else:
                return "ingest_into_temporary_table"

        @task
        def ingest_straight_into_table_in_data_raw(
            socrata_metadata: SocrataTableMetadata, conn_id: str
        ) -> None:
            local_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
            if socrata_metadata.is_geospatial:
                gdf = gpd.read_file(local_file_path)
                gdf.to_postgis(
                    name=socrata_metadata.table_name,
                    schema="data_raw",
                    con=get_pg_engine(conn_id=conn_id),
                )
            else:
                df = pd.read_csv(local_file_path)
                df.to_sql(
                    name=socrata_metadata.table_name,
                    schema="data_raw",
                    con=get_pg_engine(conn_id=conn_id),
                )

        @task
        def ingest_into_temporary_table(
            socrata_metadata: SocrataTableMetadata, conn_id: str
        ) -> None:
            local_file_path = get_local_file_path(socrata_metadata=socrata_metadata)
            if socrata_metadata.is_geospatial:
                gdf = gpd.read_file(local_file_path)
                gdf["ingestion_check_time"] = socrata_metadata.time_of_check
                gdf.to_postgis(
                    name=f"temp_{socrata_metadata.table_name}",
                    schema="data_raw",
                    con=get_pg_engine(conn_id=conn_id),
                )
            else:
                df = pd.read_csv(local_file_path)
                df["ingestion_check_time"] = socrata_metadata.time_of_check
                df.to_sql(
                    name=f"temp_{socrata_metadata.table_name}",
                    schema="data_raw",
                    con=get_pg_engine(conn_id=conn_id),
                )

        socrata_metadata_1 = download_fresh_data(socrata_metadata=socrata_metadata)

        table_exists_1 = table_exists_in_warehouse(
            socrata_metadata=socrata_metadata_1, conn_id=conn_id
        )
        ingest_into_data_raw_table_1 = ingest_straight_into_table_in_data_raw(
            socrata_metadata=socrata_metadata_1, conn_id=conn_id
        )
        ingest_into_temporary_table_1 = ingest_into_temporary_table(
            socrata_metadata=socrata_metadata_1, conn_id=conn_id
        )

        ingest_new_records_to_raw_1 = EmptyOperator(
            task_id="ingest_new_records_to_raw", trigger_rule=TriggerRule.ALL_DONE
        )

        socrata_metadata_1 >> table_exists_1 >> ingest_into_data_raw_table_1
        (
            socrata_metadata_1
            >> table_exists_1
            >> ingest_into_temporary_table_1
            >> ingest_new_records_to_raw_1
        )

    socrata_table_metadata_1 = get_socrata_table_metadata(socrata_table=chicago_cta_stations_table)
    socrata_table_metadata_2 = extract_table_freshness_info(
        socrata_metadata=socrata_table_metadata_1, conn_id=POSTGRES_CONN_ID
    )
    ingest_freshness_metadata_1 = ingest_table_freshness_check_metadata(
        socrata_metadata=socrata_table_metadata_2, conn_id=POSTGRES_CONN_ID
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        socrata_metadata=socrata_table_metadata_2
    )
    # fresh_source_data_available_1 = download_fresh_data(socrata_metadata=fresh_source_data_available_1)
    extract_load_task_group_1 = extract_load_task_group(
        socrata_metadata=socrata_table_metadata_2, conn_id=POSTGRES_CONN_ID
    )

    update_freshness_check_in_db_1 = EmptyOperator(
        task_id="update_freshness_check_in_db", trigger_rule=TriggerRule.ALL_DONE
    )

    (
        socrata_table_metadata_1
        >> socrata_table_metadata_2
        >> ingest_freshness_metadata_1
        >> fresh_source_data_available_1
        >> end_1
    )
    (
        socrata_table_metadata_1
        >> socrata_table_metadata_2
        >> ingest_freshness_metadata_1
        >> fresh_source_data_available_1
        >> extract_load_task_group_1
        >> update_freshness_check_in_db_1
    )


check_table_freshness_dag = check_table_freshness()
