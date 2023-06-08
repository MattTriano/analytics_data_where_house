import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task
from sqlalchemy import insert

from cc_utils.census.core import get_dataset_metadata_catalog
from cc_utils.db import (
    get_pg_engine,
    get_reflected_db_table,
    execute_result_returning_orm_query,
)


task_logger = logging.getLogger("airflow.task")
POSTGRES_CONN_ID = "dwh_db_conn"


@task
def request_and_ingest_dataset_metadata_catalog(conn_id: str, task_logger: Logger) -> str:
    catalog_df = get_dataset_metadata_catalog(dataset_base_url="https://api.census.gov/data.json")
    catalog_df["time_of_check"] = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    task_logger.info(f"Datasets in Census API Dataset Metadata Catalog: {len(catalog_df)}")
    engine = get_pg_engine(conn_id=conn_id)
    api_dataset_metadata_table = get_reflected_db_table(
        engine=engine, table_name="census_api_dataset_metadata", schema_name="metadata"
    )
    insert_statement = (
        insert(api_dataset_metadata_table)
        .values(catalog_df.to_dict(orient="records"))
        .returning(api_dataset_metadata_table)
    )
    ingested_catalog_df = execute_result_returning_orm_query(
        engine=engine, select_query=insert_statement
    )
    task_logger.info(f"Max dataset_id value after ingestion: {ingested_catalog_df['id'].max()}")
    return "success"


@dag(
    schedule="42 4 1,15 * *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["census", "metadata"],
)
def update_census_api_dataset_metadata_catalog():
    update_catalog = request_and_ingest_dataset_metadata_catalog(
        conn_id=POSTGRES_CONN_ID, task_logger=task_logger
    )

    update_catalog


update_census_api_dataset_metadata_catalog()
