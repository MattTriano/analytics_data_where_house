import datetime as dt
import os

from airflow.decorators import dag, task
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine

from airflow.providers.postgres.hooks.postgres import PostgresHook

def check_that_schema_exists(schema_name: str, conn_id: str = "dwh_db_conn") -> bool:
    pg_hook = PostgresHook(conn_id)
    rows = pg_hook.get_records(
        f"""
            SELECT * 
            FROM information_schema.schemata
            WHERE schema_name = '{schema_name}';"""
    )
    return len(rows) > 0

@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def ensure_metadata_table_exists():
    
    @task
    def check_for_metadata_schema(conn_id: str) -> bool:
        return check_that_schema_exists(conn_id=conn_id, schema_name="metadata")

    @task
    def create_metadata_schema(conn_id: str = "dwh_db_conn") -> bool:
        pg_hook = PostgresHook(conn_id)
        rows = pg_hook.get_records(
            """SELECT * FROM metadata.table_metadata;"""
        )
        return len(rows) > 0

    @task
    def check_for_metadata_table(conn_id: str = "dwh_db_conn") -> bool:
        pg_hook = PostgresHook(conn_id)
        rows = pg_hook.get_records(
            """SELECT * FROM metadata.table_metadata;"""
        )

        return len(rows) > 0

    
    # create_pet_table = PostgresOperator(
    #     task_id="check_for",
    #     sql="""
    #         CREATE TABLE IF NOT EXISTS pet (
    #         pet_id SERIAL PRIMARY KEY,
    #         name VARCHAR NOT NULL,
    #         pet_type VARCHAR NOT NULL,
    #         birth_date DATE NOT NULL,
    #         OWNER VARCHAR NOT NULL);
    #       """,
    # )

    metadata_schema_exists = check_for_metadata_schema()
    create_metadata_schema()

ensure_metadata_table_exists()