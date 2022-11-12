import datetime as dt
from typing import Dict

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy.engine.base import Engine

from utils.db import database_has_schema, execute_structural_command


def check_that_schema_exists(schema_name: str, conn_id: str = "dwh_db_conn") -> bool:
    pg_hook = PostgresHook(conn_id)
    rows = pg_hook.get_records(
        f"""
            SELECT * 
            FROM information_schema.schemata
            WHERE schema_name = '{schema_name}';"""
    )
    return len(rows) > 0


def get_pg_engine(conn_id: str) -> Dict:
    try:
        pg_hook = PostgresHook(conn_id)
        engine = pg_hook.get_sqlalchemy_engine()
        return {"engine": engine}
    except Exception as e:
        print(f"Failed to generate engine to pg db using conn_id {conn_id}. Error: {e}, {type(e)}")


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def ensure_metadata_table_exists():
    @task
    def get_dwh_db_engine(conn_id: str = "dwh_db_conn") -> Dict:
        return get_pg_engine(conn_id=conn_id)

    @task
    def db_has_metadata_schema(engine: Engine) -> bool:
        return database_has_schema(engine=engine, schema_name="metadata")

    @task
    def create_metadata_schema(engine: Engine) -> None:
        schema_name = "metadata"
        try:
            username = engine.url.username
            execute_structural_command(
                query=f"""
                    GRANT USAGE ON SCHEMA {schema_name} TO {username};
                    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema_name} TO {username};
                    ALTER DEFAULT PRIVILEGES FOR USER {username} IN SCHEMA {schema_name}
                        GRANT ALL ON TABLES TO {username};
                """,
                engine=engine,
            )
        except Exception as e:
            print(f"Failed to create metadata schema. Error: {e}, {type(e)}")

    # @task
    # def check_for_metadata_table(conn_id: str = "dwh_db_conn") -> bool:
    #     pg_hook = PostgresHook(conn_id)
    #     rows = pg_hook.get_records(
    #         """SELECT * FROM metadata.table_metadata;"""
    #     )
    #     return len(rows) > 0

    engine_dict = get_dwh_db_engine(conn_id="dwh_db_conn")
    metadata_schema_exists = db_has_metadata_schema(engine=engine_dict["engine"])
    if not metadata_schema_exists:
        create_metadata_schema(engine=engine_dict["engine"])


ensure_metadata_table_exists()
