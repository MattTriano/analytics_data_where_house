import datetime as dt
import logging

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import (
    get_pg_engine,
    database_has_schema,
    execute_structural_command,
)


task_logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "dwh_db_conn"
SCHEMA_NAME = "data_raw"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["metadata"],
)
def ensure_data_raw_schema_exists():
    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    def schema_exists(conn_id: str, schema_name: str) -> str:
        if not database_has_schema(engine=get_pg_engine(conn_id=conn_id), schema_name=schema_name):
            task_logger.info(
                f"Schema '{schema_name}' not in database. Branching to create_schema task."
            )
            return "create_schema"
        else:
            task_logger.info(f"Schema '{schema_name}' in database. Ending DAG.")
            return "end"

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def create_schema(conn_id: str, schema_name: str) -> None:
        task_logger.info(f"Creating '{schema_name}' schema")
        engine = get_pg_engine(conn_id=conn_id)
        try:
            username = engine.url.username
            execute_structural_command(
                query=f"""
                CREATE SCHEMA {schema_name};
                GRANT USAGE ON SCHEMA metadata TO {username};
                GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema_name} TO {username};
                ALTER DEFAULT PRIVILEGES FOR USER {username} IN SCHEMA {schema_name}
                    GRANT ALL ON TABLES TO {username};
                """,
                engine=engine,
            )
        except Exception as e:
            print(f"Failed to create '{schema_name}' schema. Error: {e}, {type(e)}")

    schema_exists_1 = schema_exists(conn_id=POSTGRES_CONN_ID, schema_name=SCHEMA_NAME)
    create_schema_1 = create_schema(conn_id=POSTGRES_CONN_ID, schema_name=SCHEMA_NAME)
    end_1 = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    schema_exists_1 >> [create_schema_1, end_1]


check_metadata_schema_dag = ensure_data_raw_schema_exists()
