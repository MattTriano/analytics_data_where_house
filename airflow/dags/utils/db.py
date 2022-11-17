from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.schema import CreateSchema
from sqlalchemy.engine.base import Engine
from sqlalchemy.dialects.postgresql import insert


def get_pg_engine(conn_id: str) -> Engine:
    try:
        pg_hook = PostgresHook(conn_id)
        engine = pg_hook.get_sqlalchemy_engine()
        return engine
    except Exception as e:
        print(f"Failed to generate engine to pg db using conn_id {conn_id}. Error: {e}, {type(e)}")


def get_data_schema_names(engine: Engine) -> List:
    insp = inspect(engine)
    return insp.get_schema_names()


def database_has_schema(engine: Engine, schema_name: str) -> bool:
    with engine.connect() as conn:
        return engine.dialect.has_schema(connection=conn, schema=schema_name)


def get_data_table_names_in_schema(engine: Engine, schema_name: str) -> List:
    insp = inspect(engine)
    return insp.get_table_names(schema=schema_name)


def create_database_schema(engine: Engine, schema_name: str, verbose: bool = False) -> None:
    if not database_has_schema(engine=engine, schema_name=schema_name):
        with engine.connect() as conn:
            conn.execute(CreateSchema(name=schema_name))
            conn.commit()
            if verbose:
                print(f"Database schema '{schema_name}' successfully created.")
    else:
        if verbose:
            print(f"Database schema '{schema_name}' already exists.")


def execute_result_returning_query(query: str, engine: Engine) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(query))
        results_df = pd.DataFrame(result.fetchall(), columns=result.keys())
        if engine._is_future:
            conn.commit()
    return results_df


def execute_structural_command(query: str, engine: Engine) -> None:
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(query))


def to_sql_on_conflict_do_nothing(table, conn, keys, data_iter) -> None:
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(insert(table.table).on_conflict_do_nothing(), data)
