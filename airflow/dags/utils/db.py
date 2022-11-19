from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from geoalchemy2 import Geometry, Geography  # Necessary for reflection of cols with spatial dtypes
import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema, MetaData, Table
from sqlalchemy.sql.selectable import Select


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


def get_reflected_db_table(engine: Engine, table_name: str, schema_name: str) -> Table:
    try:
        metadata_obj = MetaData(schema=schema_name)
        metadata_obj.reflect(bind=engine)
        full_table_name = f"{schema_name}.{table_name}"
        if full_table_name in metadata_obj.tables.keys():
            return metadata_obj.tables[full_table_name]
        else:
            raise NoSuchTableError(f"Table {table_name} not found in schema {schema_name}.")
    except Exception as err:
        print(f"Error while attempting table reflection. {err}, error type: {type(err)}")


def execute_result_returning_orm_query(
    engine: Engine, select_query: Select, limit_n: int = None
) -> pd.DataFrame:
    try:
        with Session(engine) as session:
            if limit_n is None:
                query_result = session.execute(select_query).all()
            else:
                query_result = session.execute(select_query).fetchmany(size=limit_n)
        return pd.DataFrame(query_result)
    except Exception as err:
        print(f"Couldn't execute the given ORM-style query: {err}, type: {type(err)}")
