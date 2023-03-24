from copy import copy
import datetime as dt
from logging import Logger
import logging
from pathlib import Path
import os
from typing import List

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from sqlalchemy.engine.base import Engine

from cc_utils.db import execute_result_returning_query, get_pg_engine
from cc_utils.socrata import SocrataTable
from cc_utils.file_factory import (
    get_table_sqlalchemy_col_objects,
    write_lines_to_file,
    col_type_cast_formatter,
    format_dbt_stub_for_standardized_stage,
)
from sources.tables import COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES as SOCRATA_TABLE

task_logger = logging.getLogger("airflow.task")


def generate_table_col_stats_df(full_table_name: str, engine: Engine) -> pd.DataFrame:
    ignore_cols = ["geometry", "source_data_updated", "ingestion_check_time"]

    col_names_df = execute_result_returning_query(
        query=f"SELECT * FROM {full_table_name} LIMIT 0", engine=engine
    )
    col_names = col_names_df.columns
    n_rows = execute_result_returning_query(
        query=f"SELECT COUNT(*) FROM {full_table_name}", engine=engine
    )["count"].values[0]

    col_names = [cn for cn in col_names if cn not in ignore_cols]

    n_rows = execute_result_returning_query(
        query=f"SELECT COUNT(*) FROM {full_table_name}", engine=engine
    )["count"].values[0]

    col_stats = []
    for col_name in col_names:
        unique_col_vals = {}
        unique_vals = execute_result_returning_query(
            query=f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT {col_name} FROM {full_table_name}
                ) AS temp;
            """,
            engine=engine,
        )
        unique_col_vals["col_name"] = col_name
        unique_col_vals["unique_count"] = unique_vals["count"].values[0]
        unique_col_vals["prop_unique"] = unique_col_vals["unique_count"] / n_rows
        col_stats.append(copy(unique_col_vals))
        if unique_col_vals["prop_unique"] == 1:
            break
    col_stats_df = pd.DataFrame(col_stats)
    col_stats_df["pct_unique"] = (100 * col_stats_df["unique_count"] / n_rows).round(2)
    col_stats_df = col_stats_df.sort_values(by="pct_unique", ascending=False, ignore_index=True)
    return col_stats_df


def get_columns_w_most_unique_vals(
    col_stats_df: pd.DataFrame, full_table_name: str, engine: Engine
) -> List:
    col_stats_df = col_stats_df.sort_values(by="pct_unique", ascending=False, ignore_index=True)
    ordered_col_names = list(col_stats_df["col_name"])
    most_unique_col = ordered_col_names[0]
    remaining_cols = ordered_col_names[1:]
    count_distinct_cols = ", ".join([f"COUNT(DISTINCT {col}) AS {col}" for col in remaining_cols])
    recs_w_dupe_col_val = execute_result_returning_query(
        query=f"""
            WITH dupe_records AS (
                SELECT *,
                row_number() over(partition by {most_unique_col}) as rn
                FROM {full_table_name}
            ),
            recs_w_dupe_val AS (
                SELECT {", ".join(remaining_cols)}
                FROM {full_table_name}
                WHERE {most_unique_col} IN (
                    SELECT {most_unique_col}
                    FROM dupe_records
                    WHERE rn > 1
                )
            ),
            distinct_vals_per_col AS (
                SELECT {count_distinct_cols}
                FROM recs_w_dupe_val
            )

            SELECT *
            FROM distinct_vals_per_col;
        """,
        engine=engine,
    )
    cols_with_most_unique_vals = list(recs_w_dupe_col_val.idxmax(axis="columns"))
    return cols_with_most_unique_vals


@task
def make_dbt_standardized_model(table_name: str, conn_id: str, task_logger: Logger) -> None:

    engine = get_pg_engine(conn_id=conn_id)
    std_file_lines = format_dbt_stub_for_standardized_stage(table_name=table_name, engine=engine)
    file_path = Path(f"/opt/airflow/dbt/models/intermediate/{table_name}_standardized.sql")
    write_lines_to_file(file_lines=std_file_lines, file_path=file_path)
    task_logger.info(f"file_lines for table {table_name}")
    for file_line in std_file_lines:
        task_logger.info(f"    {file_line}")

    task_logger.info(f"Leaving make_dbt_standardized_model")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def dbt_standardized_model_ready(table_name: str, task_logger: Logger, **kwargs) -> str:
    airflow_home = os.environ["AIRFLOW_HOME"]
    task_logger.info(f"airflow_home: {airflow_home}")
    task_logger.info(f"table_name:   {table_name}")
    file_path = Path(airflow_home).joinpath(
        "dbt", "models", "intermediate", f"{table_name}_standardized.sql"
    )
    host_file_path = str(file_path).replace(airflow_home, "/airflow")
    task_logger.info(f"file_path:         {file_path}")
    task_logger.info(f"host_file_path:    {host_file_path}")
    if file_path.is_file():
        with open(file_path, "r") as f:
            file_lines = f.readlines()
        for file_line in file_lines:
            if (
                "REPLACE_WITH_COMPOSITE_KEY_COLUMNS" in file_line
                or "REPLACE_WITH_BETTER_id" in file_line
            ):
                raise Exception(
                    f"Found unfinished stub for dbt _standardized model in "
                    + f"{host_file_path}. Please finish and try again"
                )
        task_logger.info(f"Found a _standardized stage dbt model that looks finished; Proceeding")
        return "endpoint"
    else:
        task_logger.info(f"No _standardized stage dbt model found.")
        task_logger.info(f"Creating a stub in loc: {host_file_path}")
        task_logger.info(f"Edit the stub before proceeding to generate _clean stage dbt models.")
        return "make_dbt_standardized_model"


@task
def endpoint(task_logger: Logger) -> None:
    task_logger.info("Ending run")
    return "end"


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["dev_experiment"],
)
def dev_rollout_standardized_dbt_model():

    std_model_exists_1 = dbt_standardized_model_ready(
        table_name=SOCRATA_TABLE.table_name,
        task_logger=task_logger,
    )
    make_standardized_stage_1 = make_dbt_standardized_model(
        table_name=SOCRATA_TABLE.table_name,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    endpoint_1 = endpoint(task_logger=task_logger)
    chain(std_model_exists_1, [make_standardized_stage_1, endpoint_1])


dev_rollout_standardized_dbt_model()
