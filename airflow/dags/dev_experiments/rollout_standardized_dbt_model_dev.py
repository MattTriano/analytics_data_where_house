from copy import copy
import datetime as dt
from logging import Logger
import logging
from pathlib import Path
from typing import List

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from sqlalchemy.engine.base import Engine

from cc_utils.db import execute_result_returning_query, get_pg_engine
from cc_utils.socrata import SocrataTable
from cc_utils.file_factory import get_table_sqlalchemy_col_objects, write_lines_to_file
from sources.tables import CHICAGO_CTA_TRAIN_STATIONS as SOCRATA_TABLE

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


def col_type_cast_formatter(col_name: str, sqlalch_col_type) -> str:
    #     sqlalch_col_type: sqlalchemy.sql.sqltypes.*
    if str(sqlalch_col_type).upper() == "BIGINT":
        return f"        {col_name}::bigint AS {col_name},"
    elif str(sqlalch_col_type).upper() == "INTEGER":
        return f"        {col_name}::int AS {col_name},"
    elif str(sqlalch_col_type).upper() == "SMALLINT":
        return f"        {col_name}::smallint AS {col_name},"
    elif str(sqlalch_col_type).upper() == "BOOLEAN":
        return f"        {col_name}::boolean AS {col_name},"
    elif str(sqlalch_col_type).upper() == "TEXT":
        return f"        upper({col_name}::text) AS {col_name},"
    elif str(sqlalch_col_type).upper().startswith("VARCHAR"):
        return f"        upper({col_name}::varchar) AS {col_name},"
    elif str(sqlalch_col_type).upper().startswith("CHAR"):
        return f"        upper({col_name}::char) AS {col_name},"
    elif str(sqlalch_col_type).upper().startswith("GEOMETRY"):
        return f"        {col_name}::{str(sqlalch_col_type).upper()} AS {col_name},"
    elif str(sqlalch_col_type).upper().startswith("DOUBLE_PRECISION"):
        return f"        {col_name}::double precision AS {col_name},"
    elif str(sqlalch_col_type).upper() == "DATE":
        return f"        {col_name}::date AS {col_name},"
    else:
        return f"        {col_name}::MANUALLY_REPLACE (was {str(sqlalch_col_type)} AS {col_name},"


def format_dbt_stub_for_standardized_stage(
    table_name: str, engine: Engine, task_logger: Logger
) -> List[str]:
    table_cols = get_table_sqlalchemy_col_objects(
        table_name=table_name, schema_name="data_raw", engine=engine
    )
    table_col_details = [{"name": col.name, "type": col.type} for col in table_cols]
    file_lines = [
        "{{ config(materialized='view') }}",
        "{% set ck_cols = [",
        "        REPLACE_WITH_COMPOSITE_KEY_COLUMNS",
        "] %}",
        "",
        "WITH records_with_basic_cleaning AS (",
        "    SELECT",
        "        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS REPLACE_WITH_BETTER_id,",
    ]

    col_lines = []
    for table_col_deet in table_col_details:
        col_type = table_col_deet["type"]
        col_name = table_col_deet["name"]
        if col_name == "source_data_updated":
            col_lines.append(f"        {col_name}::timestamptz AS {col_name},")
        elif col_name == "ingestion_check_time":
            col_lines.append(f"        {col_name}::timestamptz AS {col_name}")
        else:
            col_lines.append(col_type_cast_formatter(col_name=col_name, sqlalch_col_type=col_type))
    file_lines.extend(col_lines)
    file_lines.extend(
        [
            f"""    FROM {{{{ ref('{table_name}') }}}}""",
            """    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}""",
            ")",
            "",
            "",
            "SELECT *",
            "FROM records_with_basic_cleaning",
            "ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated",
        ]
    )
    task_logger.info(f"file_lines for table {table_name}")
    for file_line in file_lines:
        task_logger.info(f"    {file_line}")
    return file_lines


@task(retries=1)
def make_dbt_standardized_model(table_name: str, conn_id: str, task_logger: Logger) -> None:

    engine = get_pg_engine(conn_id=conn_id)
    std_file_lines = format_dbt_stub_for_standardized_stage(
        table_name=f"temp_{table_name}", engine=engine, task_logger=task_logger
    )
    file_path = Path(f"/opt/airflow/dbt/models/intermediate/{table_name}_standardized.sql")
    write_lines_to_file(file_lines=std_file_lines, file_path=file_path)

    task_logger.info(f"Leaving make_dbt_standardized_model")


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["dev_experiment"],
)
def dev_rollout_standardized_dbt_model():

    make_standardized_stage_1 = make_dbt_standardized_model(
        table_name=SOCRATA_TABLE.table_name,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    make_standardized_stage_1


dev_rollout_standardized_dbt_model()
