from pathlib import Path
import textwrap
from typing import List
import yaml

from sqlalchemy import inspect
from sqlalchemy.engine.base import Engine

from cc_utils.db import get_reflected_db_table


def format_jinja_variable_declaration_of_col_list(
    table_col_names: List[str], var_name: str
) -> List[str]:
    cols_str = '"' + '", "'.join(table_col_names) + '"'
    col_lines = textwrap.wrap(cols_str, width=96, break_long_words=False)
    col_lines = [f"    {line}" for line in col_lines]
    lines = [f"{{% set {var_name} = ["]
    lines.extend(col_lines)
    lines.append("] %}")
    return lines


def get_table_sqlalchemy_col_objects(table_name: str, schema_name: str, engine: Engine) -> List:
    insp = inspect(engine)
    schema_tables = insp.get_table_names(schema=schema_name)
    if f"temp_{table_name}" in schema_tables:
        ref_table = get_reflected_db_table(
            engine=engine, table_name=f"temp_{table_name}", schema_name=schema_name
        )
    elif table_name in schema_tables:
        ref_table = get_reflected_db_table(
            engine=engine, table_name=table_name, schema_name=schema_name
        )
    else:
        raise Exception(f"Table {table_name} not present in schema {schema_name}. Can't mock up.")
    table_cols = ref_table.columns.values()
    return table_cols


def format_dbt_stub_for_data_raw_stage(table_name: str, engine: Engine) -> List[str]:
    table_cols = get_table_sqlalchemy_col_objects(
        table_name=table_name, schema_name="data_raw", engine=engine
    )
    table_col_names = [col.name for col in table_cols]
    metadata_cols = ["source_data_updated", "ingestion_check_time"]
    source_cols = [col for col in table_col_names if col not in metadata_cols]
    format_jinja_variable_declaration_of_col_list(
        table_col_names=source_cols, var_name="source_cols"
    )
    if table_name.startswith("temp_"):
        table_name = table_name[5:]
    file_lines = [
        "{{ config(materialized='table') }}",
    ]
    file_lines.extend(
        format_jinja_variable_declaration_of_col_list(
            table_col_names=source_cols, var_name="source_cols"
        )
    )
    file_lines.extend(
        [
            """{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}""",
            "",
            "-- selecting all records already in the full data_raw table",
            "WITH records_in_data_raw_table AS (",
            "    SELECT *, 1 AS retention_priority",
            f"""    FROM {{{{ source('staging', '{table_name}') }}}}""",
            "),",
            "",
            """-- selecting all distinct records from the latest data pull (in the "temp" table)""",
            "current_pull_with_distinct_combos_numbered AS (",
            "    SELECT *,",
            "        row_number() over(partition by",
            "            {% for sc in source_cols %}{{ sc }},{% endfor %}",
            """            {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}""",
            "        ) as rn",
            f"""    FROM {{{{ source('staging', 'temp_{table_name}') }}}}""",
            "),",
            "distinct_records_in_current_pull AS (",
            "    SELECT",
            "        {% for sc in source_cols %}{{ sc }},{% endfor %}",
            "        {% for mc in metadata_cols %}{{ mc }},{% endfor %}",
            "        2 AS retention_priority",
            "    FROM current_pull_with_distinct_combos_numbered",
            "    WHERE rn = 1",
            "),",
            "",
            "-- stacking the existing data with all distinct records from the latest pull",
            "data_raw_table_with_all_new_and_updated_records AS (",
            "    SELECT *",
            "    FROM records_in_data_raw_table",
            "        UNION ALL",
            "    SELECT *",
            "    FROM distinct_records_in_current_pull",
            "),",
            "",
            "-- selecting records that where source columns are distinct (keeping the earlier recovery",
            "--  when there are duplicates to chose from)",
            "data_raw_table_with_new_and_updated_records AS (",
            "    SELECT *,",
            "    row_number() over(partition by",
            """        {% for sc in source_cols %}{{ sc }}{{ "," if not loop.last }}{% endfor %}""",
            "        ORDER BY retention_priority",
            "        ) as rn",
            "    FROM data_raw_table_with_all_new_and_updated_records",
            "),",
            "distinct_records_for_data_raw_table AS (",
            "    SELECT",
            "        {% for sc in source_cols %}{{ sc }},{% endfor %}",
            """        {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}""",
            "    FROM data_raw_table_with_new_and_updated_records",
            "    WHERE rn = 1",
            ")",
            "",
            "SELECT *",
            "FROM distinct_records_for_data_raw_table",
        ]
    )
    return file_lines


def write_lines_to_file(file_lines: List[str], file_path: Path, line_sep: str = "\n") -> None:
    if not isinstance(file_path, Path):
        file_path = Path(file_path)
    if not file_path.is_file():
        if not (all(line.endswith(line_sep) for line in file_lines)):
            file_lines = [f"{line}{line_sep}" for line in file_lines]
        with open(file_path, "x") as f:
            f.writelines(file_lines)
    else:
        raise Exception(
            f"There's already a file at {file_path}. Remove it and rerun if you aim to replace it."
        )


def update_sources_yml(table_name: str) -> None:
    """Updates the dbt staging sources.yml file when adding a data set to the warehouse.

    I'm not sure if this func belongs in this module; may move in the future.
    """

    class MySafeDumper(yaml.SafeDumper):
        """Enables nicer yaml formatting, but not necessary"""

        def increase_indent(self, flow=False, *args, **kwargs):
            return super().increase_indent(flow=flow, indentless=False)

    sources_path = Path("/opt/airflow/dbt/models/staging/sources.yml")
    if sources_path.is_file():
        with open(sources_path, "r") as sf:
            sources_list = yaml.safe_load(sf)

        source_tables = sources_list["sources"][0]["tables"]
        init_n_tables = len(source_tables)
        if all(st["name"] != table_name for st in source_tables):
            source_tables.append({"name": table_name})
        if all(st["name"] != f"temp_{table_name}" for st in source_tables):
            source_tables.append({"name": f"temp_{table_name}"})
        if len(source_tables) > init_n_tables:
            sources_list["sources"][0]["tables"] = sorted(source_tables, key=lambda k: k["name"])
            with open(sources_path, "w") as sfw:
                yaml.dump(sources_list, sfw, sort_keys=False, indent=2, Dumper=MySafeDumper)


def make_dbt_data_raw_table_staging_model(table_name: str, engine: Engine) -> None:
    file_lines = format_dbt_stub_for_data_raw_stage(table_name=f"temp_{table_name}", engine=engine)
    file_path = Path(f"/opt/airflow/dbt/models/staging/{table_name}.sql")
    write_lines_to_file(file_lines=file_lines, file_path=file_path)
    update_sources_yml(table_name=table_name)
