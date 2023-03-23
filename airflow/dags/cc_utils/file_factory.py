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


def get_ordered_table_cols_from__standardized_model(std_file_path: Path) -> List[str]:
    if std_file_path.is_file():
        with open(std_file_path, "r") as f:
            std_file_lines = f.readlines()
    else:
        raise Exception(f"No file found at the expected location, {std_file_path}.")

    record_index = [
        std_file_lines.index(el) for el in std_file_lines if el.startswith("{% set record_id")
    ][0]
    record_id = std_file_lines[record_index].split('"')[1]
    table_cols = [record_id]

    select_index = std_file_lines.index("    SELECT\n") + 2
    end_index = [std_file_lines.index(el) for el in std_file_lines if el.startswith("    FROM")][0]
    col_containing_lines = std_file_lines[select_index:end_index]
    table_cols.extend(
        [
            line.split(" AS ")[1].split(",")[0].replace("\n", "")
            for line in col_containing_lines
            if " AS " in line
        ]
    )
    print(col_containing_lines)
    return table_cols


def get_composite_key_cols_definition_line_from__standardized_model(
    std_file_path: Path,
) -> str:
    with open(std_file_path, "r") as f:
        std_file_lines = f.readlines()
    ck_cols_el = [ln.replace("\n", "") for ln in std_file_lines if ln.startswith("{% set ck_cols")][
        0
    ]
    return ck_cols_el


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


def format_dbt_stub_for_standardized_stage(table_name: str, engine: Engine) -> List[str]:
    table_cols = get_table_sqlalchemy_col_objects(
        table_name=table_name, schema_name="data_raw", engine=engine
    )
    table_col_details = [{"name": col.name, "type": col.type} for col in table_cols]
    file_lines = [
        """{{ config(materialized='view') }}""",
        """{% set ck_cols = ["REPLACE_WITH_COMPOSITE_KEY_COLUMNS"] %}""",
        """{% set record_id = "REPLACE_WITH_BETTER_id" %}""",
        "",
        "WITH records_with_basic_cleaning AS (",
        "    SELECT",
        "        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},",
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

    return file_lines


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
        return f"        {col_name}::MANUALLY_REPLACE (was {str(sqlalch_col_type)}) AS {col_name},"


def format_dbt_stub_for_clean_stage(table_name: str) -> List[str]:
    std_file_path = Path(f"/opt/airflow/dbt/models/standardized/{table_name}_standardized.sql")
    table_cols = get_ordered_table_cols_from__standardized_model(std_file_path=std_file_path)
    record_col_el = table_cols[0]
    ck_cols_el = get_composite_key_cols_definition_line_from__standardized_model(
        std_file_path=std_file_path
    )
    file_lines = [
        "{{ config(materialized='view') }}",
        ck_cols_el,
        f"""{{% set record_id = "{record_col_el}" %}}""",
    ]
    table_col_lines = format_jinja_variable_declaration_of_col_list(
        table_col_names=table_cols, var_name="base_cols"
    )
    file_lines.extend(table_col_lines)
    cte_lines = [
        "",
        "-- selects all records from the standardized view of this data",
        "WITH std_data AS (",
        "    SELECT *",
        f"""    FROM {{{{ ref('{table_name}_standardized') }}}}""",
        "),",
        "",
        "-- keeps the most recently updated version of each record ",
        "std_records_numbered_latest_first AS (",
        "    SELECT *,",
        "        row_number() over(partition by {{record_id}} ORDER BY source_data_updated DESC) as rn",
        "    FROM std_data",
        "),",
        "most_current_records AS (",
        "    SELECT *",
        "    FROM std_records_numbered_latest_first",
        "    WHERE rn = 1",
        "),",
        "",
        "-- selects the source_data_updated (ie the date of publication) value from each record's",
        "--   first ingestion into the local data warehouse",
        "std_records_numbered_earliest_first AS (",
        "    SELECT *,",
        "        row_number() over(partition by {{record_id}} ORDER BY source_data_updated ASC) as rn",
        "FROM std_data",
        "),",
        "records_first_ingested_pub_date AS (",
        "    SELECT {{record_id}}, source_data_updated AS first_ingested_pub_date",
        "    FROM std_records_numbered_earliest_first",
        "    WHERE rn = 1",
        ")",
        "",
        "SELECT",
        "    {% for bc in base_cols %}mcr.{{ bc }},{% endfor %}",
        "    fi.first_ingested_pub_date",
        "FROM most_current_records AS mcr",
        "LEFT JOIN records_first_ingested_pub_date AS fi",
        "ON mcr.{{ record_id }} = fi.{{ record_id }}",
        "ORDER BY {% for ck in ck_cols %}mcr.{{ ck }} DESC, {% endfor %} mcr.source_data_updated DESC",
    ]
    file_lines.extend(cte_lines)
    return file_lines
