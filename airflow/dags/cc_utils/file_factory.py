import re
import textwrap
from pathlib import Path

import yaml
from sqlalchemy import inspect
from sqlalchemy.engine.base import Engine

from cc_utils.db import get_reflected_db_table


def format_jinja_variable_declaration_of_col_list(
    table_col_names: list[str], var_name: str
) -> list[str]:
    cols_str = '"' + '", "'.join(table_col_names) + '"'
    col_lines = textwrap.wrap(cols_str, width=96, break_long_words=False)
    col_lines = [f"    {line}" for line in col_lines]
    lines = [f"{{% set {var_name} = ["]
    lines.extend(col_lines)
    lines.append("] %}")
    return lines


def get_table_sqlalchemy_col_objects(table_name: str, schema_name: str, engine: Engine) -> list:
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


def get_composite_key_cols_definition_line_from__standardized_model(
    std_file_path: Path,
) -> str:
    with open(std_file_path, "r") as f:
        std_file_lines = f.readlines()
    ck_cols_start_ind = [
        i for i, line in enumerate(std_file_lines) if line.startswith("{% set ck_cols")
    ][0]
    if not std_file_lines[ck_cols_start_ind].endswith("%}\n"):
        ck_cols_end_ind = [i for i, line in enumerate(std_file_lines) if line.endswith("%}\n")][
            0
        ] + 1
    else:
        ck_cols_end_ind = ck_cols_start_ind + 1
    ck_cols_el = "".join(
        [el.replace("\n", "") for el in std_file_lines[ck_cols_start_ind:ck_cols_end_ind]]
    )
    ck_cols_el = " ".join(ck_cols_el.split())
    return ck_cols_el


def get_composite_key_cols(std_file_path: Path) -> list[str]:
    ck_cols_el = get_composite_key_cols_definition_line_from__standardized_model(std_file_path)
    ck_cols = re.findall("\\[(.*?)\\]", "".join(ck_cols_el))[0]
    ck_cols = [el.strip().replace('"', "") for el in ck_cols.split(",")]
    return ck_cols


def get_ordered_table_cols_from__standardized_model(std_file_path: Path) -> list[str]:
    if std_file_path.is_file():
        with open(std_file_path, "r") as f:
            std_file_lines = f.readlines()
    else:
        raise Exception(f"No file found at the expected location, {std_file_path}.")
    ck_cols = get_composite_key_cols(std_file_path=std_file_path)
    record_index = [
        std_file_lines.index(el) for el in std_file_lines if el.startswith("{% set record_id")
    ][0]
    record_id = std_file_lines[record_index].split('"')[1]
    table_cols = [record_id]
    if len(ck_cols) == 1:
        select_index = std_file_lines.index("    SELECT\n") + 2
    else:
        select_index = std_file_lines.index("    SELECT\n") + 1
    end_index = [std_file_lines.index(el) for el in std_file_lines if el.startswith("    FROM")][0]
    col_containing_lines = std_file_lines[select_index:end_index]
    table_cols.extend(
        [
            line.split(" AS ")[1].split(",")[0].replace("\n", "")
            for line in col_containing_lines
            if " AS " in line
        ]
    )
    return table_cols


def format_dbt_stub_for_data_raw_stage(dataset_name: str, engine: Engine) -> list[str]:
    table_cols = get_table_sqlalchemy_col_objects(
        table_name=dataset_name, schema_name="data_raw", engine=engine
    )
    table_col_names = [col.name for col in table_cols]
    metadata_cols = ["source_data_updated", "ingestion_check_time"]
    metadata_cols_str = '["' + '", "'.join(metadata_cols) + '"]'
    source_cols = [col for col in table_col_names if col not in metadata_cols]
    format_jinja_variable_declaration_of_col_list(
        table_col_names=source_cols, var_name="source_cols"
    )
    if dataset_name.startswith("temp_"):
        dataset_name = dataset_name[5:]
    file_lines = [
        f"""{{% set dataset_name = "{dataset_name}" %}}""",
        *format_jinja_variable_declaration_of_col_list(
            table_col_names=source_cols, var_name="source_cols"
        ),
        f"""{{% set metadata_cols = {metadata_cols_str} %}}""",
        "",
        "{% set query = get_and_add_new_and_updated_records_to_data_raw(",
        """    dataset_name=dataset_name,""",
        """    source_cols=source_cols,""",
        """    metadata_cols=metadata_cols""",
        """) %}""",
        "",
        """{{- query -}}""",
    ]
    return file_lines


def write_lines_to_file(file_lines: list[str], file_path: Path, line_sep: str = "\n") -> None:
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


def update_sources_yml(dataset_name: str) -> None:
    """Updates the dbt data_raw sources.yml file when adding a data set to the warehouse.

    I'm not sure if this func belongs in this module; may move in the future.
    """

    class MySafeDumper(yaml.SafeDumper):
        """Enables nicer yaml formatting, but not necessary"""

        def increase_indent(self, flow=False, *args, **kwargs):
            return super().increase_indent(flow=flow, indentless=False)

    sources_path = Path("/opt/airflow/dbt/models/data_raw/sources.yml")
    if sources_path.is_file():
        with open(sources_path, "r") as sf:
            sources_list = yaml.safe_load(sf)

        source_tables = sources_list["sources"][0]["tables"]
        init_n_tables = len(source_tables)
        if all(st["name"] != dataset_name for st in source_tables):
            source_tables.append({"name": dataset_name})
        if all(st["name"] != f"temp_{dataset_name}" for st in source_tables):
            source_tables.append({"name": f"temp_{dataset_name}"})
        if len(source_tables) > init_n_tables:
            sources_list["sources"][0]["tables"] = sorted(source_tables, key=lambda k: k["name"])
            with open(sources_path, "w") as sfw:
                yaml.dump(sources_list, sfw, sort_keys=False, indent=2, Dumper=MySafeDumper)


def make_dbt_data_raw_model_file(dataset_name: str, engine: Engine) -> None:
    file_lines = format_dbt_stub_for_data_raw_stage(dataset_name=dataset_name, engine=engine)
    file_path = Path(f"/opt/airflow/dbt/models/data_raw/{dataset_name}.sql")
    write_lines_to_file(file_lines=file_lines, file_path=file_path)
    update_sources_yml(dataset_name=dataset_name)


def format_dbt_stub_for_standardized_stage(
    table_name: str, engine: Engine, default_tz: str = "America/Chicago"
) -> list[str]:
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
    ]

    col_lines = []
    for table_col_deet in table_col_details:
        col_type = table_col_deet["type"]
        col_name = table_col_deet["name"]
        if col_name == "source_data_updated":
            col_lines.append(f"        {col_name}::timestamptz")
            col_lines.append(
                f"            AT TIME ZONE 'UTC' AT TIME ZONE '{default_tz}' AS {col_name},"
            )
        elif col_name == "ingestion_check_time":
            col_lines.append(f"        {col_name}::timestamptz")
            col_lines.append(
                f"            AT TIME ZONE 'UTC' AT TIME ZONE '{default_tz}' AS {col_name}"
            )
        else:
            col_lines.append(col_type_cast_formatter(col_name=col_name, sqlalch_col_type=col_type))
    file_lines.extend(col_lines)
    file_lines.extend(
        [
            f"""    FROM {{{{ ref('{table_name}') }}}}""",
            """    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}""",  # noqa: E501
            ")",
            "",
            "",
            "SELECT",
            "    {% if ck_cols|length > 1 %}",
            "        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},",
            "    {% endif %}",
            "    a.*",
            "FROM records_with_basic_cleaning AS a",
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


def format_dbt_stub_for_clean_stage(table_name: str) -> list[str]:
    std_file_path = Path(f"/opt/airflow/dbt/models/standardized/{table_name}_standardized.sql")
    table_cols = get_ordered_table_cols_from__standardized_model(std_file_path=std_file_path)
    record_col_el = table_cols[0]
    ck_cols_el = get_composite_key_cols_definition_line_from__standardized_model(
        std_file_path=std_file_path
    )
    table_col_lines = format_jinja_variable_declaration_of_col_list(
        table_col_names=table_cols, var_name="base_cols"
    )
    file_lines = [
        f"""{{% set dataset_name = "{table_name}" %}}""",
        ck_cols_el,
        f"""{{% set record_id = "{record_col_el}" %}}""",
        *table_col_lines,
        """{{% set updated_at_col = "source_data_updated" %}}""",
    ]
    cte_lines = [
        "",
        "{% set query = generate_clean_stage_incremental_dedupe_query(",
        "    dataset_name=dataset_name,",
        "    record_id=record_id,",
        "    ck_cols=ck_cols,",
        "    base_cols=base_cols,",
        "    updated_at_col=updated_at_col",
        ") %}",
        "",
        "{{ query }}",
    ]
    file_lines.extend(cte_lines)
    return file_lines
