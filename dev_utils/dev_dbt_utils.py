from pathlib import Path
import textwrap
from typing import List, Union
import yaml

import pandas as pd
from sqlalchemy import create_engine, inspect, text, MetaData, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine


def get_credential_from_env_file(env_file_path: Path, env_credential_name: str) -> str:
    env_file_path = Path(env_file_path).resolve()
    if env_file_path.is_file():
        with open(env_file_path, "r") as f:
            creds = yaml.load(f, Loader=yaml.SafeLoader)
            cred_dict = {
                cred[0]: cred[1] for cred in [cred.split("=") for cred in creds.split(" ")]
            }
        if env_credential_name in cred_dict.keys():
            return cred_dict[env_credential_name]
        else:
            raise Exception(f"No credential named '{env_credential_name}' found in the given file")
    else:
        raise Exception("No .env file found at the given path.")


def get_connection_url_from_env_file(env_file_path: Path) -> URL:
    return URL.create(
        drivername="postgresql+psycopg2",
        host="localhost",
        username=get_credential_from_env_file(env_file_path, "POSTGRES_USER"),
        database=get_credential_from_env_file(env_file_path, "POSTGRES_DB"),
        password=get_credential_from_env_file(env_file_path, "POSTGRES_PASSWORD"),
        port=5431,
    )


def get_engine_from_env_file(
    env_file_path: Path, use_sqlalchemy_v2: bool = True, echo: bool = True
) -> Engine:
    connection_url = get_connection_url_from_env_file(env_file_path=env_file_path)
    engine = create_engine(connection_url, future=use_sqlalchemy_v2, echo=echo)
    return engine


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
        raise


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
        return f"        {col_name}::{str(sqlalch_col_type).upper()}) AS {col_name},"
    elif str(sqlalch_col_type).upper().startswith("DOUBLE_PRECISION"):
        return f"        {col_name}::double precision AS {col_name},"
    elif str(sqlalch_col_type).upper() == "DATE":
        return f"        {col_name}::date AS {col_name},"
    else:
        return f"        {col_name}::MANUALLY_REPLACE (was {str(sqlalch_col_type)} AS {col_name},"


def format_dbt_stub_for_standardized_stage(table_name: str, engine: Engine) -> List[str]:
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
    return file_lines


def get_distinct_records_w_col_values(
    col: Union[str, List], full_table_name: str, engine: Engine, verbose=True
) -> int:
    if isinstance(col, list):
        col_str = ", ".join(col)
    else:
        col_str = col
    distinct_col_vals = execute_result_returning_query(
        engine=engine,
        query=f"""
            SELECT COUNT(*)
            FROM (
                SELECT
                    {col_str},
                    row_number() over(partition by {col_str}) as rn
                FROM {full_table_name}
            ) AS cv
            WHERE cv.rn = 1;
        """,
    )
    if verbose:
        print(f"Records with distinct ({col_str}) values: {distinct_col_vals.values[0][0]}")
    return distinct_col_vals.values[0][0]


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
        f"-- Save to file in /airflow/dbt/models/staging/{table_name}.sql",
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


def format_dbt_stub_for_intermediate_clean_stage(table_name: str, engine: Engine) -> List[str]:
    table_cols = get_table_sqlalchemy_col_objects(
        table_name=table_name, schema_name="data_raw", engine=engine
    )
    table_col_names = ["REPLACE_WITH_BETTER_id"]
    table_col_names.extend([col.name for col in table_cols])
    file_lines = [
        f"-- Save to file in /airflow/dbt/models/intermediate/{table_name}_clean.sql",
        "{{ config(materialized='view') }}",
        "{% set ck_cols = [",
        "        REPLACE_WITH_COMPOSITE_KEY_COLUMNS",
        "] %}",
        """{% set id_col = "REPLACE_WITH_BETTER_id" %}""",
    ]
    table_col_lines = format_jinja_variable_declaration_of_col_list(
        table_col_names=table_col_names, var_name="base_cols"
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
        "        row_number() over(partition by {{id_col}} ORDER BY source_data_updated DESC) as rn",
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
        "        row_number() over(partition by {{id_col}} ORDER BY source_data_updated ASC) as rn",
        "FROM std_data",
        "),",
        "records_first_ingested_pub_date AS (",
        "    SELECT {{id_col}}, source_data_updated AS first_ingested_pub_date",
        "    FROM std_records_numbered_earliest_first",
        "    WHERE rn = 1",
        ")",
        "",
        "SELECT",
        "    {% for bc in base_cols %}mcr.{{ bc }},{% endfor %}",
        "    fi.first_ingested_pub_date",
        "FROM most_current_records AS mcr",
        "LEFT JOIN records_first_ingested_pub_date AS fi",
        "ON mcr.{{ id_col }} = fi.{{ id_col }}",
        "ORDER BY {% for ck in ck_cols %}mcr.{{ ck }} DESC, {% endfor %} mcr.source_data_updated DESC",
    ]
    file_lines.extend(cte_lines)
    return file_lines
