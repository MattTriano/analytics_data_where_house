import datetime as dt
import logging
from logging import Logger
import os
from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from cc_utils.db import get_pg_engine
from cc_utils.socrata import SocrataTable, SocrataTableMetadata
from cc_utils.file_factory import (
    write_lines_to_file,
    format_dbt_stub_for_intermediate_standardized_stage,
)
from sources.tables import CHICAGO_TOWED_VEHICLES as SOCRATA_TABLE
from tasks.socrata_tasks import (
    check_table_metadata,
    fresher_source_data_available,
    download_fresh_data,
    file_ext_branch_router,
    load_geojson_data,
    load_csv_data,
    socrata_table_checkpoint_exists,
    run_socrata_checkpoint,
    table_exists_in_data_raw,
    create_table_in_data_raw,
    dbt_staging_model_exists,
    make_dbt_staging_model,
    update_result_of_check_in_metadata_table,
    short_circuit_downstream,
)

task_logger = logging.getLogger("airflow.task")


@task_group
def load_data_tg(
    socrata_metadata: SocrataTableMetadata,
    socrata_table: SocrataTable,
    conn_id: str,
    task_logger: Logger,
) -> SocrataTableMetadata:
    task_logger.info(f"Entered load_data_tg task_group")
    file_ext_route_1 = file_ext_branch_router(socrata_metadata=socrata_metadata)

    geojson_route_1 = load_geojson_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )
    csv_route_1 = load_csv_data(
        route_str=file_ext_route_1, conn_id=conn_id, task_logger=task_logger
    )
    checkpoint_exists_1 = socrata_table_checkpoint_exists(task_logger=task_logger)
    checkpoint_1 = run_socrata_checkpoint(task_logger=task_logger)
    table_exists_1 = table_exists_in_data_raw(conn_id=conn_id, task_logger=task_logger)
    create_staging_table_1 = create_table_in_data_raw(
        conn_id=conn_id, task_logger=task_logger, temp_table=False
    )
    dbt_staging_model_exists_1 = dbt_staging_model_exists(task_logger=task_logger, temp_table=False)
    make_dbt_staging_model_1 = make_dbt_staging_model(conn_id=conn_id, task_logger=task_logger)
    update_data_raw_table_1 = BashOperator(
        task_id="update_data_raw_table",
        bash_command=f"""cd /opt/airflow/dbt && \
            dbt --warn-error run --select re_dbt.staging.{socrata_table.table_name}""",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    update_metadata_true_1 = update_result_of_check_in_metadata_table(
        conn_id=conn_id, task_logger=task_logger, data_updated=True
    )

    chain(
        file_ext_route_1,
        [geojson_route_1, csv_route_1],
        checkpoint_exists_1,
        [Label("Checkpoint Doesn't Exist"), checkpoint_1],
        table_exists_1,
        [Label("Table Exists"), create_staging_table_1],
        dbt_staging_model_exists_1,
        [Label("dbt Staging Model Exists"), make_dbt_staging_model_1],
        update_data_raw_table_1,
        update_metadata_true_1,
    )


@task
def make_dbt_standardized_model(conn_id: str, task_logger: Logger, **kwargs) -> None:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    engine = get_pg_engine(conn_id=conn_id)
    std_file_lines = format_dbt_stub_for_intermediate_standardized_stage(
        table_name=socrata_metadata.table_name, engine=engine
    )
    file_path = Path(
        f"/opt/airflow/dbt/models/intermediate/{socrata_metadata.table_name}_standardized.sql"
    )
    write_lines_to_file(file_lines=std_file_lines, file_path=file_path)
    task_logger.info(f"file_lines for table {socrata_metadata.table_name}")
    for file_line in std_file_lines:
        task_logger.info(f"    {file_line}")

    task_logger.info(f"Leaving make_dbt_standardized_model")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def dbt_standardized_model_ready(task_logger: Logger, **kwargs) -> str:
    ti = kwargs["ti"]
    socrata_metadata = ti.xcom_pull(task_ids="update_socrata_table.download_fresh_data")
    airflow_home = os.environ["AIRFLOW_HOME"]
    file_path = Path(airflow_home).joinpath(
        "dbt",
        "models",
        "intermediate",
        f"{socrata_metadata.table_name}_standardized.sql",
    )
    host_file_path = str(file_path).replace(airflow_home, "/airflow")
    if file_path.is_file():
        with open(file_path, "r") as f:
            file_lines = f.readlines()
        for file_line in file_lines:
            if (
                "REPLACE_WITH_COMPOSITE_KEY_COLUMNS" in file_line
                or "REPLACE_WITH_BETTER_id" in file_line
            ):
                task_logger.info(
                    f"Found unfinished stub for dbt _standardized model in {host_file_path}."
                    + " Please update that model before proceeding to feature engineering."
                )
                return "update_socrata_table.highlight_unfinished_dbt_standardized_stub"
        task_logger.info(f"Found a _standardized stage dbt model that looks finished; Proceeding")
        return "update_socrata_table.endpoint"
    else:
        task_logger.info(f"No _standardized stage dbt model found.")
        task_logger.info(f"Creating a stub in loc: {host_file_path}")
        task_logger.info(f"Edit the stub before proceeding to generate _clean stage dbt models.")
        return "update_socrata_table.make_dbt_standardized_model"


@task
def highlight_unfinished_dbt_standardized_stub(task_logger: Logger) -> str:
    task_logger.info(
        f"Hey! Go finish the dbt _standardized model file indicated in the logs for the "
        + "dbt_standardized_model_ready task! It still contains at least one placeholder value"
        + "(REPLACE_WITH_COMPOSITE_KEY_COLUMNS or REPLACE_WITH_BETTER_id)."
    )
    return "Please and thank you!"


# @task.branch(trigger_rule=TriggerRule.NONE_FAILED)
# def dbt_standardized_model_exists(socrata_metadata: SocrataTableMetadata, task_logger: Logger) -> str:
#     airflow_home = os.environ["AIRFLOW_HOME"]
#     task_logger.info(f"airflow_home: {airflow_home}")
#     task_logger.info(f"table_name:   {socrata_metadata.table_name}")
#     file_path = Path(airflow_home).joinpath(
#         "dbt", "models", "intermediate", f"{socrata_metadata.table_name}_standardized.sql"
#     )
#     host_file_path = str(file_path).replace(airflow_home, "/airflow")
#     task_logger.info(f"file_path:         {file_path}")
#     task_logger.info(f"host_file_path:    {host_file_path}")
#     if file_path.is_file():
#         task_logger.info(f"Found a _standardized stage dbt model.")
#         task_logger.info(f"Proceeding to check if it's done.")
#         return "update_socrata_table.dbt_standardized_model_ready"
#     else:
#         task_logger.info(f"No _standardized stage dbt model found.")
#         task_logger.info(f"Creating a stub in loc: {host_file_path}")
#         task_logger.info(f"Edit the stub before proceeding to generate _clean stage dbt models.")
#         return "update_socrata_table.make_dbt_standardized_model"


@task
def endpoint(task_logger: Logger) -> None:
    task_logger.info("Ending run")
    return "end"


@task_group
def update_socrata_table(
    socrata_table: SocrataTable,
    conn_id: str,
    task_logger: Logger,
) -> SocrataTableMetadata:
    task_logger.info(f"Updating Socrata table {socrata_table.table_name}")

    metadata_1 = check_table_metadata(
        socrata_table=socrata_table, conn_id=conn_id, task_logger=task_logger
    )
    fresh_source_data_available_1 = fresher_source_data_available(
        socrata_metadata=metadata_1, conn_id=conn_id, task_logger=task_logger
    )
    extract_data_1 = download_fresh_data(task_logger=task_logger)
    load_data_tg_1 = load_data_tg(
        socrata_metadata=extract_data_1,
        socrata_table=socrata_table,
        conn_id=conn_id,
        task_logger=task_logger,
    )
    std_model_exists_1 = dbt_standardized_model_ready(
        socrata_metadata=load_data_tg_1, task_logger=task_logger
    )
    std_model_unfinished_1 = highlight_unfinished_dbt_standardized_stub(task_logger=task_logger)
    make_standardized_stage_1 = make_dbt_standardized_model(
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    endpoint_1 = endpoint(task_logger=task_logger)
    update_metadata_false_1 = update_result_of_check_in_metadata_table(
        conn_id=conn_id, task_logger=task_logger, data_updated=False
    )
    short_circuit_update_1 = short_circuit_downstream()

    chain(
        metadata_1,
        fresh_source_data_available_1,
        Label("Fresher data available"),
        extract_data_1,
        load_data_tg_1,
        std_model_exists_1,
        [
            make_standardized_stage_1,
            std_model_unfinished_1,
            Label("_standardized dbt model looks good!"),
        ],
        endpoint_1,
    )
    chain(
        metadata_1,
        fresh_source_data_available_1,
        Label("Local data is fresh"),
        update_metadata_false_1,
        short_circuit_update_1,
    )


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "chicago", "fact_table", "transit", "data_raw"],
)
def update_data_raw_chicago_towed_vehicles():
    update_1 = update_socrata_table(
        socrata_table=SOCRATA_TABLE,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    update_1


update_data_raw_chicago_towed_vehicles()
