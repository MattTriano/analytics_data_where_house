import datetime as dt
import logging
from logging import Logger

from airflow.decorators import dag, task

from cc_utils.socrata import SocrataTable

# from tasks.socrata_tasks import update_socrata_table
from sources.tables import CHICAGO_AFFORDABLE_RENTAL_HOUSING as SOCRATA_TABLE

task_logger = logging.getLogger("airflow.task")


import subprocess
from cc_utils.validation import get_data_context, run_checkpoint


@task
def run_ge_validation_checkpoint_subprocess(
    socrata_table: SocrataTable, schema_name: str, task_logger: Logger
):
    checkpoint_name = f"{schema_name}.{socrata_table.table_name}"
    checkpoint_run = subprocess.run(
        ["great_expectations", "checkpoint", "run", checkpoint_name],
        check=True,
        capture_output=True,
        universal_newlines=True,
    )
    task_logger.info(f"checkpoint_run:        {checkpoint_run}")
    task_logger.info(f"checkpoint_run_stdout: {checkpoint_run.stdout}")
    if checkpoint_run.returncode != 0:
        task_logger.info(
            f"Validation check exited with a non-zero value: {checkpoint_run.returncode}"
        )
        task_logger.error(f"Validation check output: {checkpoint_run}")
        raise Exception("Validation checkpoint failed. Check data docs to find failed validations.")


@task
def run_ge_validation_checkpoint_pythonic(
    socrata_table: SocrataTable, schema_name: str, task_logger: Logger
):
    checkpoint_name = f"{schema_name}.{socrata_table.table_name}"
    checkpoint_results = run_checkpoint(checkpoint_name=checkpoint_name, task_logger=task_logger)
    if checkpoint_results.success:
        task_logger.info("Validation successful!")
    else:
        task_logger.info("Validation Failed! Check data docs to find failed validations.")


@task
def list_ge_validation_checkpoints(task_logger: Logger):
    checkpoint_list = subprocess.run(
        ["great_expectations", "checkpoint", "list"],
        check=True,
        capture_output=True,
        universal_newlines=True,
    )
    task_logger.info(f"checkpoint_list:        {checkpoint_list}")
    task_logger.info(f"checkpoint_list.stdout: {checkpoint_list.stdout}")
    if checkpoint_list.returncode != 0:
        task_logger.info(
            f"Checkpoint list exited with a non-zero value: {checkpoint_list.returncode}"
        )
        task_logger.error(f"Checkpoint list output: {checkpoint_list}")
        raise Exception("Checkpoint list failed. Check data docs to find failed validations.")


@task
def check_for_checkpoint(
    socrata_table: SocrataTable, task_logger: Logger, schema_name: str = "data_raw"
):
    data_context = get_data_context()
    checkpoint_name = f"{schema_name}.{socrata_table.table_name}"
    checkpoint_list = data_context.list_checkpoints()
    if checkpoint_name not in checkpoint_list:
        task_logger.info(f"checkpoint {checkpoint_name} doesn't appear to exist.")
        task_logger.info(f"Here are the available checkpoints: {checkpoint_list}.")
    else:
        task_logger.info(f"checkpoint {checkpoint_name} exists!")
        task_logger.info(f"Here are the available checkpoints: {checkpoint_list}.")


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["public_housing", "chicago", "cook_county", "geospatial", "data_raw"],
)
def dev_ge_data_raw_chicago_affordable_rental_housing():
    # update_1 = update_socrata_table(
    #     socrata_table=SOCRATA_TABLE,
    #     conn_id="dwh_db_conn",
    #     task_logger=task_logger,
    # )
    # checkpoint_check_1 = check_for_checkpoint(
    #     socrata_table=SOCRATA_TABLE,
    #     schema_name="data_raw",
    #     task_logger=task_logger,
    # )
    # checkpoint_list_1 = list_ge_validation_checkpoints(task_logger=task_logger)
    checkpoint_1 = run_ge_validation_checkpoint_pythonic(
        socrata_table=SOCRATA_TABLE,
        schema_name="data_raw",
        task_logger=task_logger,
    )
    # checkpoint_check_1 >> checkpoint_list_1 >>
    checkpoint_1


dev_ge_data_raw_chicago_affordable_rental_housing()
