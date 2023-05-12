from logging import Logger
from pathlib import Path

import great_expectations as gx
from great_expectations.exceptions import DataContextError
from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource


def get_data_context() -> gx.DataContext:
    context_root_dir = Path("/opt/airflow/great_expectations")
    if not context_root_dir.is_dir():
        if Path("/home/great_expectations").is_dir():
            context_root_dir = Path("/opt/airflow/great_expectations")
        else:
            raise Exception(f"great_expectations project dir not found (in expected places).")
    return gx.DataContext(context_root_dir=context_root_dir)


def check_if_checkpoint_exists(checkpoint_name: str, task_logger: Logger) -> bool:
    data_context = get_data_context()
    checkpoint_list = data_context.list_checkpoints()
    if checkpoint_name in checkpoint_list:
        task_logger.info(f"checkpoint {checkpoint_name} exists!")
        task_logger.info(f"Here are the available checkpoints: {checkpoint_list}.")
        return True
    else:
        task_logger.info(f"checkpoint {checkpoint_name} doesn't appear to exist.")
        task_logger.info(f"Here are the available checkpoints: {checkpoint_list}.")
        return False


def run_checkpoint(checkpoint_name: str, task_logger: Logger):
    data_context = get_data_context()
    if check_if_checkpoint_exists(checkpoint_name, task_logger):
        checkpoint_run = data_context.run_checkpoint(checkpoint_name=checkpoint_name)
        checkpoint_run_stats = checkpoint_run.get_statistics()
        task_logger.info(f"Checkpoint run results: {checkpoint_run_stats}")
        return checkpoint_run
    else:
        raise Exception(f"Checkpoint '{checkpoint_name}' not found. Did you leave off the schema?")


def get_datasource(datasource_name: str, task_logger: Logger) -> Datasource:
    context = get_data_context()
    try:
        datasource = context.get_datasource(datasource_name)
        return datasource
    except DataContextError as dce:
        task_logger.error(f"Couldn't get the datasource named {datasource_name}")
        task_logger.error(f"You might have to register this datasource via a command like")
        task_logger.error(
            """
            datasource = context.sources.add_sql(
                name=datasource_name,
                connection_string="${GX_DWH_DB_CONN}"
            )"""
        )
        task_logger.error(f"Actual error message: {dce}, {type(dce)}")
        raise


def register_data_asset(
    schema_name: str, table_name: str, datasource: Datasource, task_logger: Logger
):
    if f"{schema_name}.{table_name}" not in datasource.get_asset_names():
        _ = datasource.add_table_asset(
            name=f"{schema_name}.{table_name}",
            schema_name=schema_name,
            table_name=table_name,
        )
        task_logger.info(f"Registered {schema_name}.{table_name} as a DataAsset.")
    else:
        task_logger.info(f"A DataAsset named {schema_name}.{table_name} already exists.")
