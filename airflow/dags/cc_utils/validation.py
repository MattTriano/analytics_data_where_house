from logging import Logger
from pathlib import Path

import great_expectations as ge


def get_data_context() -> ge.DataContext:
    context_root_dir = Path("/opt/airflow/great_expectations")
    if not context_root_dir.is_dir():
        if Path("/home/great_expectations").is_dir():
            context_root_dir = Path("/opt/airflow/great_expectations")
        else:
            raise Exception(f"great_expectations project dir not found (in expected places).")
    return ge.DataContext(context_root_dir=context_root_dir)


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
