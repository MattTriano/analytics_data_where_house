import datetime as dt
from logging import Logger
import logging
from pathlib import Path
import subprocess

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain


task_logger = logging.getLogger("airflow.task")


def get_dwh_backup_file_path(task_logger: Logger) -> Path:
    backups_dir_path = Path("/opt/airflow/backups")
    try:
        backup_dir_contents = [dp.name for dp in backups_dir_path.iterdir()]
        task_logger.info(f"Files in {backups_dir_path}: {backup_dir_contents}")
        if "to_restore" in backup_dir_contents:
            db_backup_file_names = [
                dp.name for dp in backups_dir_path.joinpath("to_restore").iterdir()
            ]
            db_backup_file_names.sort()
            task_logger.info(f"Files in .../to_restore/: {db_backup_file_names}")
        return backups_dir_path.joinpath("to_restore", db_backup_file_names[-1])
    except Exception as e:
        task_logger.info(f"Ran into error {e}, {type(e)}")
        raise


@task
def run_restore_dwh_db_cmd(task_logger: Logger) -> None:
    dwh_backup_file_path = get_dwh_backup_file_path(task_logger=task_logger)
    task_logger.info(f"dwh_backup_file_path: {dwh_backup_file_path}")
    cmd = f"""export PGPASSWORD=$(echo $DWH_POSTGRES_PASSWORD); \
              gzip -dc {str(dwh_backup_file_path)} | psql postgres -U $DWH_POSTGRES_USER -h dwh_db; \
              export PGPASSWORD="" \
           """
    subproc_output = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    task_logger.info(subproc_output)
    for el in subproc_output.stdout.split("\n"):
        task_logger.info(f"{el}")
    return "result"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["utility", "maintenance"],
)
def restore_dwh_db():
    restore_db_1 = run_restore_dwh_db_cmd(task_logger=task_logger)
    chain(restore_db_1)


# restore_dwh_db()
