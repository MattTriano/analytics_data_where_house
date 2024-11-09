import datetime as dt
from logging import Logger
import logging
import subprocess

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from tasks.maintenance.backup import backup_and_lightly_verify_postgres_db

task_logger = logging.getLogger("airflow.task")


@task
def dump_db(task_logger: Logger) -> None:
    cmd = f"""PGPASSWORD=$POSTGRES_PASSWORD \
              pg_dump -h airflow_db -U $POSTGRES_USER $POSTGRES_DB > \
                /opt/airflow/backups/airflow_metadata_db_dump_`date +%d-%m-%Y"_"%H_%M_%S`.sql && \
              PGPASSWORD="" \
           """
    subproc_output = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    for el in subproc_output.stdout.split("\n"):
        task_logger.info(f"{el}")
    return "result"


@task
def dumpall_dwh_db(task_logger: Logger) -> None:
    cmd = f"""PGPASSWORD=$DWH_POSTGRES_PASSWORD \
              pg_dumpall -U $DWH_POSTGRES_USER -h dwh_db -c | gzip > \
                /opt/airflow/backups/dwh_db_dump_`date +%d-%m-%Y"_"%H_%M_%S`.sql.gz && \
              PGPASSWORD="" \
           """
    subproc_output = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    for el in subproc_output.stdout.split("\n"):
        task_logger.info(f"{el}")
    return "result"


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["utility", "maintenance"],
)
def backup_dwh_db():
    # dump_db_1 = dumpall_dwh_db(task_logger=task_logger)
    dump_db_1 = backup_and_lightly_verify_postgres_db(conn_id="dwh_db_conn")
    chain(dump_db_1)


backup_dwh_db()
