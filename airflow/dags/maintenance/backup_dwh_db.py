import datetime as dt

from airflow.decorators import dag
from airflow.models.baseoperator import chain

from tasks.maintenance.backup import backup_and_lightly_verify_postgres_db


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["utility", "maintenance"],
)
def backup_dwh_db():
    dump_db_1 = backup_and_lightly_verify_postgres_db(conn_id="dwh_db_conn")
    chain(dump_db_1)


backup_dwh_db()
