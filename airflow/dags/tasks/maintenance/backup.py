import datetime as dt
import gzip
import logging
import os
from pathlib import Path
import shutil
import subprocess

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task
def dump_postgres_db_v0(conn_id: str) -> Path:
    logger = logging.getLogger(__name__)
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    service_name = conn.host
    file_name = f"{service_name}_backup_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_file_dir = Path("/opt/airflow/backups").joinpath(service_name)
    backup_file_dir.mkdir(exist_ok=True)
    backup_file_path = backup_file_dir.joinpath(f"{file_name}.dump")
    gzipped_file_path = backup_file_dir.joinpath(f"{file_name}.gz")
    logger.info(f"Preparing to backup the {service_name} db to {backup_file_path}.")
    try:
        result = subprocess.run(
            [
                "pg_dumpall",
                "-h",
                service_name,
                "-p",
                str(conn.port),
                "-U",
                conn.login,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={"PGPASSWORD": conn.password},
            check=True,
        )
        if result.stderr:
            logger.warning(f"pg_dumpall warnings: {result.stderr}")
        with open(backup_file_path, "wb") as f:
            f.write(result.stdout)
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Database backup failed: {e.stderr.decode()}")

    logger.info("Compressing backup file...")
    try:
        with open(backup_file_path, "wb") as f_in:
            with gzip.open(gzipped_file_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        raise AirflowException(f"Compression failed: {str(e)}")
    finally:
        if backup_file_path.exists():
            backup_file_path.unlink()
    logger.info(
        f"Finished backing up and compressing the {service_name} db to {gzipped_file_path}."
    )
    return gzipped_file_path


@task
def dump_postgres_db(conn_id: str) -> Path:
    logger = logging.getLogger(__name__)
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    service_name = conn.host
    file_name = f"{service_name}_backup_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_file_dir = Path("/opt/airflow/backups").joinpath(service_name)
    backup_file_dir.mkdir(exist_ok=True)
    backup_file_path = backup_file_dir.joinpath(f"{file_name}.dump")
    gzipped_file_path = backup_file_dir.joinpath(f"{file_name}.gz")
    logger.info(f"Preparing to backup the {service_name} db to {backup_file_path}.")
    try:
        with gzip.open(gzipped_file_path, "wb", compresslevel=9) as gz_file:
            process = subprocess.Popen(
                [
                    "pg_dumpall",
                    "-h",
                    service_name,
                    "-p",
                    str(conn.port),
                    "-U",
                    conn.login,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env={"PGPASSWORD": conn.password},
            )
            while True:
                chunk = process.stdout.read(8192)
                if not chunk:
                    break
                gz_file.write(chunk)
            stderr = process.stderr.read()
            return_code = process.wait()
            if return_code != 0:
                raise AirflowException(f"Database backup failed: {stderr.decode()}")
            if stderr:
                logger.warning(f"pg_dumpall warnings: {stderr.decode()}")
    except Exception as e:
        if gzipped_file_path.exists():
            gzipped_file_path.unlink()
        raise AirflowException(f"Backup failed: {str(e)}")
    logger.info(f"Successfully backed up and compressed {service_name} db to {gzipped_file_path}")
    return gzipped_file_path


@task
def lightly_verify_backup(backup_file_path: Path) -> bool:
    """Verify the compressed backup file using lightweight heuristic checks"""

    logger = logging.getLogger(__name__)
    backup_file_path = Path(backup_file_path)
    try:
        logger.info(f"Verifying backup file: {backup_file_path}")
        try:
            with gzip.open(backup_file_path, "rb") as f:
                f.read(1024)
        except gzip.BadGzipFile as e:
            raise AirflowException(f"Corrupt gzip file: {str(e)}")
        expected_patterns = [
            b"PostgreSQL database dump",
            b"SET statement_timeout",
            b"CREATE DATABASE",
            b"COMMENT",
        ]
        pattern_matches = {pattern: False for pattern in expected_patterns}
        with gzip.open(backup_file_path, "rb") as f:
            chunk_size = 1024 * 1024
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                for pattern in expected_patterns:
                    if pattern in chunk:
                        pattern_matches[pattern] = True
                if all(pattern_matches.values()):
                    break
        matches_found = sum(pattern_matches.values())
        if matches_found < 2:
            raise AirflowException(
                f"Backup file doesn't appear to be a valid PostgreSQL dump. "
                f"Only found {matches_found} expected patterns."
            )
        file_size = backup_file_path.stat().st_size
        if file_size < 100:
            raise AirflowException(f"Backup file suspiciously small: {file_size} bytes")
        logger.info(
            f"Backup verification successful: "
            f"Found {matches_found} expected patterns, "
            f"file size: {file_size/1024/1024:.2f}MB"
        )
        return True
    except Exception as e:
        logger.error(f"Backup verification failed: {str(e)}")
        raise AirflowException(f"Backup verification failed: {str(e)}")


@task_group
def backup_and_lightly_verify_postgres_db(conn_id: str) -> None:
    dump_db_ = dump_postgres_db(conn_id=conn_id)
    verify_db_ = lightly_verify_backup(dump_db_)

    chain(dump_db_, verify_db_)
