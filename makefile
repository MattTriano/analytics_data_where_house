SHELL := /bin/bash
.PHONY: startup shutdown quiet_startup restart make_credentials serve_dbt_docs \
	build_images init_airflow initialize_system create_warehouse_infra update_dbt_packages \
	dbt_generate_docs build_python_img get_py_utils_shell make_fernet_key run_tests \
	build_images_no_cache setup_project_dir
	
.DEFAULT_GOAL: startup

MAKEFILE_FILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR_PATH := ${dir ${MAKEFILE_FILE_PATH}}
STARTUP_DIR := ${MAKEFILE_DIR_PATH}.startup/
run_time := "$(shell date '+%Y_%m_%d__%H_%M_%S')"

PROJECT_NAME := $(shell basename $(MAKEFILE_DIR_PATH) | tr '[:upper:]' '[:lower:]')

make_credentials:
	@if [ -f .env ] || [ -f .env.dwh ] || [ -f .env.superset ]; then \
		echo "Some .env files already exist. Remove or rename them to rerun startup process."; \
	else \
		echo "Running startup scripts to create .env files with ADWH credentials."; \
		docker build -t adwh_startup -f .startup/Dockerfile.startup .startup/; \
		docker run --rm -it -v "${STARTUP_DIR}:/startup" -e UID=$(shell id -u) adwh_startup; \
		mv "${STARTUP_DIR}/.env" "${MAKEFILE_DIR_PATH}/.env"; \
		mv "${STARTUP_DIR}/.env.dwh" "${MAKEFILE_DIR_PATH}/.env.dwh"; \
		mv "${STARTUP_DIR}/.env.superset" "${MAKEFILE_DIR_PATH}/.env.superset"; \
	fi

setup_project_dir:
	mkdir -p $(MAKEFILE_DIR_PATH)/logs

build_images: setup_project_dir
	echo "Building docker images and outputting build logs to ./logs/"; \
	docker compose build 2>&1 | tee logs/where_house_build_logs_$(run_time).txt

build_images_no_cache:
	echo "Building docker images without using cached layers and outputting build logs to ./logs/"; \
	docker compose build --no-cache 2>&1 | tee logs/where_house_build_logs_$(run_time).txt

init_airflow: build_images
	echo "Initializing Airflow"; \
	docker compose up airflow-init

initialize_system: setup_project_dir build_images init_airflow

startup:
	docker compose up

quiet_startup:
	docker compose up -d

shutdown:
	docker compose down

restart:
	docker compose down;
	docker compose up;

dbt_generate_docs:
	docker compose exec -u airflow airflow-scheduler /bin/bash -c "cd dbt && dbt docs generate"

serve_dbt_docs: dbt_generate_docs
	docker compose exec dbt_proj /bin/bash -c "dbt docs serve --port 18080";

update_dbt_packages: quiet_startup
	docker compose exec airflow-scheduler /bin/bash -c "cd dbt && dbt deps"

clean_dbt:
	docker compose exec -u airflow airflow-scheduler /bin/bash -c "cd dbt && dbt clean";
	docker compose exec -u airflow airflow-scheduler /bin/bash -c "cd dbt && dbt deps";
	docker compose exec -u airflow airflow-scheduler /bin/bash -c "mkdir -p /opt/airflow/dbt/target"

create_warehouse_infra:
	echo "Creating essential schemas, metadata-tracking tables, directories, and other infra."; \
	docker compose exec -u airflow airflow-scheduler /bin/bash -c \
		"airflow dags unpause create_socrata_dataset_metadata_table &&\
		 airflow dags trigger create_socrata_dataset_metadata_table &&\
		 airflow dags unpause setup_schemas &&\
		 airflow dags trigger setup_schemas &&\
		 airflow dags unpause create_census_api_dataset_metadata_tables &&\
		 airflow dags trigger create_census_api_dataset_metadata_tables &&\
		 cd /opt/airflow/dbt && dbt deps &&\
		 mkdir -p /opt/airflow/dbt/models/data_raw &&\
		 mkdir -p /opt/airflow/dbt/models/standardized &&\
		 mkdir -p /opt/airflow/dbt/models/clean &&\
		 mkdir -p /opt/airflow/dbt/models/feature &&\
		 mkdir -p /opt/airflow/dbt/models/dwh"

run_tests:
	echo "Running pytest in airflow-scheduler container."; \
	docker compose exec -u airflow airflow-scheduler /bin/bash -c \
		"cd /opt/airflow && python -m pytest -s"

serve_great_expectations_jupyterlab:
	docker compose exec -u airflow airflow-scheduler /bin/bash -c \
		"mkdir -p /opt/airflow/.jupyter/share/jupyter/runtime &&\
		cd /opt/airflow/great_expectations/ &&\
		jupyter lab --ip 0.0.0.0 --port 18888"
