SHELL = /bin/bash
.phony: startup shutdown quiet_startup restart make_credentials serve_dbt_docs \
	build_images init_airflow initialize_system create_warehouse_infra update_dbt_packages \
	dbt_generate_docs build_python_img get_py_utils_shell make_fernet_key run_tests \
	build_images_no_cache
	
.DEFAULT_GOAL: startup

MAKEFILE_FILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR_PATH := ${dir ${MAKEFILE_FILE_PATH}}
STARTUP_DIR := ${MAKEFILE_DIR_PATH}.startup/
VENV_PATH := $(MAKEFILE_DIR_PATH).venv
run_time := "$(shell date '+%Y_%m_%d__%H_%M_%S')"

PROJECT_NAME := $(shell basename $(MAKEFILE_DIR_PATH) | tr '[:upper:]' '[:lower:]')

$(VENV_PATH):
	python -m venv $(VENV_PATH); \
	source $(VENV_PATH)/bin/activate; \
	python -m pip install --upgrade pip; \
	python -m pip install -Ur $(STARTUP_DIR)venv_reqs.txt

make_venv: | $(VENV_PATH)

make_credentials: | make_venv
	source $(VENV_PATH)/bin/activate; \
	python $(STARTUP_DIR)make_env.py \
		--project_dir=$(MAKEFILE_DIR_PATH)

build_images:
	docker compose build 2>&1 | tee logs/where_house_build_logs_$(run_time).txt

build_images_no_cache:
	docker compose build --no-cache 2>&1 | tee logs/where_house_build_logs_$(run_time).txt

init_airflow: build_images
	docker compose up airflow-init

initialize_system: build_images init_airflow

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
	docker compose exec airflow-scheduler /bin/bash -c "cd dbt && dbt docs generate"

serve_dbt_docs: dbt_generate_docs
	docker compose exec dbt_proj /bin/bash -c "dbt docs serve --port 18080";

update_dbt_packages: quiet_startup
	docker compose exec airflow-scheduler /bin/bash -c "cd dbt && dbt deps"

clean_dbt:
	docker compose exec airflow-scheduler /bin/bash -c "cd dbt && dbt clean";
	docker compose exec airflow-scheduler /bin/bash -c "cd dbt && dbt deps";
	docker compose exec airflow-scheduler /bin/bash -c "mkdir -p /opt/airflow/dbt/target"

create_warehouse_infra: quiet_startup
	docker compose exec airflow-scheduler /bin/bash -c \
		"airflow dags trigger ensure_metadata_table_exists";
	docker compose exec airflow-scheduler /bin/bash -c \
		"airflow dags trigger setup_schemas";
	docker compose exec airflow-scheduler /bin/bash -c "cd dbt && dbt deps";

build_python_img:
	docker compose build --no-cache py-utils 2>&1 | tee logs/python_build_logs_$(run_time).txt

start_python_container:
	docker compose up -d py-utils

get_py_utils_shell: start_python_container
	docker compose exec py-utils /bin/bash

run_tests: start_python_container
	docker compose exec py-utils /bin/bash -c "python -m pytest"
