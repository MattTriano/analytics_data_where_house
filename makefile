.phony: startup shutdown quiet_startup restart make_credentials serve_dbt_docs \
	build_images init_airflow initialize_system create_warehouse_infra update_dbt_packages \
	dbt_generate_docs build_python_img get_py_utils_shell make_fernet_key run_tests
	
.DEFAULT_GOAL: startup

MAKEFILE_FILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR_PATH := ${dir ${MAKEFILE_FILE_PATH}}
STARTUP_DIR := ${MAKEFILE_DIR_PATH}.startup/
run_time := "$(shell date '+%Y_%m_%d__%H_%M_%S')"

PROJECT_NAME := $(shell basename $(MAKEFILE_DIR_PATH) | tr '[:upper:]' '[:lower:]')

make_credentials:
	python $(STARTUP_DIR)make_env.py \
		--project_dir=$(MAKEFILE_DIR_PATH)

build_images:
	docker-compose build

init_airflow: build_images
	docker-compose up airflow-init

initialize_system: build_images init_airflow

startup:
	docker-compose up

quiet_startup:
	docker-compose up -d

shutdown:
	docker-compose down

restart:
	docker-compose down;
	docker-compose up;

dbt_generate_docs:
	docker-compose exec dbt_proj /bin/bash -c "dbt docs generate";

serve_dbt_docs: dbt_generate_docs
	docker-compose exec dbt_proj /bin/bash -c "dbt docs serve --port 18080";

update_dbt_packages: quiet_startup
	docker-compose exec dbt_proj /bin/bash -c "dbt deps";

clean_dbt:
	docker-compose exec dbt_proj /bin/bash -c "dbt clean";
	docker-compose exec dbt_proj /bin/bash -c "dbt deps";

create_warehouse_infra:
	docker-compose exec airflow-scheduler /bin/bash -c \
		"airflow dags trigger ensure_metadata_table_exists";
	docker-compose exec airflow-scheduler /bin/bash -c \
		"airflow dags trigger ensure_data_raw_schema_exists";
	docker-compose exec airflow-scheduler /bin/bash -c \
		"airflow dags trigger ensure_clean_schema_exists";
	docker-compose exec airflow-scheduler /bin/bash -c \
		"airflow dags trigger ensure_feature_schema_exists";
	docker-compose exec airflow-scheduler /bin/bash -c \
		"airflow dags trigger ensure_dwh_schema_exists";
	docker-compose exec dbt_proj /bin/bash -c "dbt deps";

build_python_img:
	docker-compose build py-utils

start_python_container:
	docker-compose up -d py-utils

get_py_utils_shell: start_python_container
	docker-compose exec py-utils /bin/bash

run_tests: start_python_container
	docker-compose exec py-utils /bin/bash -c "python -m pytest"

make_fernet_key: start_python_container
	docker-compose exec py-utils /bin/bash -c "python make_fernet_key.py"
