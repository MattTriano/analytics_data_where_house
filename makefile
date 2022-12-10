.phony: startup shutdown make_credentials all serve_dbt_docs update_dbt_packages build init_airflow restart
.DEFAULT_GOAL: startup

MAKEFILE_FILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR_PATH := ${dir ${MAKEFILE_FILE_PATH}}
STARTUP_DIR := ${MAKEFILE_DIR_PATH}.startup/
AF_DB_DFILE := "$(MAKEFILE_DIR_PATH)Dockerfile/airflow_db.Dockerfile"
AF_DFILE := "$(MAKEFILE_DIR_PATH)Dockerfile/airflow.Dockerfile"
DBT_DFILE := "$(MAKEFILE_DIR_PATH)Dockerfile/dbt.Dockerfile"
PG_DWH_DFILE := "$(MAKEFILE_DIR_PATH)Dockerfile/postgis.Dockerfile"
PGADMIN_DFILE := "$(MAKEFILE_DIR_PATH)Dockerfile/pgadmin4.Dockerfile"
AF_REQS := "$(MAKEFILE_DIR_PATH)requirements/airflow_requirements.txt"
run_time := "$(shell date '+%Y_%m_%d__%H_%M_%S')"

PROJECT_NAME := $(shell basename $(MAKEFILE_DIR_PATH) | tr '[:upper:]' '[:lower:]')
DBT_CONTAINER_ID := $(shell docker ps -aqf "name=$(PROJECT_NAME)_dbt_proj*")

startup:
	docker-compose up

shutdown:
	docker-compose down

all:
	@echo "$(AF_REQS)";
	@echo $(DBT_CONTAINER_ID)

make_credentials:
	python $(STARTUP_DIR)make_env.py \
		--project_dir=$(MAKEFILE_DIR_PATH)

serve_dbt_docs:
	docker exec $(DBT_CONTAINER_ID) /bin/bash -c "dbt docs generate";
	docker exec $(DBT_CONTAINER_ID) /bin/bash -c "dbt docs serve --port 18080";

update_dbt_packages:
	docker exec $(DBT_CONTAINER_ID) /bin/bash -c "dbt deps";

build: $(AF_DB_DFILE) $(AF_DFILE) $(AF_REQS) $(DBT_DFILE) $(PG_DWH_DFILE) $(PGADMIN_DFILE)
	docker-compose build

init_airflow:
	docker-compose up airflow-init

restart:
	docker-compose down;
	docker-compose up;
