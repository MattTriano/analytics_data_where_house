.phony: all make_credentials

MAKEFILE_FILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR_PATH := ${dir ${MAKEFILE_FILE_PATH}}

DOT_ENV_PATH := "${MAKEFILE_DIR_PATH}.env"
DOT_DWH_ENV_PATH := "${MAKEFILE_DIR_PATH}.dwh.env"

run_time := "$(shell date '+%Y_%m_%d__%H_%M_%S')"

ifneq ("$(wildcard ${DOT_ENV_PATH})", "")
	dot_env_exists = 1
else
	dot_env_exists = 0
endif

all:
	@echo "${MAKEFILE_FILE_PATH}"
	@echo "${MAKEFILE_DIR_PATH}"
	@echo "$(DOT_ENV_PATH)"
	@echo "$(DOT_DWH_ENV_PATH)"
	@echo "$(run_time)"
	@echo "$(shell echo -e 'AIRFLOW_UID=$(shell id -u)')"
	@echo "${dot_env_exists}"

make_credentials:
	python make_env.py \
		--project_dir=$(MAKEFILE_DIR_PATH)