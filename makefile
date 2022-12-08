.phony: make_credentials

MAKEFILE_FILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR_PATH := ${dir ${MAKEFILE_FILE_PATH}}
STARTUP_DIR := ${MAKEFILE_DIR_PATH}.startup/
run_time := "$(shell date '+%Y_%m_%d__%H_%M_%S')"

make_credentials:
	python $(STARTUP_DIR)make_env.py \
		--project_dir=$(STARTUP_DIR)
