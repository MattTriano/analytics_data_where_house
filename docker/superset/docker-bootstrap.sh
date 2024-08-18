#!/usr/bin/env bash

set -eo pipefail

REQUIREMENTS_LOCAL="/app/docker/requirements-local.txt"
#
# Make sure we have dev requirements installed
#
if [ -f "${REQUIREMENTS_LOCAL}" ]; then
  echo "Installing local overrides at ${REQUIREMENTS_LOCAL}"
  pip install -r "${REQUIREMENTS_LOCAL}"
else
  echo "Skipping local overrides"
fi

if [[ "${1}" == "worker" ]]; then
  echo "Starting Celery worker..."
  celery --app=superset.tasks.celery_app:app worker -O fair -l INFO
elif [[ "${1}" == "beat" ]]; then
  echo "Starting Celery beat..."
  celery --app=superset.tasks.celery_app:app beat --pidfile /tmp/celerybeat.pid -l INFO -s "${SUPERSET_HOME}"/celerybeat-schedule
elif [[ "${1}" == "app" ]]; then
  echo "Starting web app..."
  flask run -p 8088 --with-threads --reload --debugger --host=0.0.0.0
elif [[ "${1}" == "app-gunicorn" ]]; then
  echo "Starting web app..."
  /usr/bin/run-server.sh
fi
