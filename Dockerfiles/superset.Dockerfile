FROM apache/superset:2.1.0

COPY requirements/superset/docker-init.sh /app/docker/docker-init.sh
COPY requirements/superset/pythonpath_dev /app/docker/pythonpath_dev
COPY requirements/superset/docker-bootstrap.sh /app/docker/docker-bootstrap.sh
COPY requirements/superset/superset_requirements.txt /app/docker/requirements.txt

USER root
RUN pip install -v --no-cache -r /app/docker/requirements.txt
USER superset