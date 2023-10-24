FROM apache/airflow:2.7.2-python3.10
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  jq \
  postgresql-client \
  libpq-dev \
  bash-completion \
  git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN mkdir -p /opt/airflow/.jupyter/share/jupyter/runtime
COPY requirements/airflow_requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install -v --no-cache-dir --user -r /requirements.txt
