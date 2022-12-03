FROM apache/airflow:2.5.0-python3.10
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         jq \
         postgresql-client \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements/airflow_requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt