FROM apache/airflow:2.10.4-python3.12
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
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install -v --no-cache-dir -r /requirements.txt
