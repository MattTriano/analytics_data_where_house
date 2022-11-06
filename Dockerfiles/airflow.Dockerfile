FROM apache/airflow:2.4.2-python3.10
COPY requirements/airflow_requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt