FROM python:3.9.16
WORKDIR /home
COPY requirements/python_requirements.txt /requirements.txt
COPY airflow/pytest.ini pytest.ini
COPY .startup/make_fernet_key.py make_fernet_key.py
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt