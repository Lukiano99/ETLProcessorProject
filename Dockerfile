FROM apache/airflow:2.10.4-python3.11

COPY requirements-airflow.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src/ /opt/airflow/src/
COPY sql/ /opt/airflow/sql/
