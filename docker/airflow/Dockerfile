FROM apache/airflow:2.8.0

USER root
COPY . /opt/airflow/MusicMetrics

USER airflow
RUN pip install -e /opt/airflow/MusicMetrics