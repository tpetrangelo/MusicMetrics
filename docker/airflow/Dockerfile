FROM apache/airflow:2.8.0

USER root
COPY . /opt/airflow/MusicMetrics
RUN chown -R airflow:root /opt/airflow/MusicMetrics

USER airflow
RUN pip install -r /opt/airflow/MusicMetrics/docker/airflow/requirements.txt
RUN pip install -e /opt/airflow/MusicMetrics