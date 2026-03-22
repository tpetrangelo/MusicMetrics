FROM apache/airflow:2.8.0

USER root
COPY . /opt/airflow/MusicMetrics
RUN chown -R airflow:root /opt/airflow/MusicMetrics && \
    chmod -R 775 /opt/airflow/MusicMetrics && \
    mkdir -p /opt/airflow/MusicMetrics/music_metrics_pipeline/dbt/music_metrics/logs && \
    chown -R airflow:root /opt/airflow/MusicMetrics/music_metrics_pipeline/dbt/music_metrics/logs && \
    chmod -R 777 /opt/airflow/MusicMetrics/music_metrics_pipeline/dbt/music_metrics/logs

USER airflow
RUN pip install -r /opt/airflow/MusicMetrics/docker/airflow/requirements.txt
RUN pip install -e /opt/airflow/MusicMetrics