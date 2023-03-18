FROM apache/airflow:2.5.2
RUN pip install --no-cache-dir apache-airflow-providers-discord==3.1.0