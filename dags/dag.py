from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

with DAG (
    "population", 
    start_date=datetime(2023, 1, 1), 
    schedule_interval='@daily', 
    catchup=False
) as dag:

    task_a = DockerOperator (
        task_id="task_a",
        image='population:1.0',
        command='python3 population.py',
        docker_url='tcp://docker-proxy:2375',
        network_mode='host'
    )