from airflow import DAG
from datetime import datetime

try:
    from docker.types import Mount
    from airflow.providers.docker.operators.docker import DockerOperator
except ImportError:
    print("Couldn't import docker.types and/or DockerOperator modules.")


# Population Table
with DAG("population", start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    task_a = DockerOperator (
        task_id="task_a",
        image='population:1.0',
        command='python3 population.py',
        docker_url='tcp://docker-proxy:2375',
        network_mode='host',
        mounts=[
            Mount(
                source='/tmp/keys/keys.json',
                target='/tmp/keys/keys.json',
                type='bind'
            )
        ],
        container_name='population-container'
    )

# Weather Table
with DAG("weather", start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    task_a = DockerOperator (
        task_id="task_a",
        image='weather:1.0',
        command='python3 weather.py',
        docker_url='tcp://docker-proxy:2375',
        network_mode='host',
        container_name='weather-container'
    )

# City Coordinates Table
with DAG("city_coordinates", start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    task_a = DockerOperator (
        task_id="task_a",
        image='city_coordinates:1.0',
        command='python3 location.py',
        docker_url='tcp://docker-proxy:2375',
        network_mode='host',
        container_name='city_coordinates-container'
    )