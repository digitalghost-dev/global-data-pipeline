from airflow import DAG
from datetime import datetime

try:
    from docker.types import Mount
    from airflow.providers.docker.operators.docker import DockerOperator
    from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
    from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
except ImportError:
    print("Couldn't import docker.types and/or DockerOperator modules.")

bucket = 'global-data-storage-bucket'
table = 'cloud-data-infrastructure.global_data_dataset.city_weather'

# Coordinates Table
with DAG("city_coordinates", start_date=datetime(2023, 1, 1), schedule_interval='*/10 * * * *', catchup=False) as dag:

    task_1 = DockerOperator (
        task_id="1-run_docker_container",
        image='digitalghostdev/global-data-pipeline:city_coordinates',
        command='python3 city_coordinates.py',
        docker_url='tcp://docker-proxy:2375',
        network_mode='host',
        mounts=[
            Mount(
                source='/tmp/keys/keys.json',
                target='/tmp/keys/keys.json',
                type='bind'
            )
        ]
    )

# Country Statistics Table
with DAG("country_statistics", start_date=datetime(2023, 1, 1), schedule_interval='@hourly', catchup=False) as dag:

    task_a = DockerOperator (
        task_id="task_a",
        image='digitalghostdev/global-data-pipeline:country_statistics',
        command='python3 country_statistics.py',
        docker_url='tcp://docker-proxy:2375',
        network_mode='host',
        mounts=[
            Mount(
                source='/tmp/keys/keys.json',
                target='/tmp/keys/keys.json',
                type='bind'
            )
        ]
    )

# Weather Table
with DAG("city_weather", start_date=datetime(2023, 1, 1), schedule_interval='@hourly', catchup=False) as dag:

    task_a = DockerOperator (
        task_id="run_docker_container",
        image='digitalghostdev/global-data-pipeline:city_weather',
        command='python3 city_weather.py',
        docker_url='tcp://docker-proxy:2375',
        network_mode='host',
        mounts=[
            Mount(
                source='/tmp/keys/keys.json',
                target='/tmp/keys/keys.json',
                type='bind'
            )
        ]
    )

    task_b = PostgresToGCSOperator (
        task_id="postgres_to_cloud_storage",
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM "gd.city_weather";',
        bucket=bucket,
        filename='city_weather.csv',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

    task_c = GCSToBigQueryOperator (
        task_id="cloud_storage_to_bigquery",
        gcp_conn_id='google_cloud_default',
        bucket=bucket,
        source_objects=['city_weather.csv'],
        source_format='CSV',
        destination_project_dataset_table=table,
        schema_fields = [
            {'name': 'city', 'type': 'STRING'},
            {'name': 'current_temp', 'type': 'INTEGER'},
            {'name': 'wind_speed', 'type': 'INTEGER'},
            {'name': 'precip', 'type': 'FLOAT'},
            {'name': 'humidity', 'type': 'INTEGER'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

task_a >> task_b >> task_c

# Population Table
with DAG("population", start_date=datetime(2023, 1, 1), schedule_interval='@hourly', catchup=False) as dag:

    task_a = DockerOperator (
        task_id="task_a",
        image='digitalghostdev/global-data-pipeline:population',
        command='python3 population.py',
        docker_url='tcp://docker-proxy:2375',
        network_mode='host',
        mounts=[
            Mount(
                source='/tmp/keys/keys.json',
                target='/tmp/keys/keys.json',
                type='bind'
            )
        ]
    )