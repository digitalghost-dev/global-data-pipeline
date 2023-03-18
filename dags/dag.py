from airflow import DAG
from datetime import datetime, timedelta

from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator

bucket = 'global-data-storage-bucket'
table = 'cloud-data-infrastructure.global_data_dataset.'

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 1, 1),
    'email': ['christian@digitalghost.dev'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Discord notification
with DAG("discord_alert", default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    discord_task = DiscordWebhookOperator (
        task_id="discord_message",
        http_conn_id="discord",
        message="Hello from Airflow!"
    )

discord_task

# coordinates table
with DAG("city_coordinates", default_args=default_args, schedule_interval='*/20 * * * *', catchup=False) as dag:

    task_a = DockerOperator (
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

    task_b = PostgresToGCSOperator (
        task_id="postgres_to_cloud_storage",
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM "gd.city_coordinates";',
        bucket=bucket,
        filename='city_coordinates.csv',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

    task_c = GCSToBigQueryOperator (
        task_id="cloud_storage_to_bigquery",
        gcp_conn_id='google_cloud_default',
        bucket=bucket,
        source_objects=['city_coordinates.csv'],
        source_format='CSV',
        destination_project_dataset_table=(table + "city_coordinates"),
        schema_fields = [
            {'name': 'city', 'type': 'STRING'},
            {'name': 'lat', 'type': 'FLOAT'},
            {'name': 'lon', 'type': 'FLOAT'},
            {'name': 'coordinates', 'type': 'STRING'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

task_a >> task_b >> task_c

# country statistics table
with DAG("country_statistics", default_args=default_args, schedule_interval='@monthly', catchup=False) as dag:

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

    # Fertility table tasks.
    task_b = PostgresToGCSOperator (
        task_id="postgres_to_cloud_storage_fertility",
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM "gd.fertility";',
        bucket=bucket,
        filename='fertility.csv',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

    task_c = GCSToBigQueryOperator (
        task_id="cloud_storage_to_bigquery_fertility",
        gcp_conn_id='google_cloud_default',
        bucket=bucket,
        source_objects=['fertility.csv'],
        source_format='CSV',
        destination_project_dataset_table=(table + "fertility"),
        schema_fields = [
            {'name': 'country', 'type': 'STRING'},
            {'name': 'fertility_Rate', 'type': 'FLOAT'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

    # Homicide table tasks.
    task_d = PostgresToGCSOperator (
        task_id="postgres_to_cloud_storage_homicide",
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM "gd.homicide";',
        bucket=bucket,
        filename='homicide.csv',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

    task_e = GCSToBigQueryOperator (
        task_id="cloud_storage_to_bigquery_homicide",
        gcp_conn_id='google_cloud_default',
        bucket=bucket,
        source_objects=['homicide.csv'],
        source_format='CSV',
        destination_project_dataset_table=(table + "homicide"),
        schema_fields = [
            {'name': 'country', 'type': 'STRING'},
            {'name': 'region', 'type': 'STRING'},
            {'name': 'subregion', 'type': 'STRING'},
            {'name': 'rate', 'type': 'FLOAT'},
            {'name': 'count', 'type': 'INTEGER'},
            {'name': 'last_updated', 'type': 'INTEGER'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

    # Obesity table tasks.
    task_f = PostgresToGCSOperator (
        task_id="postgres_to_cloud_storage_obesity",
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM "gd.obesity";',
        bucket=bucket,
        filename='obesity.csv',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

    task_g = GCSToBigQueryOperator (
        task_id="cloud_storage_to_bigquery_obesity",
        gcp_conn_id='google_cloud_default',
        bucket=bucket,
        source_objects=['obesity.csv'],
        source_format='CSV',
        destination_project_dataset_table=(table + "obesity"),
        schema_fields = [
            {'name': 'country', 'type': 'STRING'},
            {'name': 'obesity_rate_percentage', 'type': 'FLOAT'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

    # Unemployment table tasks.
    task_h = PostgresToGCSOperator (
        task_id="postgres_to_cloud_storage_unemployment",
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM "gd.unemployment";',
        bucket=bucket,
        filename='unemployment.csv',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

    task_i = GCSToBigQueryOperator (
        task_id="cloud_storage_to_bigquery_unemployment",
        gcp_conn_id='google_cloud_default',
        bucket=bucket,
        source_objects=['hdi.csv'],
        source_format='CSV',
        destination_project_dataset_table=(table + "hdi"),
        schema_fields = [
            {'name': 'country', 'type': 'STRING'},
            {'name': 'unemployment_rate', 'type': 'FLOAT'},
            {'name': 'last_updated', 'type': 'STRING'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

    # HDI table tasks.
    task_j = PostgresToGCSOperator (
        task_id="postgres_to_cloud_storage_hdi",
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM "gd.hdi";',
        bucket=bucket,
        filename='hdi.csv',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

    task_k = GCSToBigQueryOperator (
        task_id="cloud_storage_to_bigquery_hdi",
        gcp_conn_id='google_cloud_default',
        bucket=bucket,
        source_objects=['hdi.csv'],
        source_format='CSV',
        destination_project_dataset_table=(table + "hdi"),
        schema_fields = [
            {'name': 'country', 'type': 'STRING'},
            {'name': 'hdi', 'type': 'FLOAT'},
            {'name': 'hdi_growth', 'type': 'FLOAT'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

task_a >> task_b >> task_c
task_a >> task_d >> task_e
task_a >> task_f >> task_g
task_a >> task_h >> task_i
task_a >> task_j >> task_k

# weather table
with DAG("city_weather", default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:

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
        destination_project_dataset_table=(table + "city_weather"),
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

# population table
with DAG("population", default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:

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

    task_b = PostgresToGCSOperator (
        task_id="postgres_to_cloud_storage",
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM "gd.city_pop";',
        bucket=bucket,
        filename='city_pop.csv',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

    task_c = GCSToBigQueryOperator (
        task_id="cloud_storage_to_bigquery",
        gcp_conn_id='google_cloud_default',
        bucket=bucket,
        source_objects=['city_pop.csv'],
        source_format='CSV',
        destination_project_dataset_table=(table + "city_pop"),
        schema_fields = [
            {'name': 'rank', 'type': 'INTEGER'},
            {'name': 'city', 'type': 'STRING'},
            {'name': 'country', 'type': 'STRING'},
            {'name': 'population', 'type': 'BIGNUMERIC'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

task_a >> task_b >> task_c