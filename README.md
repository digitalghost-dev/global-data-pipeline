# Global Data Pipeline with Docker Compose, PostgreSQL, Airflow and Looker Studio

<div>
    <img alt="Version" src="https://img.shields.io/badge/Project Number-3-orange.svg?cacheSeconds=2592000" />
</div>

## Overiew

* A `docker-compose` runs Airflow, Postgres, and Redis in Docker containers.
* Python scripts reach out to different data sources to extract, transform and load the data into a Postgres database, orchestrated through Airflow on various schedules.
* The tables from Postgres are then loaded to BigQuery for easy integration to Looker Studio where the data is visualized.
* A Discord Airflow operator is used to send a daily message to a Discord server.

## Pipeline Steps
1. A shell scripts runs: `docker-compose up` to start the Airflow service, Postgres, and Redis in Docker containers.
2. The Airflow DAG uses the `DockerOperator` to pull the latest image from my Docker Hub repository for each container and runs the containers which executes a Python script.
3. Each script is reponsible for extracting data from different sources, transforming the data, then loading that data into a Postgres table.
4. The following containers are ran at the following schedules:

| city_coordinates | country_statistics | population | city_weather |
| ---------------- | ------------------ | ---------- | ------------ |
| `@weekly`        | `@monthly`         | `@weekly`  | `@hourly`    |

5. After the Postgres tables are loaded, these tables are then uploaded to BigQuery.
6. Visiualizations displaying the data are created in Looker Studio.

### Pipeline Flowchart

## Services Used

* **APIs:** [WeatherStack](https://weatherstack.com), [OpenWeatherMap](https://openweathermap.org), Wikipedia Tables
* **CI/CD:** [GitHub Actions](https://github.com/features/actions)
* **Containerization:** [Docker](https://www.docker.com/)
* **Container Registry:** [Docker Hub](https://hub.docker.com)
* **Data Warehouse:** [BigQuery](https://cloud.google.com/bigquery/)
* **Orchestration:** [Airflow](https://airflow.apache.org)
* **Relational Database:** [PostgreSQL](https://www.postgresql.org)
* **Visualization:** [Looker Studio](https://lookerstudio.google.com)