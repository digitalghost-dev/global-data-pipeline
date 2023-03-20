# Global Data Pipeline with Docker Compose, PostgreSQL, Airflow and Looker Studio

<div>
    <img alt="Version" src="https://img.shields.io/badge/Project Number-3-orange.svg?cacheSeconds=2592000" />
</div>

## Overiew

* A `docker-compose.yml` file runs Airflow, Postgres, and Redis in Docker containers.
* Python scripts reach out to different data sources to extract, transform and load the data into a Postgres database, orchestrated through Airflow on various schedules.
* Using Airflow operators, data is moved from Postgres to Google Cloud Storage then to BigQuery where the data is visualized with Looker Studio.
* A Discord Airflow operator is used to send a daily message to a Discord server with current weather stats.

## How the Pipeline Works
### Data Pipeline
1. Running `docker-compose up` starts the Airflow services, Postgres, and Redis in Docker containers.
2. Five different Python scripts reach out to different data sources (two APIs, six Wikipedia tables) to extract, transform, and load data to the Postgres database.
    * Data for the 50 most populated cities:
        * Weather
        * Air Quality
        * Population
    * Data for the 30 most populated countries:
        * Fertility rates
        * Homicide rates
        * Human Development Index
        * Obesity reates
        * Unemployment rates
3. The Python scripts are packaged into Docker containers with their dependencies.
4. DAGs are defined with Airflow and perform the following in order:
    1. `DockerOperator` runs the specified container.
    2. `PostgresToGCSOperator` sends the table data to Googe Cloud Storage as a `.csv` file.
    3. `GCSToBigQueryOperator` transfers the `.csv` file to its respective table.
4. The DAGs are ran on the following schedules:

| city_coordinates | country_statistics | population | city_weather | air_quality | discord_alert |
| ---------------- | ------------------ | ---------- | ------------ | ----------- | ------------- |
| `@weekly`        | `@weekly`          | `@weekly`  | `@hourly`    | `@hourly`   | `@daily`      |

5. A connection to BigQuery is established and visiualizations are then created in Looker Studio.
6. A Discord message is sent to a Discord server that contains the current hottest and coldest city (from the 50 most populated list) once a day.

### CI/CD
* When a push is made to this repository, a workflow with [GitHub Actions](https://github.com/features/actions) starts which does the following:
    1. Login to DockerHub. 
    2. New Docker images are built for each directory under `containers/`.
    3. The new Docker images are pushed to a repository on DockerHub.
    4. The Airflow `DiscordOperator` can then run the newly updated images.

### Pipeline Flowchart
![global-data-flowchart](https://storage.googleapis.com/pipeline-flowcharts/global-data-pipeline-flowchart.png)

## Services Used

* **APIs:** [WeatherStack](https://weatherstack.com), [OpenWeatherMap](https://openweathermap.org), Wikipedia Tables
* **CI/CD:** [GitHub Actions](https://github.com/features/actions)
* **Containerization:** [Docker](https://www.docker.com/)
* **Container Registry:** [Docker Hub](https://hub.docker.com)
* **Data Warehouse:** [BigQuery](https://cloud.google.com/bigquery/)
* **Notifications:** [Discord](https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks)
* **Orchestration:** [Airflow](https://airflow.apache.org)
* **Relational Database:** [PostgreSQL](https://www.postgresql.org)
* **Visualization:** [Looker Studio](https://lookerstudio.google.com)