# Global Data Pipeline with Docker Compose, PostgreSQL, Airflow and Looker Studio

<div>
    <img alt="Version" src="https://img.shields.io/badge/Project Number-3-orange.svg?cacheSeconds=2592000" />
</div>

## Overiew

* A `docker-compose.yml` file runs Airflow, Postgres, and Redis in Docker containers.
* Python scripts reach out to different data sources to extract, transform and load the data into a Postgres database, orchestrated through Airflow on various schedules.
* Using Airflow operators, data is moved from Postgres to Google Cloud Storage then to BigQuery where the data is visualized with Looker Studio.
* A Discord Airflow operator is used to send a daily message to a server with current weather stats.


### Important Links
* Visualization
* Documentation

## How the Pipeline Works
### Data Pipeline
1. Run `docker-compose up`.
2. The Airflow services, Postgres, and Redis are ran in Docker containers.
3. Five different Python scripts reach out to different data sources (two APIs, six Wikipedia tables) to extract, transform, and load data to the Postgres database. All scripts are packaged into Docker containers.
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
4. The containers are ran with the `DockerOperator` and the `DiscordOperator` sends an alert to a Discord server.
5. The `PostgresToGCSOperator` sends the table data to Googe Cloud Storage as a `.csv` file.
6. The `GCSToBigQueryOperator` transfers the `.csv` file to its respective table in BigQuery.
7. The data is visualized in Looker Studio.

### DAG schedules:

| city_coordinates | country_statistics | population | city_weather | air_quality | discord_alert |
| ---------------- | ------------------ | ---------- | ------------ | ----------- | ------------- |
| `@weekly`        | `@weekly`          | `@weekly`  | `@hourly`    | `@hourly`   | `0 */4 * * *` |

### CI/CD
CI/CD is setup with [GitHub Actions](https://github.com/features/actions).
1. Detect a change on `main` branch.
2. New Docker images are built for each directory under `containers/` and pushed to DockerHub.
3. The Airflow DAGs can now run the newly updated images.

### Pipeline Flowchart
![global-data-flowchart](https://storage.googleapis.com/pipeline-flowcharts/global-data-pipeline-flowchart.png)

## Services Used

* **APIs:** [WeatherStack](https://weatherstack.com), [OpenWeatherMap](https://openweathermap.org)
* **Webscraping:** Wikipedia Tables
* **CI/CD:** [GitHub Actions](https://github.com/features/actions)
* **Containerization:** [Docker](https://www.docker.com/)
* **Container Registry:** [Docker Hub](https://hub.docker.com)
* **Google Cloud Services:**
    * **Data Warehouse:** [BigQuery](https://cloud.google.com/bigquery/)
    * **Object Storage:** [Cloud Storage](https://cloud.google.com/storage)
    * **Visualization:** [Looker Studio](https://lookerstudio.google.com)
* **Notifications:** [Discord](https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks)
* **Orchestration:** [Airflow](https://airflow.apache.org)
* **Relational Database:** [PostgreSQL](https://www.postgresql.org)
