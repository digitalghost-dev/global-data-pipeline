# Using the OpenWeatherMap API, this file is responsible for building the air_quality table.

# Running this command at the start of the script to authenticate with Google Cloud.
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/tmp/keys/keys.json"

# Importing needed libraries.
import requests
import pandas as pd
from google.cloud import secretmanager
from sqlalchemy import create_engine, text

# Timing how long the script takes to run.
import time
start_time = time.time()

# Fetching Database URI from Google Cloud's Secret Manager.
def gcp_database_secret():
    client = secretmanager.SecretManagerServiceClient()
    DATABASE_URL = "projects/463690670206/secrets/DATABASE_URL/versions/1"
    response = client.access_secret_version(request={"name": DATABASE_URL})
    payload_db = response.payload.data.decode("UTF-8")

    return payload_db

# Fetching API key from Google Cloud's Secret Manager.
def gcp_openweathermap_secret():
    client = secretmanager.SecretManagerServiceClient()
    OPENWEATHERMAP_KEY = "projects/463690670206/secrets/openweathermap-api/versions/1"
    response = client.access_secret_version(request={"name": OPENWEATHERMAP_KEY})
    payload_key = response.payload.data.decode("UTF-8")

    return payload_key

# Fetching the city_coordinates table from the database.
def database_call():
    payload_db = gcp_database_secret()

    engine = create_engine(payload_db)

    with engine.begin() as conn:
        query = text('SELECT * FROM "gd.city_coordinates"')
        city_coordinates_dataframe = pd.read_sql_query(query, conn)

    city_list = city_coordinates_dataframe['city'].tolist()
    lat_list = city_coordinates_dataframe['lat'].tolist()
    lon_list = city_coordinates_dataframe['lon'].tolist()

    return city_list, lat_list, lon_list

# Calling the OpenWeatherMap Air Quality API.
def api_call():
    city_list, lat_list, lon_list = database_call()
    payload_key = gcp_openweathermap_secret()

    # Empty lists to filled with API data.
    co = []
    no2 = []
    o3 = []
    so2 = []
    pm2_5 = []
    pm10 = []

    count = 0
    while count < 50:
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat_list[count]}&lon={lon_list[count]}&appid={payload_key}"
        r = requests.get(url)

        co.append(r.json()["list"][0]["components"]["co"])
        no2.append(r.json()["list"][0]["components"]["no2"])
        o3.append(r.json()["list"][0]["components"]["o3"])
        so2.append(r.json()["list"][0]["components"]["so2"])
        pm2_5.append(r.json()["list"][0]["components"]["pm2_5"])
        pm10.append(r.json()["list"][0]["components"]["pm10"])

        count += 1

    return city_list, co, no2, o3, so2, pm2_5, pm10

# Creating a dataframe with the air quality data.
def create_dataframe():
    city_list, co, no2, o3, so2, pm2_5, pm10 = api_call()

    # Setting the headers then zipping the lists to create a dataframe.
    headers = ['city', 'CO', 'NO2', 'O3', 'SO2', 'PM2_5', 'PM10']
    zipped = list(zip(city_list, co, no2, o3, so2, pm2_5, pm10))

    air_quality_dataframe = pd.DataFrame(zipped, columns = headers)

    return air_quality_dataframe

# Loading the dataframe into the Postgres database.
def load_air_quality(DATABASE_URI):
    air_quality_dataframe = create_dataframe()

    engine = create_engine(DATABASE_URI)
    
    # Sending dataframe to table in PostgreSQL.
    air_quality_dataframe.to_sql('gd.air_quality', engine, if_exists='replace', index=False)
    print("Process completed!")

payload_db = gcp_database_secret()
load_air_quality(payload_db)

print(f'{(time.time() - start_time)} seconds')