# Using the WeatherStack weather API, this file is responsible for building the weather table.

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
def gcp_weatherstack_secret():
    client = secretmanager.SecretManagerServiceClient()
    WEATHERSTACK_KEY = "projects/463690670206/secrets/weatherstack-api/versions/1"
    response = client.access_secret_version(request={"name": WEATHERSTACK_KEY})
    payload_key = response.payload.data.decode("UTF-8")

    return payload_key

# Fetching the population table from the database.
def database_call():
    payload_db = gcp_database_secret()

    engine = create_engine(payload_db)

    with engine.begin() as conn:
        query = text('SELECT * FROM "gd.city_pop"')
        city_population_dataframe = pd.read_sql_query(query, conn)

    city_list = city_population_dataframe['city'].tolist()

    return city_list

# Calling the OpenWeatherMap Geocoding API to get the latitude and longitude for each city from the city list.
def api_call():
    city_list = database_call()
    payload_key = gcp_weatherstack_secret()

    # Empty lists to filled with API data.
    current_temp = []
    wind_speed = []
    precip = []
    humidity = []

    count = 0
    while count < 50:
        url = f'https://api.weatherstack.com/current?access_key={payload_key}&query={city_list[count]}'
        r = requests.get(url)

        current_temp.append(r.json()["current"]["temperature"])
        wind_speed.append(r.json()["current"]["wind_speed"])
        precip.append(r.json()["current"]["precip"])
        humidity.append(r.json()["current"]["humidity"])

        count += 1

    # lat = [round(num, 4) for num in lat_temp]
    
    return city_list, current_temp, wind_speed, precip, humidity

# Creating a dataframe with the city name and its latitude and longitude.
def create_dataframe():
    city_list, current_temp, wind_speed, precip, humidity = api_call()

    # Setting the headers then zipping the lists to create a dataframe.
    headers = ['city', 'current_temp', 'wind_speed', 'precip', 'humidity']
    zipped = list(zip(city_list, current_temp, wind_speed, precip, humidity))

    weather_dataframe = pd.DataFrame(zipped, columns = headers)

    return weather_dataframe

# Loading the dataframe into the Postgres database.
def load_weather(DATABASE_URI):
    weather_dataframe = create_dataframe()

    engine = create_engine(DATABASE_URI)
    
    # Sending dataframe to table in PostgreSQL.
    weather_dataframe.to_sql('gd.city_weather', engine, if_exists='replace', index=False)
    print("Process completed!")

payload_db = gcp_database_secret()
load_weather(payload_db)

print(f'{(time.time() - start_time)} seconds')