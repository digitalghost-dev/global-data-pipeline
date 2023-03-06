# Using the OpenWeatherMap Geocoding API, this file is responsible for building the city_coordinates table.

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

# Fetching API key from Google Cloud's Secret Manager.
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

# Grabbing the population table from the database to the 50 most populous cities in the world.
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
    payload_key = gcp_openweathermap_secret()

    lat_temp = []
    lon_temp = []

    count = 0
    while count < 50:
        url = f'http://api.openweathermap.org/geo/1.0/direct?q={city_list[count]}&limit=1&appid={payload_key}'
        r = requests.get(url)

        lat_temp.append(r.json()[0]['lat'])
        lon_temp.append(r.json()[0]['lon'])

        count += 1

    lat = [round(num, 4) for num in lat_temp]
    lon = [round(num, 4) for num in lon_temp]
    
    return city_list, lat, lon

# Creating a dataframe with the city name and its latitude and longitude.
def create_dataframe():
    city_list, lat, lon = api_call()

    # Setting the headers then zipping the lists to create a dataframe.
    headers = ['city', 'lat', 'lon']
    zipped = list(zip(city_list, lat, lon))

    coordinates_dataframe = pd.DataFrame(zipped, columns = headers)

    # Creating a new column with the latitude and longitude together, this is needed for Looker Studio to plot the coordinates.
    cols = ['lat', 'lon']
    coordinates_dataframe['coordinates'] = coordinates_dataframe[cols].astype(str).apply(','.join, axis=1)

    return coordinates_dataframe 

# # Loading the dataframe into the Postgres database.
def load_coordinates(DATABASE_URI):
    dataframe = create_dataframe()

    engine = create_engine(DATABASE_URI)
    
    # Sending dataframe to table in PostgreSQL.
    dataframe.to_sql('gd.city_coordinates', engine, if_exists='replace', index=False)
    print("Process completed!")

payload_db = gcp_database_secret()
load_coordinates(payload_db)

print(f'{(time.time() - start_time)} seconds')