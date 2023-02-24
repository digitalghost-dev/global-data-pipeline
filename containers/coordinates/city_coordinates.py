# Using the OpenWeatherMap Geocoding API, this file is responsible for building the location table.

# Importing needed libraries.
import toml
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# Timing how long the script takes to run.
import time
start_time = time.time()

# Loading key-value pairs from config.toml
USER = toml.load("./config.toml")
PASSWORD = toml.load("./config.toml")
NAME = toml.load("./config.toml")

# Grabbing the population table from the database to the 100 most populous cities in the world.
def database_call():
    engine = create_engine(f'postgresql+psycopg2://{USER["DATABASE_USER"]}:{PASSWORD["DATABASE_PASSWORD"]}@localhost/{NAME["DATABASE_NAME"]}')
    connection = engine.connect()

    query = text("SELECT population.\"City\" FROM population")
    conn = connection.execute(query)
    response = conn.all()

    response_list = (response[0:50])
    city_list = []

    for city in response_list:
        city = str(city).strip("'(),")
        city_list.append(city)

    return city_list

# Calling the OpenWeatherMap Geocoding API to get the latitude and longitude for each city from the city list.
def api_call():
    city_list = database_call()

    # Loading API key.
    KEY = toml.load("./config.toml")
    key = KEY["OPENWEATHER_KEY"]

    lat = []
    lon = []

    count = 0
    while count < 50:
        url = f'http://api.openweathermap.org/geo/1.0/direct?q={city_list[count]}&limit=1&appid={key}'
        r = requests.get(url)

        lat.append(r.json()[0]['lat'])
        lon.append(r.json()[0]['lon'])

        count += 1
    
    return city_list, lat, lon

def create_dataframe():
    city_list, lat, lon,  = api_call()

    # Setting the headers then zipping the lists to create a dataframe.
    headers = ['city', 'lat', 'lon']
    zipped = list(zip(city_list, lat, lon))

    dataframe = pd.DataFrame(zipped, columns = headers)

    cols = ['lat', 'lon']
    dataframe['location'] = dataframe[cols].astype(str).apply(','.join, axis=1)

    return dataframe

def database_load(DATABASE_URI):
    dataframe = create_dataframe()

    engine = create_engine(DATABASE_URI)
    
    # Sending dataframe to table in PostgreSQL.
    dataframe.to_sql('location', engine, if_exists='replace', index=False)
    print("Process completed!")

database_load(f'postgresql+psycopg2://{USER["DATABASE_USER"]}:{PASSWORD["DATABASE_PASSWORD"]}@localhost/{NAME["DATABASE_NAME"]}')

print(f'{(time.time() - start_time)} seconds')