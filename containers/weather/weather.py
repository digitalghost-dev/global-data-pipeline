# Using the API Ninjas Weather API, this file is responsible for building the weather table.

# Importing needed libraries.
import toml
import requests
import pandas as pd
from sqlalchemy import create_engine, text

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

    response_list = (response[0:25])
    clean_list = []

    for city in response_list:
        city = str(city).strip("'(),")
        clean_list.append(city)

    return clean_list

def api_call():
    clean_list = database_call()

    # Loading API key.
    KEY = toml.load("./config.toml")
    key = KEY["API_NINJAS_KEY"]

    max_temp = []

    count = 0
    while count < 25:
        api_url = f'https://api.api-ninjas.com/v1/weather?city={clean_list[count]}'
        response = requests.get(api_url, headers={'X-Api-Key': key})

        max_temp.append(response.json()["max_temp"])

        count += 1
    
    return clean_list, max_temp

def create_dataframe():
    clean_list, max_temp = api_call()

    # Setting the headers then zipping the lists to create a dataframe.
    headers = ['city', 'max_temp']
    zipped = list(zip(clean_list, max_temp))

    dataframe = pd.DataFrame(zipped, columns = headers)

    return dataframe

def database_load(DATABASE_URI):
    dataframe = create_dataframe()

    engine = create_engine(DATABASE_URI)
    
    # Sending dataframe to table in PostgreSQL.
    dataframe.to_sql('weather', engine, if_exists='replace', index=False)
    print("Process completed!")

database_load(f'postgresql+psycopg2://{USER["DATABASE_USER"]}:{PASSWORD["DATABASE_PASSWORD"]}@localhost/{NAME["DATABASE_NAME"]}')

print(f'{(time.time() - start_time)} seconds')

# °F = (°C × 9/5) + 32