# Webscraping a table with the 100 most populated cities in the world according to UN estimates.

# Running this command at the start of the script to authenticate with Google Cloud.
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/tmp/keys/keys.json"

# Importing needed libraries.
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from google.cloud import secretmanager

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

# Scraping a Macrotrends page that has a table of cities ordered by population.
def city_populations():
    url = 'https://www.macrotrends.net/cities/largest-cities-by-population'
    response = requests.get(url)

    # Checking if url returns a <200> status code.
    if response.status_code == requests.codes.ok:
        data = requests.get(url).text

        # Creating BeautifulSoup object.
        soup = BeautifulSoup(data, 'html.parser')

        # Selecting the World Cities table from the website.
        table = soup.find('table', id='world_cities')

        # Using Pandas to read the HTML table.
        city_dataframe = pd.read_html(str(table))[0]

        # Removing rows to only return the top 50 rows.
        extra_rows = (city_dataframe.shape[0]) - 50

        # Removing the extra rows.
        city_dataframe = city_dataframe.drop(city_dataframe.tail(extra_rows).index)

        # Renaming column headers.
        city_dataframe.columns = range(city_dataframe.shape[1])
        mapping = {0: 'rank', 1: 'city', 2: 'country', 3: 'population'}
        city_dataframe = city_dataframe.rename(columns=mapping)

        # Removing commas and characters after commas where type == 'str'.
        city_dataframe = city_dataframe.applymap(lambda x: x.split(",")[0] if isinstance(x, str) else x)

        return city_dataframe

    else:
        print("There was a connection error: " + str(response.status_code))

# Loading dataframes into the Postgres database.
def load_population(DATABASE_URI):
    city_dataframe = city_populations()

    engine = create_engine(DATABASE_URI)

    # Sending dataframe to table in PostgreSQL.
    city_dataframe.to_sql('gd.city_pop', engine, if_exists='replace', index=False)
    
    print("Process completed!")

payload = gcp_database_secret()
load_population(payload)

print(f'{(time.time() - start_time)} seconds')