# Webscraping a table with the 100 most populous cities in the world according to UN estimates.
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/tmp/keys/keys.json"

# Importing needed libraries.
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

# Timing how long the script takes to run.
import time
start_time = time.time()

def gcp_secret():
    # Import the Secret Manager client library.
    from google.cloud import secretmanager

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    DATABASE_URL = "projects/463690670206/secrets/DATABASE_URL/versions/1"

    # Access the secret version.
    response = client.access_secret_version(request={"name": DATABASE_URL})

    payload = response.payload.data.decode("UTF-8")

    return payload

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

        return city_dataframe

    else:
        print("There was a connection error: " + str(response.status_code))

# Scraping a Wikipedia page that has a table of countries ordered by population.
def country_populations():
    url = 'https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population'
    response = requests.get(url)

    # Checking if url returns a <200> status code.
    if response.status_code == requests.codes.ok:
        data = requests.get(url).text

        # Creating BeautifulSoup object.
        soup = BeautifulSoup(data, 'html.parser')

        # Selecting the table from the website.
        table = soup.find('table', class_='wikitable')

        # Using Pandas to read the HTML table.
        country_dataframe = pd.read_html(str(table))[0]

        # Removing rows to only return the top 31 rows.
        extra_rows = (country_dataframe.shape[0]) - 31

        # Removing the extra rows.
        country_dataframe = country_dataframe.drop(country_dataframe.tail(extra_rows).index)
        
        # Dropping unwanted columns.
        country_dataframe = country_dataframe.drop(country_dataframe.columns[3:], axis=1)

        # Renaming column headers.
        country_dataframe.columns = range(country_dataframe.shape[1])
        mapping = {0:'Rank', 1:'Country', 2:"Population"}
        country_dataframe = country_dataframe.rename(columns=mapping)

        # Removing the first row.
        country_dataframe = country_dataframe.tail(-1)

        return country_dataframe

    else:
        print("There was a connection error: " + str(response.status_code))

# Loading both population dataframes into the Postgres database.
def load_population(DATABASE_URI):
    city_dataframe = city_populations()
    country_dataframe = country_populations()

    engine = create_engine(DATABASE_URI)

    dataframes = [city_dataframe, country_dataframe]
    tables = ['city_populations', 'country_populations']
    
    # Looping through to upload both dataframes.
    count = 0
    while count < 2:
        # Sending city_dataframe to table in PostgreSQL.
        dataframes[count].to_sql(tables[count], engine, if_exists='replace', index=False)

        count += 1
    
    print("Process completed!")

payload = gcp_secret()
load_population(payload)

print(f'{(time.time() - start_time)} seconds')