# Webscraping a table with the 100 most populous cities in the world according to UN estimates.

# Importing needed libraries.
import toml
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

# Timing how long the script takes to run.
import time
start_time = time.time()

# Loading key-value pairs from config.toml
USER = toml.load("./config.toml")
PASSWORD = toml.load("./config.toml")
NAME = toml.load("./config.toml")

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

load_population(f'postgresql+psycopg2://{USER["DATABASE_USER"]}:{PASSWORD["DATABASE_PASSWORD"]}@localhost/{NAME["DATABASE_NAME"]}')

print(f'{(time.time() - start_time)} seconds')