# Webscraping a table with the 100 most populous cities in the world according to UN estimates.

# Importing needed libraries.
import toml
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

# Loading key-value pairs from config.toml
USER = toml.load("./config.toml")
PASSWORD = toml.load("./config.toml")
NAME = toml.load("./config.toml")

def call_website():
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
        dataframe = pd.read_html(str(table))[0]

        # Saving the amount of extra rows past 100 to a variable.
        extra_rows = (dataframe.shape[0]) - 100

        # Removing the extra rows.
        dataframe = dataframe.drop(dataframe.tail(extra_rows).index)

        return dataframe

    else:
        print("There was a connection error: " + str(response.status_code))

def load_population(DATABASE_URI):
    dataframe = call_website()

    engine = create_engine(DATABASE_URI)
    
    # Sending dataframe to table in PostgreSQL.
    dataframe.to_sql('population', engine, if_exists='replace', index=False)
    print("Process completed!")

load_population(f'postgresql+psycopg2://{USER["DATABASE_USER"]}:{PASSWORD["DATABASE_PASSWORD"]}@localhost/{NAME["DATABASE_NAME"]}')