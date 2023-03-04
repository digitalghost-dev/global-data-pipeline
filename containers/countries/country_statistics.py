# Webscraping tables with statistics for the 30 most populated countries in the world.

# Running this command at the start of the script to authenticate with Google Cloud.
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

# Fetching API key from Google Cloud's Secret Manager.
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

# This function builds the fertility rates table.
def fertility_rate():
    url = 'https://en.wikipedia.org/wiki/List_of_sovereign_states_and_dependencies_by_total_fertility_rate'
    response = requests.get(url)

    # Checking if url returns a <200> status code.
    if response.status_code == requests.codes.ok:
        data = requests.get(url).text
        # Creating BeautifulSoup object.
        soup = BeautifulSoup(data, 'html.parser')
        table = soup.find('table', class_='wikitable')

        # Using Pandas to read the HTML table.
        fertility_dataframe = pd.read_html(str(table))[0]

        # Dropping extra rows and columns.
        extra_rows = (fertility_dataframe.shape[0]) - 195
        fertility_dataframe = fertility_dataframe.drop(fertility_dataframe.tail(extra_rows).index)
        fertility_dataframe = fertility_dataframe.drop(fertility_dataframe.columns[3:], axis=1)

        # Updating column names.
        fertility_dataframe.columns = range(fertility_dataframe.shape[1])
        mapping = {0: 'rank', 1: 'country', 2: "fertility_rate"}
        fertility_dataframe = fertility_dataframe.rename(columns=mapping)

        # Removing extra characters and digits from rank column.
        fertility_dataframe['rank'] = fertility_dataframe['rank'].map(lambda x: str(x)[:3])
        fertility_dataframe['rank'] = fertility_dataframe['rank'].str.strip()

        # Setting appropriate data types.
        fertility_dataframe['fertility_rate'] = fertility_dataframe['fertility_rate'].astype(float)

        # Removing unnecessary rows.
        fertility_dataframe = fertility_dataframe[~fertility_dataframe.country.str.contains('World')]
        fertility_dataframe = fertility_dataframe[~fertility_dataframe.country.str.contains('Population')]

        return fertility_dataframe

    else:
        print("Couldn't build Fertility Rates table. Error: " + str(response.status_code))

# This function builds the unemployment rates table.
def unemployment_rate():
    url = 'https://en.wikipedia.org/wiki/List_of_sovereign_states_by_unemployment_rate'
    response = requests.get(url)

    # Checking if url returns a <200> status code.
    if response.status_code == requests.codes.ok:
        data = requests.get(url).text
        # Creating BeautifulSoup object.
        soup = BeautifulSoup(data, 'html.parser')
        table = soup.find('table', class_='wikitable')

        pd.set_option("display.max_rows", 223)

        # Using Pandas to read the HTML table.
        unemployment_dataframe = pd.read_html(str(table))[0]
        
        # Dropping extra columns.
        unemployment_dataframe = unemployment_dataframe.drop(unemployment_dataframe.columns[3:], axis=1)
        
        # Updating column names.
        unemployment_dataframe.columns = range(unemployment_dataframe.shape[1])
        mapping = {0: 'country', 1: 'unemployment_rate', 2: 'last_updated'}
        unemployment_dataframe = unemployment_dataframe.rename(columns=mapping)

        # Removing extra characters from country column.
        unemployment_dataframe['country'] = unemployment_dataframe['country'].str.replace(r'\*.*', '', regex=True)
        unemployment_dataframe['country'] = unemployment_dataframe['country'].str.strip()
        unemployment_dataframe['last_updated'] = unemployment_dataframe['last_updated'].str[-4:]

        return unemployment_dataframe

    else:
        print("Couldn't build Unemployment Rates table. Error: " + str(response.status_code))

# This function builds the homicide rates table.
def homicide_rate():

    url = "https://en.wikipedia.org/wiki/List_of_countries_by_intentional_homicide_rate"
    response = requests.get(url)

    # Checking if url returns a <200> status code.
    if response.status_code == requests.codes.ok:
        data = requests.get(url).text
        # Creating BeautifulSoup object.
        soup = BeautifulSoup(data, 'html.parser')
        table = soup.find('table', class_='static-row-numbers')

        # Using Pandas to read the HTML table.
        homicide_dataframe = pd.read_html(str(table))[0]

        # Dropping extra rows and columns.
        homicide_dataframe = homicide_dataframe.drop(index=0)
        homicide_dataframe = homicide_dataframe.drop(homicide_dataframe.columns[6:], axis=1)

        # Updating column names.
        homicide_dataframe.columns = range(homicide_dataframe.shape[1])
        mapping = {0: 'country', 1: 'region', 2: 'subregion', 3: 'rate', 4: 'count', 5: 'last_updated'}
        homicide_dataframe = homicide_dataframe.rename(columns=mapping)

        # Removing extra characters from country column.
        homicide_dataframe['country'] = homicide_dataframe['country'].str.replace(r'\*.*', '', regex=True)
        homicide_dataframe['country'] = homicide_dataframe['country'].str.strip()

        # Setting appropriate data types.
        homicide_dataframe['count'] = homicide_dataframe['count'].astype(int)
        homicide_dataframe['last_updated'] = homicide_dataframe['last_updated'].astype(int)
        
        return homicide_dataframe

    else:
        print("Couldn't build Homicide Rates table. Error: " + str(response.status_code))

# This function builds the obesity rates table.
def obesity_rate():

    url = "https://en.wikipedia.org/wiki/List_of_countries_by_obesity_rate"
    response = requests.get(url)

    # Checking if url returns a <200> status code.
    if response.status_code == requests.codes.ok:
        data = requests.get(url).text
        # Creating BeautifulSoup object.
        soup = BeautifulSoup(data, 'html.parser')
        table = soup.find('table', class_='static-row-numbers')

        # Using Pandas to read the HTML table.
        obesity_dataframe = pd.read_html(str(table))[0]

        # Updating column names.
        obesity_dataframe.columns = range(obesity_dataframe.shape[1])
        mapping = {0: 'country', 1: 'obesity_rate_percentage'}
        obesity_dataframe = obesity_dataframe.rename(columns=mapping)

        # Removing whitespace.
        obesity_dataframe['country'] = obesity_dataframe['country'].str.strip()
        
        return obesity_dataframe
    else:
        print("Couldn't build Homicide Rates table. Error: " + str(response.status_code))

# This function builds the obesity rates table.
def nominal_gdp_per_capita():

    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)_per_capita"
    response = requests.get(url)

    # Checking if url returns a <200> status code.
    if response.status_code == requests.codes.ok:
        data = requests.get(url).text
        # Creating BeautifulSoup object.
        soup = BeautifulSoup(data, 'html.parser')
        table = soup.find('table', class_='static-row-numbers')

        # Using Pandas to read the HTML table.
        nominal_gdp_dataframe = pd.read_html(str(table))[0]

        # Dropping extra rows and columns.
        nominal_gdp_dataframe = nominal_gdp_dataframe.drop(index=0)
        nominal_gdp_dataframe = nominal_gdp_dataframe.drop(nominal_gdp_dataframe.columns[1:6], axis=1)
        nominal_gdp_dataframe = nominal_gdp_dataframe.drop(nominal_gdp_dataframe.columns[2], axis=1)

        # Updating column names.
        nominal_gdp_dataframe.columns = range(nominal_gdp_dataframe.shape[1])
        mapping = {0: 'country', 1: 'estimate'}
        nominal_gdp_dataframe = nominal_gdp_dataframe.rename(columns=mapping)

        # Removing extra characters from country column.
        nominal_gdp_dataframe['country'] = nominal_gdp_dataframe['country'].str.replace(r'\*.*', '', regex=True)
        nominal_gdp_dataframe['country'] = nominal_gdp_dataframe['country'].str.strip()
        nominal_gdp_dataframe['estimate'] = nominal_gdp_dataframe['estimate'].str.strip()
        
        return nominal_gdp_dataframe

    else:
        print("Couldn't build Homicide Rates table. Error: " + str(response.status_code))

# Loading dataframes into the Postgres database.
def load_population(DATABASE_URI):
    fertility_dataframe = fertility_rate()
    unemployment_dataframe = unemployment_rate()
    homicide_dataframe = homicide_rate()
    obesity_dataframe = obesity_rate()
    nominal_gdp_dataframe = nominal_gdp_per_capita()

    engine = create_engine(DATABASE_URI)

    dataframes = [fertility_dataframe, unemployment_dataframe, homicide_dataframe, obesity_dataframe, nominal_gdp_dataframe]
    tables = ['gd.fertility', 'gd.unemployment', 'gd.homicide', 'gd.obesity', 'gd.nominal_gdp']
    
    # Looping through to upload both dataframes.
    count = 0
    while count < 5:
        # Sending city_dataframe to table in PostgreSQL.
        dataframes[count].to_sql(tables[count], engine, if_exists='replace', index=False)

        count += 1
    
    print("Process completed! " + str(os.path.basename(__file__)) + " " + "finished without errors.")

payload = gcp_secret()
load_population(payload)

print(f'{(time.time() - start_time)} seconds')