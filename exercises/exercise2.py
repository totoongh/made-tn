import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, Float
import urllib.request
from zipfile import ZipFile

dd = pd.read_csv("http://openpsychometrics.org/_rawdata/16PF.zip", compression='zip')


def load_data(url, delimiter):
    """
    Load data from an online CSV file.

    Args:
    url (str): The URL of the online CSV file.
    delimiter (str): The delimiter used in the CSV.

    Returns:
    DataFrame: A pandas df that contains the loaded data.
    """
    try:
        return pd.read_csv(url, delimiter=delimiter)
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def create_database(engine_url):
    """
    Create a SQLite database and define its structure.

    Args:
    engine_url (str): The URL for connecting to the SQLite database.

    Returns:
    engine, metadata: The SQLAlchemy engine and metadata for the database.
    """
    try:
        engine = create_engine(engine_url)
        metadata = MetaData()

        airports_table = Table('airports', metadata,
            Column('column_1', Integer),  # Lfd. Nummer
            Column('column_2', Text),     # Name des Flughafens
            Column('column_3', Text),     # Ort
            Column('column_4', Text),     # Land
            Column('column_5', Text),     # IATA
            Column('column_6', Text),     # ICAO
            Column('column_7', Float),    # Latitude
            Column('column_8', Float),    # Longitude
            Column('column_9', Integer),  # Altitude
            Column('column_10', Float),   # Zeitzone
            Column('column_11', Text),    # DST
            Column('column_12', Text),    # Zeitzonen-Datenbank
            Column('geo_punkt', Text)     # geo_punkt
        )

        metadata.create_all(engine)
        return engine, metadata
    except Exception as e:
        print(f"Error creating database: {e}")
        return None, None

def insert_data(engine, table_name, data):
    """
    Insert data into a SQLite database table.

    Args:
    engine: The SQLAlchemy engine used to connect to the database.
    table_name (str): The name of the table where data will be inserted.
    data (DataFrame): The pandas DataFrame containing the data to insert.
    """
    try:
        with engine.connect() as connection:
            data.to_sql(table_name, con=connection, if_exists='replace', index=False)
    except Exception as e:
        print(f"Error inserting data: {e}")

def main():
    """
    Main function that calls the other functinos to load data from a CSV file and insert it into a SQLite database
    """
# URL of the rhein-kreis-neuss-flughafen-weltweit dataset
# The metadata is for myself.
metadata_url = 'https://opendata.rhein-kreis-neuss.de/explore/dataset/rhein-kreis-neuss-flughafen-weltweit/information/?sort=-column_1'
dataset_url = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv'
sqlite_url = "sqlite:///museumsproject.sqlite"

museumdata_url = "https://www.egmus.eu/fileadmin/csv_temp/egmus_export_22-11-2023.csv"
museumdata_url="https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/earn_ses_agt01?format=SDMX-CSV&compressed=true"
# Fetch the data using ; as a delimiter (as stated by the description)

df = load_data(museumdata_url, delimiter=";")



if df is not None:
    engine, metadata = create_database(sqlite_url)
    if engine is not None and metadata is not None:
        insert_data(engine, 'airports', df)

if __name__ == "__main__":
    main()