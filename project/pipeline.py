import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, Float
import os
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


mobilithek_wetterereignisse_url = "https://mobilithek.info/mdp-api/files/aux/607957931775647744/Resultat_HotSpot_Analyse_neu.csv"
mobilithek_geode_url = "https://simplemaps.com/static/data/country-cities/de/de.csv"

wetter_data_df = load_data(mobilithek_wetterereignisse_url, delimiter=";")
geode_data_df = load_data(mobilithek_geode_url, delimiter=",")

database_path = os.path.join('data', 'projectdb.db')

# Step 2: Create an engine to the SQLite database at the specified path
engine = create_engine(f'sqlite:///{database_path}')

metadata = MetaData()
wettertable =  Table('geodedata', metadata,
            Column('city', Text),  # Lfd. Nummer
            Column('latitude', Float),     # Name des Flughafens
            Column('longitude', Float),     # Ort
            Column('country', Text),     # Land
            Column('iso2', Text),     # IATA
            Column('admin_name', Text),     # ICAO
            Column('capital', Float),    # Latitude
            Column('population', Integer),    # Longitude
            Column('population_proper', Integer),  # Altitude
        )
metadata.create_all(engine)

with engine.connect() as connection:
    try:
        wetter_data_df.to_sql('wetterdata', con=connection, if_exists='replace', index=False)
        geode_data_df.to_sql('geode', con=connection, if_exists='replace', index=False)
    except Exception as e:
        print(f"Error inserting data: {e}")

pass
