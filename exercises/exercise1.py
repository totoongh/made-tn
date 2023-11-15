import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, Float

# URL of the rhein-kreis-neuss-flughafen-weltweit dataset
# The metadata is for my self.
metadata_url = 'https://opendata.rhein-kreis-neuss.de/explore/dataset/rhein-kreis-neuss-flughafen-weltweit/information/?sort=-column_1'
dataset_url = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv'
# Fetch the data using ; as a delimiter (as stated by the description
df = pd.read_csv(dataset_url, delimiter=";")

## Display the first few rows to examine the data
#print(df.head())

# Define the URL of SQLite
sqlite_url = "sqlite:///airports.sqlite"

# Create an SQLite engine
sql_engine = create_engine(sqlite_url)

# Define metadata for the db
metadata = MetaData()

# Define the table structure
# Die Informationen der Columns habe ich aus der Beschreibung der CSV-Datei (siehe metadata_url)
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

# Create the table
metadata.create_all(sql_engine)

# Write the DataFrame to the SQLite table. If data already exists, then overwrite the data!
df.to_sql('airports', con=sql_engine, if_exists='replace', index=False)