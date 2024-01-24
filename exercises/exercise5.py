from urllib.request import urlretrieve
import pandas as pd
import zipfile
from sqlalchemy import create_engine

urlretrieve('https://gtfs.rhoenenergie-bus.de/GTFS.zip', 'GTFS.zip')
with zipfile.ZipFile('GTFS.zip', 'r') as zip_ref:
    zip_ref.extractall('data/GTFS')

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

stops_df = pd.read_csv('data/GTFS/stops.txt', delimiter=",", encoding="UTF-8", dtype=str)
# Only the columns stop_id, stop_name, stop_lat, stop_lon, zone_id:
stops_df = stops_df[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]

#Only keep stops from zone 2001
stops_df = stops_df[stops_df['zone_id'] == '2001']

#stop_name can be any text, check via regex:
stops_df = stops_df[stops_df['stop_name'].str.contains(r'[a-zA-Z0-9äöüÄÖÜ]+', regex=True)]

#stop_lat/stop_lon must be a geographic coordinates between -90 and 90, including upper/lower bounds
stops_df = stops_df[stops_df['stop_lat'].astype(float) >= -90]
stops_df = stops_df[stops_df['stop_lat'].astype(float) <= 90]
stops_df = stops_df[stops_df['stop_lon'].astype(float) >= -90]
stops_df = stops_df[stops_df['stop_lon'].astype(float) <= 90]

#Drop rows containing invalid data
stops_df = stops_df.dropna()

#Insert data into SQLite database
engine = create_engine("sqlite:///gtfs.sqlite")
insert_data(engine, 'stops', stops_df)