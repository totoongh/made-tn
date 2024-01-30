"""
Exercise 5: GTFS Data Processing (Rhönenergie - Stops)

This script downloads GTFS data from Rhönenergie, cleans it, and inserts it into a SQLite database.
The script focuses on 'stops' data from the provided GTFS zip file.
"""

from urllib.request import urlretrieve
import pandas as pd
import zipfile
from sqlalchemy import create_engine

def download_and_extract(url, zip_path, extract_path):
    """
    Download a zip file from a URL and extract its contents.

    Args:
    url (str): The URL of the zip file.
    zip_path (str): The path to save the zip file.
    extract_path (str): The path to extract the contents of the zip file.
    """
    urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

def clean_stops_data(file_path):
    """
    Clean and prepare stops data from a GTFS dataset.

    Args:
    file_path (str): The file path of the stops.txt file from GTFS data.

    Returns:
    DataFrame: A cleaned and filtered pandas DataFrame containing stops data.
    """
    stops_df = pd.read_csv(file_path, delimiter=",", encoding="UTF-8", dtype=str)
    stops_df = stops_df[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]

    # Using pd.to_numeric for type conversion and filtering
    stops_df['stop_id'] = pd.to_numeric(stops_df['stop_id'], errors='coerce')
    stops_df['zone_id'] = pd.to_numeric(stops_df['zone_id'], errors='coerce')
    stops_df['stop_lat'] = pd.to_numeric(stops_df['stop_lat'], errors='coerce')
    stops_df['stop_lon'] = pd.to_numeric(stops_df['stop_lon'], errors='coerce')

    # Filters: Only keep stops from zone 2001, zone_id and stop_id integer > 0, stop_lat and stop_lon float between -90 and 90, stop_name contains at least one alphanumeric character
    stops_df = stops_df[(stops_df['stop_id'] > 0) & 
                        (stops_df['zone_id'] > 0) & 
                        (stops_df['zone_id'] == 2001) & 
                        (stops_df['stop_lat'].between(-90, 90)) & 
                        (stops_df['stop_lon'].between(-90, 90)) &
                        stops_df['stop_name'].str.contains(r'[a-zA-Z0-9äöüÄÖÜ]+', regex=True)]

    # Drop rows containing invalid data
    stops_df.dropna(inplace=True)

    return stops_df

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

# Main function
if __name__ == "__main__":
    download_and_extract('https://gtfs.rhoenenergie-bus.de/GTFS.zip', 'GTFS.zip', 'data/GTFS')
    stops_df = clean_stops_data('data/GTFS/stops.txt')
    engine = create_engine("sqlite:///gtfs.sqlite")
    insert_data(engine, 'stops', stops_df)
