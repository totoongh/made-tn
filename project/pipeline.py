import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, Float
import urllib.request
from io import BytesIO
from zipfile import ZipFile

### URLS
url1 = "http://openpsychometrics.org/_rawdata/16PF.zip"
#url2 = "https://happiness-report.s3.amazonaws.com/2023/DataForFigure2.1WHR2023.xls"
url2 = "data/DataForFigure2.1WHR2023.csv"

def load_zipped_csv(url, extractionpath, filename, delimiter):
    """
    Loads a zipped CSV file from a given URL and extracts it to a specified path. 
    Then reads the extracted CSV file and returns a pandas DataFrame.

    Args:
        url (str): The URL of the zipped CSV file.
        extractionpath (str): The path where the zipped CSV file will be extracted.
        filename (str): The name of the CSV.
        delimiter (str): The delimiter used in the CSV.

    Returns:
        pandas.DataFrame: The loaded CSV data as a DataFrame.
    """
    with urllib.request.urlopen(url1) as response:
        with ZipFile(BytesIO(response.read())) as zip_file:
            zip_file.extractall(extractionpath)  # Provide the path where you want to extract
    return pd.read_csv(extractionpath + '/' + filename, delimiter=delimiter)

def load_excel(url):
    return pd.read_excel(url)

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

def create_personality_database(engine_url):
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

def create_world_kpi_database(engine_url):
    pass

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
# Fetch the data using ; as a delimiter (as stated by the description)

df1 = load_zipped_csv(url1, 'data/personality', '16PF/data.csv', '\t')
df2 = load_data(url2, ";")

engine = create_engine('sqlite:///project.sqlite')
insert_data(engine, 'personality', df1)
insert_data(engine, 'worldhappiness', df2)


if __name__ == "__main__":
    main()