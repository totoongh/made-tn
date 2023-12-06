import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, Float

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

def create_database(engine_url, table):
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
        
        cars = Table('cars', metadata,
            Column('date', Integer),  # Lfd. Nummer
            Column('CIN', Text),     # Name des Flughafens
            Column('name', Text),     # Ort
            Column('petrol', Text),     # Land
            Column('diesel', Text),     # IATA
            Column('gas', Text),     # ICAO
            Column('electro', Float),    # Latitude
            Column('hybrid', Float),    # Longitude
            Column('plugInHybrid', Integer),  # Altitude
            Column('others', Float),   # Zeitzone
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
    pass
    """
    Main function that calls the other functinos to load data from a CSV file and insert it into a SQLite database
    """
# URL of the rhein-kreis-neuss-flughafen-weltweit dataset
# The metadata is for myself.
dataset_url = 'https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv'
metadata_url = ''
sqlite_url = "sqlite:///cars.sqlite"

daf = pd.read_csv(dataset_url, delimiter=";", skiprows=7, skipfooter=4, header=None, encoding="ISO-8859-1", dtype=str)
keepcols= [0,1,2,12,22,32,42,52,62,72]
daf = daf[keepcols]
validate_cin_mask = daf[1].str.len() == 5
daf = daf[validate_cin_mask]

for col in keepcols[3:]:
    daf[col] = pd.to_numeric(daf[col], errors='coerce', downcast='integer').astype('Int64')

daf.dropna(inplace=True)

# Assuming daf is your DataFrame
# Create a dictionary mapping current column names (or indices) to new names
rename_dict = {
    0: 'date',
    1: 'CIN',
    2: 'name',
    12: 'petrol',
    22: 'diesel',
    32: 'gas',
    42: 'electro',
    52: 'hybrid',
    62: 'plugInHybrid',
    72: 'others',
    

    # ... add other columns as needed
}

# Rename the columns
daf.rename(columns=rename_dict, inplace=True)


engine = create_engine(sqlite_url)
insert_data(engine, 'cars', daf)

# Fetch the data using ; as a delimiter (as stated by the description)

#if df is not None:
#    engine, metadata = create_database(sqlite_url)
#    if engine is not None and metadata is not None:
#        insert_data(engine, 'cars', df)

if __name__ == "__main__":
    main()