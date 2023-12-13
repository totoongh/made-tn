import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, Float

### Define URL and SQLite URL
dataset_url = 'https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv'
metadata_url = ''
sqlite_url = "sqlite:///cars.sqlite"

### Define the table column names
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
    }


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
    
def rename_columns(dataframe, rename_dictionary):
    """
    Change the columns of the dataframe.

    Args:
    dataframe (pd.df): The dataframe to be changed
    renamce_dictionary (dict): The dictionary containing the column + names.

    Returns:
    DataFrame: A pandas df that contains the loaded data.
    """

    # Rename the columns
    dataframe.rename(columns=rename_dictionary, inplace=True)
    return dataframe

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

if __name__ == "__main__":
    ### Read CSV and only keep columns as stated by the description
    daf = pd.read_csv(dataset_url, delimiter=";", skiprows=7, skipfooter=4, header=None, encoding="ISO-8859-1", dtype=str)
    keepcols= [0,1,2,12,22,32,42,52,62,72]
    daf = daf[keepcols]

    ### Check if the CIN is valid (5 digits)
    validate_cin_mask = daf[1].str.len() == 5
    daf = daf[validate_cin_mask]

    ### Check if all columns (except first three) are integers (and convert numbers to integers for SQL table)
    for col in keepcols[3:]:
        daf[col] = pd.to_numeric(daf[col], errors='coerce', downcast='integer').astype('Int64')

    ### else drop the columns.
    daf.dropna(inplace=True)

    ###rename the columns
    daf = rename_columns(daf, rename_dict)

    ### Write columns to SQL database.
    engine = create_engine(sqlite_url)
    insert_data(engine, 'cars', daf)
