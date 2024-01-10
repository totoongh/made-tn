import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, Float
import urllib.request
from io import BytesIO
from zipfile import ZipFile

### URLS
url1 = "http://openpsychometrics.org/_rawdata/16PF.zip"
url2 = "data/DataForFigure2.1WHR2023.csv"

### LOAD DATA FROM THE INTERNET

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
    

### PREPROCESS DATAFRAMES
    
def preprocess_personality_df(df):
    """
    Preprocesses the personality DataFrame.

    Args:
    df (DataFrame): The personality DataFrame to be preprocessed.

    Returns:
    DataFrame: The preprocessed DataFrame.
    """
    # Drop columns gender, age, source and elapsed from df, also drop the given index.
    df = df.drop(columns=[ 'gender', 'age', 'source'])

    # Rename column "country" to "country_code".
    df = df.rename(columns={'country': 'country_code'})

    # Drop all rows where elapsed is less than 60.
    df = df[df['elapsed'] >= 163]

    # Only keep rows where value in column 'accuracy' is between 80 and 100.
    df = df[(df['accuracy'] >= 80) & (df['accuracy'] <= 100)]

    # Drop all rows which have a zero-value in ANY column.
    df = df[(df != 0).all(axis=1)]

    # Group by column "country".
    # For each country, count how often it occurs in the original dataset and add this as a new column "count".
    # Make this a dataframe with count column.
    occurances = df.groupby('country_code').size().to_frame('count')

    #Here, drop the countries where the count is less than 20 (for reliability).
    occurances = occurances[occurances['count'] >= 20]

    df = df.groupby('country_code').mean()

    # Merge the two DataFrames on the country column. 
    df = pd.merge(df, occurances, on='country_code', how='inner')

    # Make it so that the dataframe is a real dataframe and not grouped by (i.e., that the country column remains a column).
    df = df.reset_index()

    return df

def preprocess_worldhappiness_df(df):
    # Only keep country name and ladder score columns.
    df = df[['Country name', 'Ladder score']]


    # Read country codes.txt into a DataFrame; a row is in the format AD,"Andorra"
    country_codes = pd.read_csv('data/Country codes.txt', delimiter=',', names=['country_code', 'Country name'])

    # Merge the two DataFrames on the country column.
    df = pd.merge(df, country_codes, on='Country name', how='left')

    # Make the country_code of Namibia NA instead of NaN.
    df.loc[df['Country name'] == 'Namibia', 'country_code'] = 'NA'

    # Remove the row where country_code is NaN (but not NA). This effectively only removes the row for Kosovo,
    # which is not in the personality dataset and most likely is part of the Serbia row.
    df = df[df['country_code'].notna()]

    return df


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

personality_df_raw = load_zipped_csv(url1, 'data/personality', '16PF/data.csv', '\t')
df2 = load_data(url2, ";")

personality_df_preprocessed = preprocess_personality_df(personality_df_raw)
worldhappiness_df_preprocessed = preprocess_worldhappiness_df(df2)
# Make ladder score a float.
worldhappiness_df_preprocessed['Ladder score'] = worldhappiness_df_preprocessed['Ladder score'].str.replace(',','.').astype(float)

engine = create_engine('sqlite:///project.sqlite')
insert_data(engine, 'personality', personality_df_preprocessed)
insert_data(engine, 'worldhappiness', worldhappiness_df_preprocessed)