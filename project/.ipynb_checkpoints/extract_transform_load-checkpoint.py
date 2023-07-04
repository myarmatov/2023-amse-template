import pandas as pd
import io
import requests
from sqlalchemy import create_engine

url1 = "https://offenedaten-konstanz.de/sites/default/files/Zaehlstelle_Herose_2019_stuendlich_Wetter.csv"
encoding1 = "ISO-8859-1"
url2 = "https://bicycle.vlba.uni-oldenburg.de/csv?region=Konstanz&table=zaehl&startDate=2019-01-01&endDate=2019-12-31&stations=Herosépark&columns=countTo&columns=countFrom&columns=classification"
encoding2 = "utf-8"



def extract(url, encoding, withRequest=False):
    if withRequest == True:
        #For Datasource 2
        response = requests.get(url)
        response.raise_for_status()
        content = response.content.decode(encoding)
        df = pd.read_csv(io.StringIO(content))

    else:
        #For Datasource 1
        df = pd.read_csv(url, encoding=encoding, sep=';')

    return df



def transform(df):
    ###Transform
    #we already know the station's name and we got data from only that station in Konstanz
    if 'station' in list(df.columns):
        df = df.drop('station', axis=1)

    return df



def load(df, name):

    # Create a connection to the SQLite database using SQLAlchemy
    database_uri = 'sqlite:///' + name + '.sqlite'
    engine = create_engine(database_uri)

    # Save the DataFrame to the SQL table
    df.to_sql(name, engine, if_exists='replace', index=False)

    # Close the database connection
    engine.dispose()



def etl(url, encoding, withRequest=False, name="random"):
    df = extract(url, encoding, withRequest=withRequest)
    df = transform(df)
    load(df, name)


    
etl(url1, encoding1, withRequest=False, name="bikeCountKonstanz_ds1")
etl(url2, encoding2, withRequest=True, name="bikeCountKonstanz_ds2")