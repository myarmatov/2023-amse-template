import pandas as pd
import io
import requests
from sqlalchemy import create_engine

def save_dfToSql(df, name):

    # Create a connection to the SQLite database using SQLAlchemy
    database_uri = 'sqlite:///' + name + '.sqlite'
    engine = create_engine(database_uri)

    # Save the DataFrame to the SQL table
    df.to_sql(name, engine, if_exists='replace', index=False)

    # Close the database connection
    engine.dispose()


###Extract

#Datasource 1
url1 = "https://offenedaten-konstanz.de/sites/default/files/Zaehlstelle_Herose_2019_stuendlich_Wetter.csv"
encoding1 = "ISO-8859-1"
df1 = pd.read_csv(url1, encoding = encoding1, sep=';')


#Datasource 2

url2 = "https://bicycle.vlba.uni-oldenburg.de/csv?region=Konstanz&table=zaehl&startDate=2019-01-01&endDate=2019-12-31&stations=Herosépark&columns=countTo&columns=countFrom&columns=classification"
encoding2 = "utf-8"

response = requests.get(url2)
response.raise_for_status()

# Create a pandas DataFrame from the downloaded content
content = response.content.decode(encoding2)
df2 = pd.read_csv(io.StringIO(content))




###Transform
#we already know the station's name and we got data from only that station in Konstanz
df2 = df2.drop('station', axis=1)


###Load
save_dfToSql(df1, "bikeCountKonstanz_ds1")
save_dfToSql(df2, "bikeCountKonstanz_ds2")



