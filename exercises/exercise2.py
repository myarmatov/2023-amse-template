import pandas as pd
from sqlalchemy import create_engine, BIGINT, TEXT, FLOAT

url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = pd.read_csv(url, sep=';')

#dropping Status column
df = df.drop(["Status"], axis = 1)

#dropping none values
df.dropna(inplace=True)

#dropping rows with empty values of Betreiber_Nr
df = df[df["Betreiber_Nr"] >= 0 ]

#dropping rows where the value in the "Verkehr" column is not 'FV', 'RV', or 'nur DPN'
df = df[df['Verkehr'].isin(['FV', 'RV', 'nur DPN'])]

#dropping rows where Lanege and Breite values are not between -90 and 90
df['Laenge'] = df['Laenge'].str.replace(',', '.')
df['Laenge'] = df['Laenge'].astype(float)
df = df[(df["Laenge"] <= 90) & (df["Laenge"] >= -90)]

df['Breite'] = df['Breite'].str.replace(',', '.')
df['Breite'] = df['Breite'].astype(float)
df = df[(df["Breite"] <= 90) & (df["Breite"] >= -90)]

#dropping rows where IFOPT has more than two letter in first pattern
df = df[df['IFOPT'].str[2] == ':']




#creating a SQLAlchemy engine to connect to the SQLite database
engine = create_engine('sqlite:///trainstops.sqlite')

#defining the desired column types for the SQLite database
dtype = {
    'EVA_NR': BIGINT,
    'DS100': TEXT,
    'IFOPT': TEXT,
    'NAME': TEXT,
    'Verkehr': TEXT,
    'Laenge': FLOAT,
    'Breite': FLOAT,
    'Betreiber_Name': TEXT,
    'Betreiber_Nr': FLOAT
}

#saving the DataFrame to the SQLite database table named "trainstops"
df.to_sql('trainstops', engine, if_exists='replace', index=False, dtype=dtype)

#closing the SQLAlchemy engine
engine.dispose()