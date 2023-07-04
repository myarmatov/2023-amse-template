import pandas as pd
import urllib.request
from zipfile import ZipFile
import sqlite3
from sqlalchemy import create_engine, BIGINT, TEXT, FLOAT

myFileName = "temperatures"
myUrl = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"


# Download and unzip data
urllib.request.urlretrieve(myUrl, '{}.zip'.format(myFileName))
with ZipFile('{}.zip'.format(myFileName), 'r') as f:
    f.extractall()

    
# Read and Pre-process data
df = pd.read_csv("data.csv", delimiter=";", usecols=range(11), decimal=",", header=None)
df = df.applymap(lambda x: str(x.replace(',','.')))
df.columns = list(df.iloc[0])
df = df.iloc[1:].reset_index(drop=True)


# Reshape data
myColumns = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"]
df = df[myColumns]

df[["Hersteller","Geraet aktiv","Model"]] = df[["Hersteller","Geraet aktiv","Model"]].astype(str)
df[["Geraet", "Monat"]] = df[["Geraet", "Monat"]].astype(int)
df[["Temperatur in °C (DWD)", "Batterietemperatur in °C"]] = df[["Temperatur in °C (DWD)", "Batterietemperatur in °C"]].astype(float)

oldToNewNames = {"Temperatur in °C (DWD)": "Temperatur", 
                 "Batterietemperatur in °C": "Batterietemperatur"}
df = df.rename(columns = oldToNewNames)


# Transform data
df["Temperatur"] = df["Temperatur"]*1.8 + 32
df["Batterietemperatur"] = df["Batterietemperatur"]*1.8 + 32


# Validate data

# Creating a SQLAlchemy engine to connect to the SQLite database
engine = create_engine('sqlite:///temperatures.sqlite')

# Defining the desired column types for the SQLite database
dtype = {
    'Geraet': BIGINT,
    'Hersteller': TEXT,
    'Model': TEXT,
    'Monat': BIGINT,
    'Temperatur': FLOAT,
    'Batterietemperatur': FLOAT,
    'Geraet aktiv': TEXT
}

# Saving the DataFrame to the SQLite database table named "temperatures"
df.to_sql('temperatures', engine, if_exists='replace', index=False, dtype=dtype)

# Closing the SQLAlchemy engine
engine.dispose()