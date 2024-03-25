import os
import requests
import pandas as pd
from datetime import datetime

def fetch_database(branch:str):
    url = f"https://raw.githubusercontent.com/Artsdatabanken/nin-kode-api/{branch}/NinKode.WebApi/databases/nin3kodeapi.db" # Henter nin3 database fra nin3 repo branch:develop
    response = requests.get(url)
    with open("inn_data/nin3kodeapi.db", "wb") as f:
        f.write(response.content)
        print(f"File size: {os.path.getsize('inn_data/nin3kodeapi.db')} bytes fetched")


def makeConnection():
    import pandas as pd
    import sqlite3
    return sqlite3.connect("inn_data/nin3kodeapi.db")

def tableToCsv(tablename:str, filter_kolonne:str=None):
    conn = makeConnection()
    # Skriv inn ønsket tabell
    tabell = "Enumoppslag" # 'Versjon', 'Hovedtypegruppe', 'Hovedtype', 'Grunntype', 'Kartleggingsenhet', 'Variabel', 'Variabelnavn', 
    filter_kolonne = None # Kolonnenavn i valgt tabell
    filter_verdi = None # Verdi i valgt kolonne
    df = pd.read_sql_query(f"SELECT * FROM {tablename}", conn)
    if(filter_kolonne):
        df = df[df[filter_kolonne] == filter_verdi]
    print(f"Ant. rader {df.shape[0]}")
    print("Preview: første 3 rader:")
    print(df.head(3))
    # Skrive resultat til CSV
    filename = f"rapporter/{tabell}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"\nResultat er skrevet til: {filename}")
    conn.close()

def listTablesAndViews():
    conn = makeConnection()
    df_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
    df_views = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='view'", conn)
    print("** Tabeller:")
    for table in df_tables['name']:
        count = pd.read_sql_query(f"SELECT COUNT(*) FROM {table}", conn)
        display(f"{table}: {count.iloc[0][0]} rows")
    print("\n** Views:")
    for view in df_views['name']:
        count = pd.read_sql_query(f"SELECT COUNT(*) FROM {view}", conn)
        display(f"{view}: {count.iloc[0][0]} rows")
    conn.close()

def variabelnavn_numOfTrinn():
    conn = makeConnection()
    query = """SELECT vn.Kode, vn.Navn AS Variabelnavn, COUNT(t.Id) AS NumberOfTrinn
    FROM Variabelnavn vn
    LEFT JOIN VariabelnavnMaaleskala vnm ON vn.Id = vnm.VariabelnavnId
    LEFT JOIN Trinn t ON vnm.MaaleskalaId = t.MaaleskalaId
    GROUP BY vn.Navn, vn.Kode
    ORDER BY NumberOfTrinn ASC;"""
    df_vnt = pd.read_sql_query(query, conn)
    conn.close()
    print("First 5 rows:")
    print(df_vnt.head(5))
    filename = f"rapporter/variabelnavn_numOfTrinn_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_vnt.to_csv(filename, index=False)
    print(f"\nResultat er skrevet til: {filename}")
