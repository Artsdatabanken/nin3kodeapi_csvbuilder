import pandas as pd



def find_rows_with_semicolon(df, key_column):
    df_sk = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    rows_with_semicolon = pd.DataFrame(columns=[key_column, 'columnname'])
    
    # Iterate over each column in the dataframe
    for column in df_sk.columns:
        # Check if the column contains a semicolon (;)
        if df_sk[column].astype(str).str.contains(';').any():
            # Get the rows where the semicolon (;) is present
            rows = df_sk[df_sk[column].astype(str).str.contains(';')]
            
            # Append the rows to the dataframe
            rows['columnname'] = column
            rows['value'] = rows[column]
            rows_with_semicolon = pd.concat([rows_with_semicolon, rows])
    
    # Print the [Langkode] of the rows and the name of the column
    for index, row in rows_with_semicolon.iterrows():
        print(f"[{key_column}]: {row[key_column]}, Column: {row['columnname']}")
        print(f"\t value: {row['value']}")

def sjekk_unikhet(df: pd.DataFrame, kolonne: str):
    # Controlling uniqueness of Kode column
    #--------------------------------------
    # group the dataframe by the 'Kode' column and count the number of occurrences of each Kode
    kode_counts = df.groupby(kolonne).size().reset_index(name='count')
    # filter the resulting dataframe to only include rows where the count of each Kode is greater than 1
    kode_counts_filtered = kode_counts[kode_counts['count'] > 1]
    # display the resulting dataframe
    if kode_counts_filtered.empty:
        print(f"\tIngen duplikater i kolonnen {kolonne}")
    else:
        print("\tFant følgende duplikater i kolonnen {kolonne}:\n\n")
        print(kode_counts_filtered)
    # Empty dataframe = no duplicates in Kode column

# TODO: Hvis koden ikke får tregg forsøk oppslag med å bytt "-" med 
def create_v23_variabel_url(kode, koder23):
    import urllib.parse
    kode = kode.lower()
    var_url = 'https://nin-kode-api.artsdatabanken.no/v2.3/variasjon/hentkode/'
    type_url = 'https://nin-kode-api.artsdatabanken.no/v2.3/koder/hentkode/'
    result = {}
    if koder23.get(kode):
        kodeentry = koder23.get(kode)
        if kodeentry["Klasse"] == 'Variabel':
            result["kode23"]=kodeentry['KodeName']
            result["url"]=f"{var_url}{urllib.parse.quote(kodeentry['KodeName'])}" 
            return result
        elif kodeentry["Klasse"] == 'Type':
            result["kode23"]=kodeentry['KodeName']
            result["url"]=f"{type_url}{urllib.parse.quote(kodeentry['KodeName'])}" 
            return result
        else:
            return {}
    else:
        return {}
    
def make_list_nin2kode(nin2kode):
    reslist = []
    n2list = nin2kode.split(',')
    first = n2list[0]
    for n2 in n2list:
        if n2.isdigit():
            reslist.append(first.split('-')[0]+'-'+n2)
        else:
            reslist.append(n2)
    return reslist

def variabelnavnkode_varkode2_csv(nin3_variabler):
    vnkode_varkode2 = nin3_variabler[nin3_variabler['11 Tr/Kl'] == 'W'][['Kortkode', '8 VarKode2', '10 Målesk']]
    #display(vnkode_varkode2)
    vnkode_varkode2 = vnkode_varkode2.rename(columns={'8 VarKode2': 'Varkode2_kopi'})
    #vnkode_varkode2 = vnkode_varkode2[vnkode_varkode2['10 Målesk'].isin(['SO', 'SI','SO,K'])]
    vnkode_varkode2 = vnkode_varkode2[vnkode_varkode2['10 Målesk'].str.contains('SO|SI')]
    vnkode_varkode2 = vnkode_varkode2.drop_duplicates()
    vnkode_varkode2 = vnkode_varkode2.drop(['10 Målesk'], axis=1)
    vnkode_varkode2.to_csv('inn_data/variabelnavnkode_varkode2.csv', index=False, sep=";")

def load_nin3_variabler_sheet():
    import pandas as p
    import conf
    regnearkfil = conf.regnearkfil
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    nin3_variabler = pd.read_excel(regnearkfil, sheet_name='Variabler')
    nin3_variabler = nin3_variabler.applymap(lambda x: x.strip() if isinstance(x, str) else x) # removing whitespace
    nin3_variabler = nin3_variabler.astype(str) # setting all columns to string
    return nin3_variabler

def load_nin3_typer_sheet():
    import conf
    regnearkfil = conf.regnearkfil
    # Set display options for pandas
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    
    # Disable chained assignment warning
    pd.options.mode.chained_assignment = None
    
    # Read the Excel file into a DataFrame
    nin3_typer = pd.read_excel(regnearkfil, 
                               sheet_name='Typer', 
                               #sheet_name='HT_trinntest',
                               na_filter=False)
    
    # Convert all columns to string data type
    nin3_typer = nin3_typer.astype(str)
    
    # Remove leading and trailing whitespaces from string columns
    nin3_typer = nin3_typer.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return nin3_typer
# CSV creation:
def backup_and_remove_previous_csv_files():
    import os
    import zipfile
    import datetime
    folder_path = 'ut_data'
    # Taking backup of previous csv files
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    zip_filename = f'csv_files_{timestamp}.zip'
    
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.csv'):
                file_path = os.path.join(folder_path, file_name)
                zipf.write(file_path, file_name)
    print(f"CSV files zipped successfully. Zip file name: {zip_filename}")

    # Removing previous csv files
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            os.remove(file_path)

def typer_csv(nin3_typer):
    typer = nin3_typer[['Ecosystnivå', 'Typekategori', 'Typekategori2']]#.unique()
    typer2 = typer.groupby(['Ecosystnivå','Typekategori', 'Typekategori2']).count().reset_index()
    #typer2.replace(0, np.nan, inplace=True) # BYTTER UT int 0-verdi med NaN (blank string i csv, null i json)
    typer2['Kode'] = typer2['Ecosystnivå'].map(str)+'-'+typer2['Typekategori'].map(str)+'-'+typer2['Typekategori2'].map(str)
    typer2.to_csv('ut_data/type.csv', index=False, sep=";") # NaN blir blank string i csv
    #typer2.to_json('ut_data/type.json', orient="table", index=False) # NaN blir null i json, orient: tablestruktur istedenfor seriesstuktur

def hovedtypegruppe_csv(nin3_typer):
    hovedtypegrupper = nin3_typer[['Typekategori2','Hovedtypegruppe', 'Hovedtypegruppenavn', 'Typekategori3']]
    hovedtypegrupper = hovedtypegrupper.applymap(lambda x: x.strip() if isinstance(x, str) else x) #Setter alle kolonner til string
    hovedtypegrupper['Kode'] = hovedtypegrupper['Typekategori2'].map(str)+'-'+hovedtypegrupper['Hovedtypegruppe'].map(str)
    hovedtypegrupper['Typekategori3'] = hovedtypegrupper['Typekategori3'].replace('', '0').fillna('0') # BYTTER UT tomme verdier med 0 på Typekategori3

    # Convert all columns to string type for consistency
    hovedtypegrupper = hovedtypegrupper.astype(str)

    # Replace NaN values with a consistent value
    hovedtypegrupper = hovedtypegrupper.fillna('0')
    hovedtypegrupper = hovedtypegrupper[hovedtypegrupper['Hovedtypegruppenavn'] != '0']
    hovedtypegrupper2 = hovedtypegrupper.drop_duplicates()
    hovedtypegrupper2.to_csv('ut_data/hovedtypegrupper.csv', index=False, sep=";")
    # Controlling uniqueness of Kode column
    #--------------------------------------
    sjekk_unikhet(hovedtypegrupper2, 'Kode')

def type_htg_csv(nin3_typer):
    # fetch all kodecolumns for Type and HTG
    typer_htg_0 = nin3_typer[['Ecosystnivå', 'Typekategori', 'Typekategori2', 'Hovedtypegruppe']] 
    typer_htg_0 = typer_htg_0.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    typer_htg_0["typekode"] = typer_htg_0['Ecosystnivå'].map(str)+'-'+typer_htg_0['Typekategori'].map(str)+'-'+typer_htg_0['Typekategori2'].map(str)
    typer_htg_0["thgkode"] = typer_htg_0['Typekategori2'].map(str)+'-'+typer_htg_0['Hovedtypegruppe'].map(str)
    typer_htg = typer_htg_0.iloc[:, -2:] # select last two columns (typekode, thgkode)
    typer_htg = typer_htg.groupby(['typekode', 'thgkode']).count().reset_index()
    #typer_htg
    typer_htg.to_csv('ut_data/type_htg_mapping.csv', index=False, sep=";")

def hovedtype_and_ht_htg_mapping_csv(nin3_typer):
    ht0 = nin3_typer[['Hovedtype', 'Prosedyrekategori', 'Hovedtypegruppe', 'Hovedtypenavn', 'HTGKode']]
    ht0 = ht0.applymap(lambda x: x.strip() if isinstance(x, str) else x)# Removing whitespace
    ht0 = ht0.astype(str)# setting all to type string
    ht1 = ht0
    #ht1 = ht0.dropna(subset=['Hovedtypenavn'])
    #ht1 = ht0.drop(ht1[(ht1['Hovedtypenavn'] == '')].index)
    #ht1 = ht1[['Hovedtype', 'Prosedyrekategori', 'Hovedtypegruppe', 'Hovedtypenavn']]
    #ht1['Kode'] = ht1['Hovedtypegruppe'].map(str)+'-'+ht1['Prosedyrekategori'].map(str)+'-'+ht1['Hovedtype'].map(str)+ ht1['HTGKode'].str[0]
    ht1['Kode'] = ht1['HTGKode'].str[0]+ht1['Hovedtypegruppe'].map(str)+'-'+ht1['Prosedyrekategori'].map(str)+'-'+ht1['Hovedtype'].map(str)
    ht1.drop_duplicates(subset=['Kode','Hovedtypenavn'], inplace=True)
    ht1.drop(ht1[pd.isna(ht1['Hovedtypenavn'])].index, inplace=True)#Fjerne rader uten Hovedtypenavn for str type
    ht0 = nin3_typer[['Hovedtype', 'Prosedyrekategori', 'Hovedtypegruppe', 'Hovedtypenavn', 'HTGKode']]
    ht0 = ht0.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # Removing whitespace
    ht0 = ht0.astype(str)  # setting all to type string
    ht1 = ht0
    ht1['Kode'] = ht1['HTGKode'].str[0] + ht1['Hovedtypegruppe'].map(str) + '-' + ht1['Prosedyrekategori'].map(str) + '-' + ht1['Hovedtype'].map(str)
    ht1.drop_duplicates(subset=['Kode', 'Hovedtypenavn'], inplace=True)
    ht1 = ht1[ht1['Hovedtypenavn'].str.len() > 0]  # Drop rows where string length of Hovedtypenavn-value is 0
    ht1_sorted = ht1.sort_values(by=['Kode', 'HTGKode'], ascending=True)

    ht1_sorted.to_csv('ut_data/hovedtype.csv', index=False, sep=";")
    #Getting mapping file HTG<>HT
    htg_ht = ht1_sorted[['Kode', 'HTGKode']].rename(columns={'Kode':'HTKode'})
    htg_ht.to_csv('ut_data/hovedtypegruppe_hovedtype_mapping.csv', index=False, sep=";")
    ht1_sorted = ht1_sorted.reindex(columns=['Kode', 'Hovedtypenavn', 'Hovedtypegruppe', 'Prosedyrekategori', 'Hovedtype', 'HTGKode'])
    ht1_sorted.to_csv('ut_data/hovedtype.csv', index=False, sep=";")
    sjekk_unikhet(ht1_sorted, 'Kode')

def grunntyper_csv(nin3_typer):
    grunntyper = nin3_typer[['Langkode','Hovedtypegruppe', 'Prosedyrekategori', 'Hovedtype', '11 GT', 'Grunntypenavn']]
    # TODO: Forsøk å hente fra nin3HTFIX dataframe
    #display(grunntyper.columns)
    #grunntyper2 = grunntyper[grunntyper['11 GT' != '0']] #feiler
    #grunntyper
    grunntyper.rename(columns = {'11 GT':'Grunntype'}, inplace = True)
    # Filtrer vekk 
    grunntyper_vasket2 = pd.DataFrame(grunntyper[(grunntyper['Grunntype'] != '0') 
                        & (grunntyper['Grunntype'] != ' ') 
                        & (grunntyper['Grunntype'] != '-')
                        & (grunntyper['Grunntypenavn'] != '-')
                        & (grunntyper['Grunntypenavn'] != '')])
    grunntyper_vasket2['Kode'] = grunntyper_vasket2['Hovedtypegruppe'].map(str)+'-'+grunntyper_vasket2['Prosedyrekategori'].map(str)+'-'+grunntyper_vasket2['Hovedtype'].map(str)+'-'+grunntyper_vasket2['Grunntype'].map(str)
    #display(grunntyper_vasket2)
    #display(grunntyper_vasket)
    grunntyper_vasket2 = grunntyper_vasket2.sort_values(by=['Kode'])
    grunntyper_vasket2.to_csv('ut_data/grunntyper.csv', index=False, sep=";") # NaN blir blank string i csv


    # Sjekker om det mangler grunntypenavn i csv
    grunntyper_vasket2 = pd.read_csv('ut_data/grunntyper.csv', sep=';')
    grunntypenavn_diff = grunntyper_vasket2[~grunntyper_vasket2['Grunntypenavn'].isin(nin3_typer['Grunntypenavn'])]['Grunntypenavn']
    print("*** Sjekk: GTNavn som ikke finnes i grunntype.csv ***")
    print(grunntypenavn_diff)

    """
    # Controlling uniqueness of Kode column
    #--------------------------------------"""
    sjekk_unikhet(grunntyper_vasket2,'Kode')

def hovedtype_grunntype_mapping_csv(nin3_typer):
    htg_ht_gt_mapping_tmp = nin3_typer[['Typekategori2', 'Hovedtypegruppe', 'Hovedtype', 'Prosedyrekategori', '11 GT']]
    htg_ht_gt_mapping_tmp.rename(columns = {'11 GT':'Grunntype'}, inplace = True)

    htg_ht_gt_mapping =  htg_ht_gt_mapping_tmp
    #= pd.DataFrame(htg_ht_gt_mapping_tmp[(htg_ht_gt_mapping_tmp['Grunntype'] != '0') 
    ##                     & (htg_ht_gt_mapping_tmp['Grunntype'] != ' ') 
    #                    & (htg_ht_gt_mapping_tmp['Grunntype'] != '-')])
    #display(htg_ht_gt_mapping)
    htg_mm = htg_ht_gt_mapping.groupby(['Typekategori2', 'Hovedtypegruppe', 'Hovedtype', 'Prosedyrekategori', 'Grunntype']).count().reset_index()
    htg_ht_gt_mapping['hovedtypegruppe_kode'] = htg_mm['Typekategori2'].map(str)+'-'+htg_mm['Hovedtypegruppe'].map(str)
    htg_ht_gt_mapping['hovedtype_kode'] = htg_ht_gt_mapping['hovedtypegruppe_kode'].str[0]+htg_mm['Hovedtypegruppe'].map(str)+'-'+htg_mm['Prosedyrekategori'].map(str)+'-'+htg_mm['Hovedtype'].map(str)
    htg_ht_gt_mapping['grunntype_kode'] = htg_mm['Hovedtypegruppe'].map(str)+'-'+htg_mm['Prosedyrekategori'].map(str)+'-'+htg_mm['Hovedtype'].map(str)+'-'+htg_mm['Grunntype'].map(str)
    htg_ht_gt_mapping2 = htg_ht_gt_mapping.drop(['Typekategori2', 'Hovedtypegruppe','Hovedtype', 'Prosedyrekategori', 'Grunntype'], axis=1)#.drop()
    #htg_ht_gt_mapping2.info()

    htg_ht_gt_mapping_non_null = htg_ht_gt_mapping2.drop(['hovedtypegruppe_kode'], axis=1)# dropping column 'hovedtypegruppe_kode', only used to create hovedtype_kode
    #htg_ht_gt_mapping_non_null
    htg_ht_gt_mapping_non_null.to_csv('ut_data/hovedtype_grunntype_mapping.csv', index=False, sep=";")

def m005_csv(nin3_typer, regnearkfil):
    nin3_m005 = pd.read_excel(regnearkfil, 
                           sheet_name='M005', 
                           na_filter = False, 
                           converters={'11 GT': str})#Denne kolonnen må leses inn som str for å ikke miste ledende nuller
    #display(nin3_m005)

    # Henter m005 delkode, m005 kode(lang), m005 kortkode
    #m005_delkode_kode = nin3_typer[['M005', 'M005-kode']]
    #display(nin3_m005.columns)
    m005_kode_navn = nin3_m005[['M005-langkode', 'M005-navn', 'M005-kortkode']]
    m005_kode_navn.rename(columns = {'M005-langkode':'M005-kode'}, inplace = True)
    #display(m005_kode_navn.head())
    #display(f"before unique: {m005_kode_navn.shape[0]}")

    m005_unik = m005_kode_navn.groupby(['M005-kode', 'M005-navn', 'M005-kortkode']).count().reset_index()
    m005_unik.sort_values('M005-kode')
    #display(f"after unique: {m005_unik.shape[0]}")
    #display(f"after unique-attempt(groupby): {m005_unik.shape[0]}")
    #display(f"Er m005-koder unik?: {m005_unik['M005-kode'].is_unique}")
    m005_unik.to_csv('ut_data/M005.csv', index=False, sep=";")

    #display(m005_unik)

    ###################### CHECK ##############################
    ## Get the values in column 'M005-kode' that are not unique
    sjekk_unikhet(m005_unik,'M005-kode')

def m005_grunntype_mapping_csv(nin3_typer):
    import numpy as np
    n3t = nin3_typer[['Hovedtypegruppe', 'Prosedyrekategori','Hovedtype','11 GT', 'Grunntypenavn','M005-kode', 'M005-navn']]
    n3t.rename(columns = {'11 GT':'Grunntype'}, inplace = True)
    n3t['grunntype_kode'] = n3t['Hovedtypegruppe'].map(str)+'-'+n3t['Prosedyrekategori'].map(str)+'-'+n3t['Hovedtype'].map(str)+'-'+n3t['Grunntype'].map(str)
    n3t_position  = n3t[['M005-kode', 'grunntype_kode', 'M005-navn', 'Grunntypenavn']]
    n3t_m005 = n3t_position[n3t_position['M005-kode'].str.strip().replace('', np.nan).notna()]
    n3t_m005.to_csv('ut_data/m005_grunntype_mapping.csv', index=False, sep=";")

def m005_hovedtype_mapping_csv(nin3_typer):
    # fetch 'M005-kode'
    nin3_typer = nin3_typer.astype(str) # setting all columns to string
    nin3_typer = nin3_typer.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    m005_HT = nin3_typer[['M005-kode', 'HTKode', 'Typekategori2']]
    m005_HT['HTKode'] = m005_HT['Typekategori2'].str[0]+m005_HT['HTKode'].str.replace('_', '-')
    m005_HT = m005_HT[m005_HT['M005-kode'].str.startswith('NiN-3.0')]
    m005_HT = m005_HT[['M005-kode', 'HTKode']]
    m005_HT = m005_HT.drop_duplicates(subset=['M005-kode', 'HTKode'])
    #m005_HT
    m005_HT.to_csv('ut_data/m005_hovedtype_mapping.csv', index=False, sep=";")

def m020_csv(nin3_typer, regnearkfil):
    nin3_m020 = pd.read_excel(regnearkfil, 
                           sheet_name='M020', 
                           na_filter = False, 
                           converters={'11 GT': str})#Denne kolonnen må leses inn som str for å ikke miste ledende nuller
    nin3_m020 = nin3_m020.astype(str)
    nin3_m020 = nin3_m020.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    #display(nin3_m020)
    #how to get the unique values of a column that are not unique in pandas
    #https://stackoverflow.com/questions/47136436/how-to-get-the-unique-values-of-a-column-that-are-not-unique-in-pandas
    # Henter m020 delkode og m020 kode

    #m020_delkode_kode = nin3_typer[['M020', 'M020-kode']]
    #display(nin3_m020)
    m020_kode_navn = nin3_m020[['M020_kode', 'M020-navn', 'M020_kortkode']]
    m020_kode_navn.rename(columns = {'M020_kode':'M020-kode', 'M020_kortkode':'M020-kortkode'}, inplace = True)
    #display(m020_kode_navn.head()) # sikre at listen er unik
    m020_unik = m020_kode_navn.groupby(['M020-kode', 'M020-navn', 'M020-kortkode']).count().reset_index()
    m020_unik # fjern index og skriv til csv
    #order by M020-kode
    m020_unik_sorted = m020_unik.sort_values('M020-kode') 
    #display(m020_unik_sorted.head())
    m020_unik_sorted.to_csv('ut_data/m020.csv', index=False, sep=";")

def m020_grunntype_mapping_csv(nin3_typer):
    import numpy as np
    n3t = nin3_typer[['Hovedtypegruppe', 'Prosedyrekategori','Hovedtype','11 GT', 'Grunntypenavn','M020-kode', 'M020-navn']]

    n3t.rename(columns = {'11 GT':'Grunntype'}, inplace = True)
    n3t['grunntype_kode'] = n3t['Hovedtypegruppe'].map(str)+'-'+n3t['Prosedyrekategori'].map(str)+'-'+n3t['Hovedtype'].map(str)+'-'+n3t['Grunntype'].map(str)
    n3t_position  = n3t[['M020-kode', 'grunntype_kode', 'M020-navn', 'Grunntypenavn']]
    n3t_m020 = n3t_position[n3t_position['M020-kode'].str.strip().replace('', np.nan).notna()]
    n3t_m020.drop_duplicates(subset=['M020-kode', 'grunntype_kode'])
    n3t_m020 = n3t_m020[n3t_m020['M020-kode'].str.startswith('NiN-3.0')] # Fjerner rader som ikke starter med 'NiN-3.0' i [M020-kode]
    n3t_m020.to_csv('ut_data/m020_grunntype_mapping.csv', index=False, sep=";") 

def m020_hovedtype_mapping_csv(nin3_typer):
    m020_HT = nin3_typer[['M020-kode', 'HTKode', 'HTGKode']]
    m020_HT['HTKode'] = m020_HT['HTGKode'].str[0]+m020_HT['HTKode'].str.replace('_', '-')
    m020_HT = m020_HT[m020_HT['M020-kode'].str.startswith('NiN-3.0')]#remove rows with empty or incorrect m020 values
    m020_HT = m020_HT[['M020-kode', 'HTKode']]#remove HTGKode
    m020_HT = m020_HT.drop_duplicates(subset=['M020-kode', 'HTKode'])
    m020_HT.to_csv('ut_data/m020_hovedtype_mapping.csv', index=False, sep=";")
    
def m050_csv(nin3_typer, regnearkfil):
    nin3_m050 = pd.read_excel(regnearkfil, 
                           sheet_name='M050', 
                           na_filter = False, 
                           converters={'11 GT': str})#Denne kolonnen må leses inn som str for å ikke miste ledende nuller
    #m050_delkode_kode = nin3_typer[['M050', 'M050-kode']]
    m050_kode_navn = nin3_m050[['M050_kode', 'M050-navn', 'M050_kortkode']]
    #display(m050_kode_navn.head())
    m050_kode_navn.rename(columns = {'M050_kode':'M050-kode', 'M050_kortkode':'M050-kortkode'}, inplace = True)
    #display(m050_kode_navn.head()) # sikre at listen er unik
    m050_unik = m050_kode_navn.groupby(['M050-kode', 'M050-navn', 'M050-kortkode']).count().reset_index()
    m050_unik # fjern index og skriv til csv
    #order by M020-kode
    m050_unik_sorted = m050_unik.sort_values('M050-kode') 
    m050_unik_sorted.to_csv('ut_data/m050.csv', index=False, sep=";")

def m050_grunntype_mapping_csv(nin3_typer):
    import numpy as np
    n3t = nin3_typer[['Hovedtypegruppe', 'Prosedyrekategori','Hovedtype','11 GT', 'Grunntypenavn','M050-kode', 'M050-navn']]

    n3t.rename(columns = {'11 GT':'Grunntype'}, inplace = True)
    n3t['grunntype_kode'] = n3t['Hovedtypegruppe'].map(str)+'-'+n3t['Prosedyrekategori'].map(str)+'-'+n3t['Hovedtype'].map(str)+'-'+n3t['Grunntype'].map(str)
    n3t_position  = n3t[['M050-kode', 'grunntype_kode', 'M050-navn', 'Grunntypenavn']]
    n3t_m050 = n3t_position[n3t_position['M050-kode'].str.strip().replace('', np.nan).notna()]
    n3t_m050 = n3t_m050[n3t_m050['M050-kode'].str.startswith('NiN-3.0')] # Fjerner rader som ikke starter med 'NiN-3.0' i [M020-kode]
    #n3t_m005
    n3t_m050.to_csv('ut_data/m050_grunntype_mapping.csv', index=False, sep=";") 

def m050_hovedtype_mapping_csv(nin3_typer):
    nin3_typer = nin3_typer.astype(str) # setting all columns to string
    nin3_typer = nin3_typer.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    m050_HT = nin3_typer[['M050-kode', 'HTKode','HTGKode']]
    m050_HT['HTKode'] = m050_HT['HTGKode'].str[0]+m050_HT['HTKode'].str.replace('_', '-') #Ny HTkode (Kat2 in front)
    m050_HT = m050_HT[m050_HT['M050-kode'].str.startswith('NiN-3.0')]
    m050_HT = m050_HT.drop_duplicates(subset=['M050-kode', 'HTKode'])
    m050_HT.to_csv('ut_data/m050_hovedtype_mapping.csv', index=False, sep=";")

# depends on typer_csv() being run first
def typeklasser_langkode_mapping_csv(nin3_typer):
    typer = nin3_typer[['Ecosystnivå', 'Typekategori', 'Typekategori2']]#.unique()
    typer2 = typer.groupby(['Ecosystnivå','Typekategori', 'Typekategori2']).count().reset_index()
    #typer2.replace(0, np.nan, inplace=True) # BYTTER UT int 0-verdi med NaN (blank string i csv, null i json)
    typer2['Kode'] = typer2['Ecosystnivå'].map(str)+'-'+typer2['Typekategori'].map(str)+'-'+typer2['Typekategori2'].map(str)
    typer_lk = nin3_typer[['Ecosystnivå', 'Typekategori', 'Typekategori2', 'Hovedtypegruppe', 'Hovedtypegruppenavn','Hovedtype', 'Prosedyrekategori','Langkode']]#.unique()
    #typer2.replace(0, np.nan, inplace=True) # BYTTER UT int 0-verdi med NaN (blank string i csv, null i json)
    typer_lk['Type_kode'] = typer2['Ecosystnivå'].map(str)+'-'+typer2['Typekategori'].map(str)+'-'+typer2['Typekategori2'].map(str)
    typer_lk['Hovedtypegruppe_kode'] = typer_lk['Typekategori2'].map(str)+'-'+typer_lk['Hovedtypegruppe'].map(str)
    typer_lk['Hovedtype_kode'] = typer_lk['Hovedtypegruppe'].map(str)+'-'+typer_lk['Prosedyrekategori'].map(str)+'-'+typer_lk['Hovedtype'].map(str)
    typeklasser_langkode = typer_lk[['Type_kode', 'Hovedtypegruppe_kode', 'Hovedtype_kode', 'Langkode']]
    typeklasser_langkode.to_csv('ut_data/typeklasser_langkode_mapping.csv', index=False, sep=";") # NaN blir blank string i csv

def grunntype_variabeltrinn_mapping_csv(nin3_typer, nin3_typer_orig):
    import numpy as np
    gt_vt = nin3_typer[nin3_typer['10 GT/kE'] == 'G'][['Hovedtypegruppe', 'Prosedyrekategori', 'Hovedtype', '11 GT', 'Definisjonsgrunnlag', 'oLkM', '11 GT', 'Grunntypenavn']]
    gt_vt.rename(columns={'11 GT': 'Grunntype'}, inplace=True)

    gt_vt_1 = gt_vt[(gt_vt['Grunntypenavn'] != '-')
                        & (gt_vt['Grunntypenavn'] != '')]

    gt_vt_1.to_csv('ut_data/sample_gt_vt_1.csv', index=False, sep=";")

    gt_vt_1['Definisjonsgrunnlag'] = gt_vt_1['Definisjonsgrunnlag'].str.replace('[\[\]]', '')

    #gt_vt_1['GTKode'] = gt_vt_1['Hovedtypegruppe'].apply(str) + '-' + gt_vt_1['Prosedyrekategori'].apply(str) + '-' + gt_vt_1['Hovedtype'].apply(str) + '-' + gt_vt_1['Grunntype'].apply(str)
    gt_vt_1['GTKode'] = gt_vt_1[['Hovedtypegruppe', 'Prosedyrekategori', 'Hovedtype', 'Grunntype']].apply(lambda x: '-'.join(x.astype(str)), axis=1)
    gt_vt_1 = gt_vt_1.dropna(subset=['Definisjonsgrunnlag'])

    new_gt_vt_rows = []
    gt_vt_1[['Varkode2', 'Trinn']] = None
    gt_vt_1 = gt_vt_1.loc[:, ['GTKode', 'Varkode2', 'Trinn', 'Definisjonsgrunnlag']].drop_duplicates()

    for index, row in gt_vt_1.iterrows():
        if ',' in row['Definisjonsgrunnlag']:
            values = row['Definisjonsgrunnlag'].split(',')
            for value in values:
                new_row = row.copy()
                value = value.replace('[', '').replace(']', '').strip()
                vk2andTrinns = value.split('_')
                if len(vk2andTrinns) > 1:
                    vk2 = vk2andTrinns[0].upper()
                    trinns = vk2andTrinns[1]
                    for char in trinns:
                        char_row = new_row.copy()
                        char_row['Varkode2'] = vk2
                        char_row['Trinn'] = vk2 + "-" + char.strip()
                        new_gt_vt_rows.append(char_row)
                else:
                    char_row = new_row.copy()
                    char_row['Varkode2'] = vk2
                    char_row['Trinn'] = ''
                    new_gt_vt_rows.append(char_row)
        else:
            new_row = row.copy()
            row['Definisjonsgrunnlag'] = row['Definisjonsgrunnlag'].replace('[', '').replace(']', '').strip()
            vk2andTrinns = row['Definisjonsgrunnlag'].split('_')
            vk2 = vk2andTrinns[0].upper()
            if len(vk2andTrinns) > 1:
                trinns = vk2andTrinns[1]
                for char in trinns:
                    char_row = new_row.copy()
                    char_row['Varkode2'] = vk2
                    char_row['Trinn'] = vk2 + "-" + char.strip()
                    new_gt_vt_rows.append(char_row)
            else:
                char_row = new_row.copy()
                char_row['Varkode2'] = vk2
                char_row['Trinn'] = ''
                new_gt_vt_rows.append(char_row)

    new_gt_vt = pd.DataFrame(new_gt_vt_rows)
    new_gt_vt['Trinn'] = new_gt_vt['Trinn'].str.replace('-', '_')

    new_gt_vt = new_gt_vt.loc[:, ['GTKode', 'Varkode2', 'Trinn', 'Definisjonsgrunnlag']].drop_duplicates()

    variabelnavnkode_varkode2 = pd.read_csv('inn_data/variabelnavnkode_varkode2.csv', sep=";")
    new_gt_vt_done = pd.merge(new_gt_vt, variabelnavnkode_varkode2, left_on='Varkode2', right_on='Varkode2_kopi', how='left')
    new_gt_vt_done.drop(['Varkode2_kopi'], axis=1, inplace=True)
    new_gt_vt_done.rename(columns={'Kortkode': 'Variabelnavn_kortkode'}, inplace=True)

    gt_vt = nin3_typer[nin3_typer['10 GT/kE'] == 'G'][['Hovedtypegruppe', 'Prosedyrekategori', 'Hovedtype', '11 GT', 'Definisjonsgrunnlag', 'oLkM', '11 GT', 'Grunntypenavn']]
    gt_vt.rename(columns={'11 GT': 'Grunntype'}, inplace=True)

    gt_vt_1 = gt_vt[(gt_vt['Grunntypenavn'] != '-')
                        & (gt_vt['Grunntypenavn'] != '')]

    #gt_vt_1.to_csv('ut_data/sample_gt_vt_1.csv', index=False, sep=";")

    gt_vt_1['Definisjonsgrunnlag'] = gt_vt_1['Definisjonsgrunnlag'].str.replace('[\[\]]', '')

    gt_vt_1['GTKode'] = gt_vt_1[['Hovedtypegruppe', 'Prosedyrekategori', 'Hovedtype', 'Grunntype']].apply(lambda x: '-'.join(x.astype(str)), axis=1)
    gt_vt_1 = gt_vt_1.dropna(subset=['Definisjonsgrunnlag'])

    new_gt_vt_rows = []
    gt_vt_1[['Varkode2', 'Trinn']] = None
    gt_vt_1 = gt_vt_1.loc[:, ['GTKode', 'Varkode2', 'Trinn', 'Definisjonsgrunnlag']].drop_duplicates()

    for index, row in gt_vt_1.iterrows():
        if ',' in row['Definisjonsgrunnlag']:
            values = row['Definisjonsgrunnlag'].split(',')
            for value in values:
                new_row = row.copy()
                value = value.replace('[', '').replace(']', '').strip()
                vk2andTrinns = value.split('_')
                if len(vk2andTrinns) > 1:
                    vk2 = vk2andTrinns[0].upper()
                    trinns = vk2andTrinns[1]
                    for char in trinns:
                        char_row = new_row.copy()
                        char_row['Varkode2'] = vk2
                        char_row['Trinn'] = vk2 + "-" + char.strip()
                        new_gt_vt_rows.append(char_row)
                else:
                    char_row = new_row.copy()
                    char_row['Varkode2'] = vk2
                    char_row['Trinn'] = ''
                    new_gt_vt_rows.append(char_row)
        else:
            new_row = row.copy()
            row['Definisjonsgrunnlag'] = row['Definisjonsgrunnlag'].replace('[', '').replace(']', '').strip()
            vk2andTrinns = row['Definisjonsgrunnlag'].split('_')
            vk2 = vk2andTrinns[0].upper()
            if len(vk2andTrinns) > 1:
                trinns = vk2andTrinns[1]
                for char in trinns:
                    char_row = new_row.copy()
                    char_row['Varkode2'] = vk2
                    char_row['Trinn'] = vk2 + "-" + char.strip()
                    new_gt_vt_rows.append(char_row)
            else:
                char_row = new_row.copy()
                char_row['Varkode2'] = vk2
                char_row['Trinn'] = ''
                new_gt_vt_rows.append(char_row)

    new_gt_vt = pd.DataFrame(new_gt_vt_rows)
    new_gt_vt['Trinn'] = new_gt_vt['Trinn'].str.replace('-', '_')

    new_gt_vt = new_gt_vt.loc[:, ['GTKode', 'Varkode2', 'Trinn', 'Definisjonsgrunnlag']].drop_duplicates()

    variabelnavnkode_varkode2 = pd.read_csv('inn_data/variabelnavnkode_varkode2.csv', sep=";")
    new_gt_vt_done = pd.merge(new_gt_vt, variabelnavnkode_varkode2, left_on='Varkode2', right_on='Varkode2_kopi', how='left')
    new_gt_vt_done.drop(['Varkode2_kopi'], axis=1, inplace=True)
    new_gt_vt_done.rename(columns={'Kortkode': 'Variabelnavn_kortkode'}, inplace=True)
    # attempting to replace [] in definisjonsgrunnlag : 
    import re

    # Define the regex pattern to extract values within square brackets
    pattern = r'\[([^\]]+)\]'

    # Apply the regex operation to the 'Definisjonsgrunnlag' column
    new_gt_vt_done['Definisjonsgrunnlag'] = new_gt_vt_done['Definisjonsgrunnlag'].apply(lambda x: re.sub(pattern, r'\1', x))
    #new_gt_vt_done.dropna(subset=['Varkode2', 'Trinn', 'Definisjonsgrunnlag', 'Variabelnavn_kortkode'], how='all', inplace=True)#removing rows where Varkode2, Trinn, Definisjonsgrunnlag or Variabelnavn_kortkode is null
    #new_gt_vt_done.dropna(subset=['Varkode2', 'Trinn', 'Definisjonsgrunnlag', 'Variabelnavn_kortkode'], how='all', inplace=True, na_values=['', ' '])
    new_gt_vt_done.replace(' ', '', inplace=True)
    new_gt_vt_done.replace('-', '', inplace=True)
    new_gt_vt_done.replace('', np.nan, inplace=True)
    new_gt_vt_done.dropna(subset=['Varkode2', 'Trinn', 'Definisjonsgrunnlag', 'Variabelnavn_kortkode'], how='all', inplace=True)
    new_gt_vt_done.to_csv('ut_data/grunntype_variabeltrinn_mapping.csv', index=False, sep=";")

def hovedtype_variabeltrinn_mapping_csv(nin3_typer, nin3_typer_orig):
    class varkode2_Trinn:
        Variabelnavn_kortkode:str = ''
        Trinn:str = ''
        Varkode2:str = ''

    variabelnavnkode_varkode2 = pd.read_csv('inn_data/variabelnavnkode_varkode2.csv', sep=";")
    def handle_definisjonsgrunnlag(dg)->list:
        trinnlist = []
        dg = dg.replace('[', '').replace(']', '').replace('(‒)','').strip()
        dg = dg.replace('-', '').strip()
        v2_ts = dg.split(",")
        for v2t in v2_ts:
            v2t_sp = v2t.split("_")
            varkode2 = v2t_sp[0].upper()
            variabelnavnkode_varkode2_row = variabelnavnkode_varkode2[variabelnavnkode_varkode2['Varkode2_kopi'] == varkode2]
            Variabelnavn_kortkode = ''
            if not variabelnavnkode_varkode2_row.empty:
                Variabelnavn_kortkode = variabelnavnkode_varkode2_row['Kortkode'].values[0]
            if len(v2t_sp) == 2:
                for t in v2t_sp[1]:
                        vt = varkode2_Trinn()
                        vt.Variabelnavn_kortkode = Variabelnavn_kortkode
                        vt.Trinn = varkode2.strip()+"_"+t.strip()
                        vt.Varkode2 = varkode2.strip()
                        trinnlist.append(vt)
            else:
                vt = varkode2_Trinn()
                vt.Variabelnavn_kortkode = Variabelnavn_kortkode,
                vt.Varkode2 = varkode2
                trinnlist.append(vt)
        return trinnlist

    #def find_
    #def 
    variabeltrinnList = []
    # prepare a class with all output fields

    # fetch dataframe with varkode2<>variabelnavn_kortkode mapping
    variabelnavnkode_varkode2 = pd.read_csv('inn_data/variabelnavnkode_varkode2.csv', sep=";")

    # Fetch the columns + definfisjonsgrunnlag
    ht_variabeltrinn = nin3_typer_orig[(nin3_typer_orig['9 HT'] != '0') & (nin3_typer_orig['11 GT']=='0')][['7 HTG', '8 Pk','9 HT', 'Definisjonsgrunnlag', '5 kat2']]
    ## Create a valid Hovedtypekode from the row values
    ht_variabeltrinn['HTKode'] = ht_variabeltrinn['5 kat2'].str[0]+ht_variabeltrinn['7 HTG'].map(str)+'-'+ht_variabeltrinn['8 Pk'].map(str)+'-'+ht_variabeltrinn['9 HT'].map(str)
    ht_variabeltrinn_filtered = ht_variabeltrinn[(ht_variabeltrinn['Definisjonsgrunnlag'] != '') & (ht_variabeltrinn['Definisjonsgrunnlag'] != '-')]
    # dataframe for storing trinn-results
    ht_trinn_df = pd.DataFrame({
        'HTkode': [],
        'Varkode2': [],
        'Trinn': [],
        'Variabelnavn_kortkode': [],
        'Definisjonsgrunnlag': []
    })
    for index, row in ht_variabeltrinn_filtered.iterrows():
        # Create a dataframe series for each trinn given in Definisjonsgrunnlag
        varkode_trinnList = handle_definisjonsgrunnlag(row['Definisjonsgrunnlag'])

        for vt in varkode_trinnList:
            # add the new row to the final dataframe
            new_row = {
                'HTkode': row['HTKode'], 
                'Varkode2': vt.Varkode2, 
                'Trinn': vt.Trinn,
                'Variabelnavn_kortkode': vt.Variabelnavn_kortkode if str(vt.Variabelnavn_kortkode) != "('',)" else '',
                'Definisjonsgrunnlag': row['Definisjonsgrunnlag']
            }
            # add the new row to the final dataframe
            #ht_trinn_df.append(new_row, ignore_index=True)
            ht_trinn_df.loc[len(ht_trinn_df)] = new_row
            #ht_trinn_df = pd.concat([ht_trinn_df, new_series], ignore_index=True)

    # save dataframe to CSV file with ; as separator
    ht_trinn_df.to_csv('ut_data/hovedtype_variabeltrinn_mapping.csv', sep=';', index=False)


def prepare_konvertering_3_to_23():
    import sqlite3
    import csv
    import os
    import urllib.parse

    # Read sqlite query results into a pandas DataFrame
    con = sqlite3.connect("nin2prod.db")
    query = """
        Select distinct KodeName, 'Type' as Klasse from Kode where VersionId in (select id from NiNVersion where Navn ='2.3')
        UNION
        Select distinct KodeName, 'Variabel' as Klasse from VariasjonKode where VersionId in (select id from NiNVersion where Navn ='2.3')
        Order by Klasse"""
    df = pd.read_sql_query(query, con)
    con.close()
    df.to_csv('inn_data/2_3_koder_fra_sqlite_prod.csv', sep=';', index=False)

    # Load v2.3 koder to dict
    koder23 = {}
    with open('inn_data/2_3_koder_fra_sqlite_prod.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';', quotechar='"')
        koder = []
        for row in reader:
            if ',' in row['KodeName']:
                # do something if the string contains a comma
                kode_list = row['KodeName'].split(',')
                for kode in kode_list:
                    lookupkode = kode.lower().strip() #lager lookupkolonne som lowercase siden kode fra excel er ..
                    # ..ukorrekt i bruk av  i bruk små og store bokstaver
                    if lookupkode.startswith("na "):
                        lookupkode = lookupkode.replace("na ", "")
                    koder23[lookupkode.strip()] = {"KondeName":row['KodeName'].strip(), "Klasse":row['Klasse']}
                    #koder23[kode.strip()] = row['Klasse']
            else:
                # do something else if the string does not contain a comma
                lookupkode = row['KodeName'].lower().strip() #lager lookupkolonne som lowercase siden kode fra excel er ..
                    # ..ukorrekt i bruk av  i bruk små og store bokstaver
                if lookupkode.startswith("na "):
                    lookupkode = lookupkode.replace("na ", "")
                koder23[lookupkode.strip()] = {"KodeName":row['KodeName'].strip(), "Klasse":row['Klasse']}

    # Write koder23 to a text file in the tmp directory
    with open(os.path.join('tmp', 'koder23.txt'), 'w') as f:
        output = []
        for key, value in koder23.items():
            output.append(f"{key}: {value}\n")
        f.write("".join(output))
    return koder23
        

    
def htg_conv_csv(nin3_typer,nin3_typer_orig, koder23):
    import urllib.parse
    import csv
    htg0 = nin3_typer_orig[['3 AbC', '4 kat1', '5 kat2', '6 kat3','7 HTG', '8 Pk','9 HT', '11 GT', 'NiN 2 kode', 'FP', 'SP']]
    #display(htg0)
    # remove heading and tailing spaces from all columns
    htg0 = htg0.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    htg0_1 = htg0[
        (htg0['5 kat2'] != 'LA') & # holdes midlertidig utenfor
        (htg0['4 kat1'] != 'LI') & # holdes midlertidig utenfor
        (htg0['9 HT'] == '0') & 
        (htg0['11 GT']=='0')][['3 AbC', '4 kat1', '5 kat2', '6 kat3','7 HTG', '8 Pk','9 HT', '11 GT', 'NiN 2 kode', 'FP', 'SP']]
    #htg1 = htg0[(htg0['NiN 2 kode'] != '-') & (htg0['NiN 2 kode']!='')]
    htg1 = htg0_1[htg0_1['NiN 2 kode'] != '-']
    htg1 = htg1[htg1['NiN 2 kode'] != '']
    htg1['HTGkode'] = htg1['5 kat2'].map(str)+'-'+htg1['7 HTG'].map(str)
    htg2 = htg1[['HTGkode', 'NiN 2 kode', 'FP', 'SP']]
    htg2 = htg2.rename(columns={'NiN 2 kode': 'forrigekode'})
    htg2['Klasse'] = 'HTG'
    htg2.to_csv('tmp/htg_konv.csv', sep=';', index=False)


    # Open the file as dictreader
    htg_rows = []

    with open('tmp/htg_konv.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            new = {} # new dict to store the new row
            new['HTGkode']=row['HTGkode']
            new['FP']=row['FP']
            new['SP']=row['SP']
            new['Klasse']=row['Klasse']
            result = create_v23_variabel_url(row['forrigekode'], koder23)
            new['forrigekode']=result['kode23']
            new['url']=result['url']
            if new['url'] =='':# bytter siste "-" med "_" og prøver igjen
                kode = row['forrigekode'].rsplit('-', 1)
                kode = '_'.join(kode)
                result = create_v23_variabel_url(kode)
                new['forrigekode']=result['kode23']
                new['url']=result['url']
            if new['url'] =='':
                print(f"\t\tFant ikke url for {kode}")
            htg_rows.append(new)

    # Creating konvertering csv for HTG
    with open('ut_data/konvertering_htg_v30.csv', 'w', newline='') as csvfile:
        fieldnames = ['HTGkode','forrigekode','FP','SP','Klasse', 'url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(htg_rows)
        print(f"File written to {csvfile.name}")

def ht_conv_csv(nin3_typer,nin3_typer_orig, koder23):
    import urllib.parse
    import csv
    ht0 = nin3_typer_orig.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    ht0_0 = ht0[
        (ht0['5 kat2'] != 'LA') & # holdes midlertidig utenfor
        (ht0['4 kat1'] != 'LI') & # holdes midlertidig utenfor
        (ht0['NiN 2 kode'] != 'ny') &
        (ht0['9 HT'] != '0') & 
        (ht0['11 GT']=='0')][['3 AbC', '4 kat1', '5 kat2', '6 kat3','7 HTG', '8 Pk','9 HT', '11 GT', 'NiN 2 kode', 'FP', 'SP']]
    # remove heading and tailing spaces from all columns

    ht0_0 = ht0_0[ht0_0['NiN 2 kode'] != '-']
    ht0_0 = ht0_0[ht0_0['NiN 2 kode'] != '']
    ht1 = ht0_0
    ht1['HTkode'] = ht1['5 kat2'].str[0]+ht1['7 HTG'].map(str)+'-'+ht1['8 Pk'].map(str)+'-'+ht1['9 HT'].map(str)

    ht2 = ht1[['HTkode', 'NiN 2 kode', 'FP', 'SP']]
    ht2 = ht2.rename(columns={'NiN 2 kode': 'forrigekode'})
    ht2['Klasse'] = 'HT'
    ht2.to_csv('tmp/ht_konv.csv', sep=';', index=False)

    ht_rows = []

    #reading the csv file with DictReader
    with open('tmp/ht_konv.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        all_rows = []
        #check rows for multiple nin2koder
        for r in reader:
            if "," in r.get("forrigekode"):
                n2list = make_list_nin2kode(r.get("forrigekode"))
                for n2 in n2list:
                    new_row = r.copy()
                    new_row["forrigekode"] = n2.strip()
                    all_rows.append(new_row)
        
        #loop over rows
        for row in all_rows:
            new = {} # new dict to store the new row
            new['HTkode']=row['HTkode']
            new['FP']=row['FP']
            new['SP']=row['SP']
            new['Klasse']=row['Klasse']
            result = create_v23_variabel_url(row['forrigekode'], koder23)
            new['forrigekode']=result.get('kode23')
            new['url']=result.get('url')
            if not new.get('url'):# bytter siste "-" med "_" og prøver igjen
                kode = row['forrigekode'].rsplit('-', 1)
                kode = '_'.join(kode)
                result = create_v23_variabel_url(kode, koder23)
                new['forrigekode']=result.get('kode23')
                new['url']=result.get('url')
            # Sjekker om excel-kolonnen forrigekode (nin 2 koden) hadde match i koder23-dictonary 
            if not new.get('url'):
                new['forrigekode']=row['forrigekode'] 
                print(f"\t\tFant ikke url for NiN2kode:{row['forrigekode']}")
            ht_rows.append(new)

    # Creating konvertering csv for HT
    with open('ut_data/konvertering_ht_v30.csv', 'w', newline='') as csvfile:
        fieldnames = ['HTkode','forrigekode','FP','SP','Klasse', 'url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(ht_rows)
        print(f"File written to {csvfile.name}") 

def gt_conv_csv(nin3_typer,nin3_typer_orig, koder23):
    import urllib.parse
    import csv
    gt0 = nin3_typer_orig.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    gt0_0 = gt0[
        (gt0['5 kat2'] != 'LA') & # holdes midlertidig utenfor
        (gt0['4 kat1'] != 'LI') & # holdes midlertidig utenfor
        (gt0['NiN 2 kode'] != 'ny') &
        (gt0['NiN 2 kode'] != '-') &
        (gt0['NiN 2 kode'] != '') &
        (gt0['11 GT']!='0')][['3 AbC', '4 kat1', '5 kat2', '6 kat3','7 HTG', '8 Pk','9 HT', '11 GT', 'NiN 2 kode', 'FP', 'SP']]

    gt0_0['GTKode'] = gt0_0['7 HTG'].map(str)+'-'+gt0_0['8 Pk'].map(str)+'-'+gt0_0['9 HT'].map(str)+'-'+gt0_0['11 GT'].map(str)
    gt1 = gt0_0[['GTKode', 'NiN 2 kode', 'FP', 'SP']]
    gt1 = gt1.rename(columns={'NiN 2 kode': 'forrigekode'})
    gt1["FP"]=gt1["FP"].replace('?', '')
    gt1["SP"]=gt1["SP"].replace('?', '')
    gt1['Klasse'] = 'GT'
    gt1.to_csv('tmp/gt_konv.csv', sep=';', index=False)


    gt_rows = []
    with open('tmp/gt_konv.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        all_rows = []
        #check rows for multiple nin2koder
        for r in reader:
            if "," in r.get("forrigekode"):
                n2list = make_list_nin2kode(r.get("forrigekode"))
                for n2 in n2list:
                    new_row = r.copy()
                    new_row["forrigekode"] = n2.strip()
                    all_rows.append(new_row)
        
        #loop over rows
        for row in all_rows:
            new = {} # new dict to store the new row
            new['GTKode']=row['GTKode']
            new['FP']=row['FP']
            new['SP']=row['SP']
            new['Klasse']=row['Klasse']
            result = create_v23_variabel_url(row['forrigekode'], koder23)
            new['forrigekode']=result.get('kode23')
            new['url']=result.get('url')
            if not new.get('url'):# bytter siste "-" med "_" og prøver igjen
                kode = row['forrigekode'].rsplit('-', 1)
                kode = '_'.join(kode)
                result = create_v23_variabel_url(kode, koder23)
                new['forrigekode']=result.get('kode23')
                new['url']=result.get('url')
            # Sjekker om excel-kolonnen forrigekode (nin 2 koden) hadde match i koder23-dictonary 
            if not new.get('url'):
                new['forrigekode']=row['forrigekode'] 
                print(f"\t\tFant ikke url for NiN2kode:{row['forrigekode']}")
            gt_rows.append(new)

    # Creating konvertering csv for GT
    with open('ut_data/konvertering_gt_v30.csv', 'w', newline='') as csvfile:
        fieldnames = ['GTKode','forrigekode','FP','SP','Klasse', 'url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(gt_rows)
        print(f"File written to {csvfile.name}")  

# Variabel csvs
def variabel_csv(nin3_variabler):
    # hent unike kombinasjoner av kolonnene '3 ABC' og '4 NM'
    #variabler = nin3_variabler[['3 ABC', '4 NM']].drop_duplicates().sort_values(by=['3 ABC', '4 NM'])
    # above line but also drop nan values
    variabler = nin3_variabler[['3 ABC', '4 NM']].drop_duplicates().dropna().sort_values(by=['3 ABC', '4 NM'])
    variabler['Variabel_kode'] = variabler[['3 ABC', '4 NM']].apply(lambda x: '-'.join(x), axis=1)

    variabler.to_csv('ut_data/variabel.csv', index=False, sep=";")

def variabelnavn_variabel_mapping_csv(nin3_variabler):
    vvm = nin3_variabler[nin3_variabler['11 Tr/Kl'] == 'W'][['Langkode', 'Kortkode','3 ABC', '4 NM', '5 kat', '6 FG/EK', '7 Varkode1', '8 VarKode2', 'VarNavn']].drop_duplicates()
    vvm = vvm.rename(columns={'5 kat': 'Variabelkategori2', 
                            '6 FG/EK': 'Variabeltype', '7 Varkode1': 'Variabelgruppe', '8 VarKode2': 'Variabelnavn_kode', 'VarNavn': 'Variabelnavn_navn'})
    vvm['Variabel_kode'] = vvm[['3 ABC', '4 NM']].apply(lambda x: '-'.join(x), axis=1)
    #dupl = vvm['Variabelnavn_kode'].duplicated().any()
    vvm.to_csv('ut_data/variabelnavn_variabel_mapping.csv', index=False, sep=";")

def maaleskala_enhet_csv(nin3_variabler):
    from tabulate import tabulate
    from io import StringIO
    # part 1/2
    nin3_variabler_so_si = nin3_variabler[nin3_variabler['11 Tr/Kl'] != 'W']
    me_1 = nin3_variabler_so_si[['8 VarKode2', '10 Målesk']].copy()
    me_1 = me_1.rename(columns={'10 Målesk': 'Måleskala'})
    me_1 = me_1.dropna(subset=['Måleskala'])
    me_1['Enhet'] = None
    me_1.loc[me_1['Måleskala'] == 'SO', 'Enhet'] = 'VSO'
    me_1.loc[me_1['Måleskala'] == 'SI', 'Enhet'] = 'VSI'
    me_1['MåleskalaNavn'] = me_1['8 VarKode2'] + '-' + me_1['Måleskala']
    me_1 = me_1.drop('8 VarKode2', axis=1)
    me_1 = me_1.drop_duplicates()
    #me_1.head(5) 
    # part 2/2
    with open('inn_data/maaleskala_trinn_enhet.md', 'r') as file:
        file_string = file.read()
    # Remove leading/trailing white spaces and split the string into lines
    lines = [line.strip() for line in file_string.strip().split('\n')]
    # Join the lines back together, this time separating them with '\n', and read the result into a DataFrame
    me_2 = pd.read_csv(StringIO('\n'.join(lines)), sep='|',engine='python')
    # Remove any leading/trailing white spaces from the column names and drop any empty rows
    me_2.columns = me_2.columns.str.strip()
    me_2 = me_2.dropna(how='all')
    me_2 = me_2.drop(['Trinn', 'Trinnverdi'], axis=1)
    me_2 = me_2[me_2['Måleskala'].str.strip() != '--'] #removing rows where Måleskala = "--"
    me_2 = me_2.rename(columns={'Enhet': 'Everdi'})
    me_2['Enhet'] = None
    me_2.loc[me_2['Everdi'].str.strip() == 'Prosent', 'Enhet'] = 'P'
    me_2.loc[me_2['Everdi'].str.strip() == 'Observert antall', 'Enhet'] = 'OA'
    me_2.loc[me_2['Everdi'].str.strip() == 'Tetthet', 'Enhet'] = 'T'
    me_2.loc[me_2['Everdi'].str.strip() == 'Binær', 'Enhet'] = 'B'
    me_2 = me_2.drop(['Everdi'], axis=1)
    # make all rows unique
    me_2 = me_2.drop_duplicates()
    me_2['Måleskala'] = me_2['Måleskala'].str.strip()
    me_2['MåleskalaNavn'] = me_2['Måleskala']
    me = pd.concat([me_1, me_2], ignore_index=True)
    me.to_csv('ut_data/maaleskala_enhet.csv', index=False, sep=";")

def maaleskala_trinn_csv(nin3_variabler):
    from tabulate import tabulate
    from io import StringIO
    # part 1/2
    mt1 = nin3_variabler[['Kortkode', '8 VarKode2', '10 Målesk', '11 Tr/Kl', 'Trinn/klassebetegnelse']].copy()
    mt1 = mt1[mt1['11 Tr/Kl'] != 'W']
    mt1 = mt1.dropna(subset=['10 Målesk'])
    mt1['MåleskalaNavn'] = mt1['8 VarKode2'].astype(str) + '-' + mt1['10 Målesk'].astype(str).str.strip()
    mt1 = mt1.rename(columns={'11 Tr/Kl': 'Trinn', 'Trinn/klassebetegnelse': 'Trinnverdi'})
    mt1['Trinn'] = mt1['8 VarKode2'].astype(str) + '_' + mt1['Trinn'].astype(str)
    mt1 = mt1.drop(['Kortkode', '8 VarKode2', '10 Målesk'], axis=1)
    #mt1.head(3)

    # part 2/2
    # TODO-Fetch variabelnavnkode and måleskalakode
    # hent trinn fra md-fil
    # tilpass og concat med del1 dataframe
    with open('inn_data/maaleskala_trinn_enhet.md', 'r') as file:
        file_string = file.read()
    # Remove leading/trailing white spaces and split the string into lines
    lines = [line.strip() for line in file_string.strip().split('\n')]
    # Join the lines back together, this time separating them with '\n', and read the result into a DataFrame
    mt2 = pd.read_csv(StringIO('\n'.join(lines)), sep='|',engine='python')
    # Remove any leading/trailing white spaces from the column names and drop any empty rows
    mt2.columns = mt2.columns.str.strip()
    mt2 = mt2.dropna(how='all')

    mt2['Trinnverdi'] = mt2['Trinnverdi'].str.replace(' ', '')
    mt2['MåleskalaNavn'] = mt2['Måleskala'].str.strip()
    mt2 = mt2.drop(['Enhet', 'Måleskala'], axis=1)
    mt2['Trinnverdi'] = mt2['Trinnverdi'].fillna(0)
    mt2
    mt = pd.concat([mt1, mt2], ignore_index=True)
    # Strip Trinn for white spaces
    mt['Trinn'] = mt['Trinn'].str.strip()
    # Drop rows where Trinn = '--'
    mt = mt[mt['Trinn'] != '--']
    # Set first letter in trinnverdi to upper case
    mt.loc[mt['Trinnverdi'].str.len() > 0, 'Trinnverdi'] = mt.loc[mt['Trinnverdi'].str.len() > 0, 'Trinnverdi'].str.replace(';', ':')
    mt['Trinnverdi'] = mt['Trinnverdi'].str.capitalize() # capitalize first letter
    mt = mt[mt['Trinn'] != 'nan_nan']
    mt.to_csv('ut_data/maaleskala_trinn.csv', index=False, sep=";")
    #display(mt.head(3))
    #mt.tail(10)

def variabelnavn_maaleskala_mapping_csv(nin3_variabler):
    vn_ms = nin3_variabler[nin3_variabler['11 Tr/Kl'] == 'W'][['Kortkode', '8 VarKode2', '10 Målesk']].drop_duplicates()
    #display(vn_ms)
    #display(vn_ms[vn_ms['Kortkode'] == 'LM-KI_e'])
    vn_ms = vn_ms.dropna(subset=['10 Målesk'])  # drop rows where '10 Målesk' has no value
    vn_ms = vn_ms.dropna(subset=['8 VarKode2']) # drop rows where '8 VarKode2' has no value
    # create a new dataframe to store the replicated rows
    new_rows = []


    # iterate over each row in the dataframe
    for index, row in vn_ms.iterrows():
        # check if the '10 Målesk' column has multiple values separated by comma
        if ',' in row['10 Målesk']:
            # if yes, split the values and create a new row for each value
            values = row['10 Målesk'].split(',')
            for value in values:
                new_row = row.copy()
                new_row['10 Målesk'] = value.strip()
                new_rows.append(new_row)
        else:
            # if no, just append the original row to the new dataframe
            new_rows.append(row)    

    # create a new dataframe from the list of new rows
    vn_ms_new = pd.DataFrame(new_rows)

    # add Varkode2+"-" in front of [10 Målesk]-value if [10 Målesk] is "SO" or "SI"
    mask = (vn_ms_new['10 Målesk'] == 'SO') | (vn_ms_new['10 Målesk'] == 'SI')
    vn_ms_new.loc[mask, '10 Målesk'] = vn_ms_new.loc[mask, '8 VarKode2'] + '-' + vn_ms_new.loc[mask, '10 Målesk']
    # replace underscores with dash in Kortkode column
    vn_ms_new['Kortkode'] = vn_ms_new['Kortkode'].str.replace('_', '-')
    # make all rows unique
    vn_ms_new = vn_ms_new.drop_duplicates()
    vn_ms_new = vn_ms_new.drop(['8 VarKode2'], axis=1)
    # display the new dataframe
    vn_ms_new.rename(columns={'Kortkode':'Variabelnavn_kortkode', '10 Målesk': 'Måleskala'}, inplace=True)
    vn_ms_new.to_csv('ut_data/variabelnavn_maaleskala_mapping.csv', index=False, sep=";")  

def variabelnavn_konvertering_csv(nin3_variabler, koder23):
    import csv
    # hent alle nødvendige kolonner
    # kjør string trim på alle kolonner
    konv = nin3_variabler[['Kortkode', 'NiN 2 kode', 'FP', 'SP']].copy()
    konv = konv.drop(konv[(konv['NiN 2 kode'] == '') | (konv['NiN 2 kode'] == '-')].index) # remove rows where 'NiN 2 kode' is empty
    konv = konv.drop(konv[(konv['Kortkode'] == '') | (konv['Kortkode'] == '-') |(konv['Kortkode'] == 'nan')].index) # remove rows where 'Kortkode' is empty
    konv = konv.dropna(subset=['Kortkode'])
    konv = konv.drop(konv[(konv['NiN 2 kode'] == 'nan')].index)
    konv['NiN 2 kode'] = konv['NiN 2 kode'].str.replace('&', ',') # '&' means ',' so change to ','
    konv['NiN 2 kode'] = konv['NiN 2 kode'].str.replace(' ', '') # remove whitespace in 'NiN 2 kode'
    konv = konv.drop(konv[(konv['Kortkode'] == '') | (konv['Kortkode'] == '-') |(konv['Kortkode'] == 'nan')].index)
    konv['SP'] = konv['SP'].str.replace('.0', '').str.replace('nan', '')
    konv['FP'] = konv['SP'].str.replace('.0', '').str.replace('nan', '')
    konv['NiN 2 kode'] = konv['NiN 2 kode'].str.replace('·', '-')
    konv = konv.rename(columns={'NiN 2 kode': 'forrigekode'})
    konv['Klasse'] = 'VN'# setter alle rader i kolonnen 'Klasse' til 'VN'
    konv.to_csv('tmp/variabelnavn_konv_tmp.csv', index=False, sep=";")
    #konv.info() 
    # store to temp csv file

    # FROM GT<>KONV example
    gt_rows = []
    with open('tmp/variabelnavn_konv_tmp.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        all_rows = []
        #check rows for multiple nin2koder
        for r in reader:
            if "," in r.get("forrigekode"):
                n2list = make_list_nin2kode(r.get("forrigekode"))
                for n2 in n2list:
                    new_row = r.copy()
                    new_row["forrigekode"] = n2.strip()
                    all_rows.append(new_row)
        
        #loop over rows
        for row in all_rows:
            new = {} # new dict to store the new row
            new['VNKode']=row['Kortkode']
            new['FP']=row['FP']
            new['SP']=row['SP']
            new['Klasse']=row['Klasse']
            result = create_v23_variabel_url(row['forrigekode'], koder23)
            new['forrigekode']=result.get('kode23')
            new['url']=result.get('url')
            if not new.get('url'):# bytter siste "-" med "_" og prøver igjen
                kode = row['forrigekode'].rsplit('-', 1)
                kode = '_'.join(kode)
                result = create_v23_variabel_url(kode, koder23)
                new['forrigekode']=result.get('kode23', koder23)
                new['url']=result.get('url')
            # Sjekker om excel-kolonnen forrigekode (nin 2 koden) hadde match i koder23-dictonary 
            if not new.get('url'):
                new['forrigekode']=row['forrigekode'] 
                print(f"\t\tFant ikke url for NiN2kode:{row['forrigekode']}")
            gt_rows.append(new)

    # Creating konvertering csv for GT
    with open('ut_data/konvertering_vn_v30.csv', 'w', newline='') as csvfile:
        fieldnames = ['VNKode','forrigekode','FP','SP','Klasse', 'url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(gt_rows)
        print(f"\n\nFile written to {csvfile.name}")   


# MAIN
def create_csv_files():
    import conf
    regnearkfil = conf.regnearkfil
    print(f"*** Backing up and removing previous csv files")
    backup_and_remove_previous_csv_files()

    print(f"*** Fetching data from Excel file: {regnearkfil}")
    nin3_typer = load_nin3_typer_sheet()
    nin3_typer_orig = nin3_typer.copy(deep=True)
    nin3_typer.rename(columns={
            '3 AbC': 'Ecosystnivå',
            '4 kat1': 'Typekategori',
            '5 kat2': 'Typekategori2',
            '6 kat3': 'Typekategori3',
            '7 HTG': 'Hovedtypegruppe',
            '8 Pk': 'Prosedyrekategori',
            '9 HT': 'Hovedtype'
            }, inplace=True)
    
    nin3_variabler = load_nin3_variabler_sheet()

    print(f"*** Creating typer.csv")
    typer_csv(nin3_typer)
    print(f"*** Creating hovedtypegrupper.csv")
    hovedtypegruppe_csv(nin3_typer)
    print(f"*** Creating type_htg_mapping.csv")
    type_htg_csv(nin3_typer)
    print(f"*** Creating hovedtype.csv and hovedtypegruppe_hovedtype_mapping.csv")
    hovedtype_and_ht_htg_mapping_csv(nin3_typer)
    print(f"*** Creating grunntyper.csv")
    grunntyper_csv(nin3_typer)
    print(f"*** Creating hovedtype_grunntype_mapping.csv")
    hovedtype_grunntype_mapping_csv(nin3_typer)
    print(f"*** Creating M005.csv")
    m005_csv(nin3_typer, regnearkfil)
    print(f"*** Creating m005_grunntype_mapping.csv")
    m005_grunntype_mapping_csv(nin3_typer)
    print(f"*** Creating m005_hovedtype_mapping.csv")
    m005_hovedtype_mapping_csv(nin3_typer)
    print(f"*** Creating m020.csv")
    m020_csv(nin3_typer, regnearkfil)
    print(f"*** Creating m020_grunntype_mapping.csv")
    m020_grunntype_mapping_csv(nin3_typer)
    print(f"*** Creating m020_hovedtype_mapping.csv")
    m020_hovedtype_mapping_csv(nin3_typer)
    print(f"*** Creating m050.csv")
    m050_csv(nin3_typer, regnearkfil)
    print(f"*** Creating m050_grunntype_mapping.csv")
    m050_grunntype_mapping_csv(nin3_typer)
    print(f"*** Creating m050_hovedtype_mapping.csv")
    m050_hovedtype_mapping_csv(nin3_typer)
    print(f"*** Creating typeklasser_langkode_mapping.csv")
    typeklasser_langkode_mapping_csv(nin3_typer)
    print(f"*** Creating variabelnavnkode_varkode2.csv")
    variabelnavnkode_varkode2_csv(nin3_variabler)
    print(f"*** Creating grunntype_variabeltrinn_mapping.csv")
    grunntype_variabeltrinn_mapping_csv(nin3_typer, nin3_typer_orig)
    print(f"*** Creating hovedtype_variabeltrinn_mapping.csv")
    hovedtype_variabeltrinn_mapping_csv(nin3_typer, nin3_typer_orig)
    print(f"*** Creating preperation_for 3.0 to 23 convertions")
    koder23 = prepare_konvertering_3_to_23()
    print(f"*** Creating htg_konv.csv")
    htg_conv_csv(nin3_typer,nin3_typer_orig, koder23)
    print(f"*** Creating ht_konv.csv")
    ht_conv_csv(nin3_typer,nin3_typer_orig, koder23)
    print(f"*** Creating gt_konv.csv")
    gt_conv_csv(nin3_typer,nin3_typer_orig, koder23)
    print(f"*** Creating variabel.csv")
    variabel_csv(nin3_variabler)
    print(f"*** Creating variabelnavn_variabel_mapping.csv")
    variabelnavn_variabel_mapping_csv(nin3_variabler)
    print(f"*** Creating maaleskala_enhet.csv")
    maaleskala_enhet_csv(nin3_variabler)
    print(f"*** Creating maaleskala_trinn.csv")
    maaleskala_trinn_csv(nin3_variabler)
    print(f"*** Creating variabelnavn_maaleskala_mapping.csv")
    variabelnavn_maaleskala_mapping_csv(nin3_variabler)
    print(f"*** Creating variabelnavn_konvertering.csv")
    variabelnavn_konvertering_csv(nin3_variabler, koder23)


    print("Done!")







    