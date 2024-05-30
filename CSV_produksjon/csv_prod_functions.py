import pandas as pd
import shutil
import os

def copy_changelog():
    src = 'inn_data/endringslogg.csv'
    dst = 'ut_data/endringslogg.csv'
    if not os.path.exists('ut_data'):
        os.makedirs('ut_data')
    shutil.copyfile(src, dst)
    

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
        print(f"\tNo duplicates in column {kolonne}")
    else:
        print("\tFound following duplicates in column {kolonne}:\n\n")
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

def load_nin3_variabler_sheet():
    import pandas as p
    import conf
    regnearkfil = conf.regnearkfil
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    nin3_variabler = pd.read_excel(regnearkfil, sheet_name='Variabler',
                                   dtype={'FP':str, 'SP':str})
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
                               na_filter=False,
                               dtype={'9 HT': str,'11 GT': str}  # Specify the data type of column [11 GT] as string
                               )
    # Remove leading and trailing whitespaces from string columns
    nin3_typer = nin3_typer.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    nin3_typer.to_csv('tmp/nin3_typer.csv', index=False, sep=";")
    return nin3_typer

# CSV creation:
def backup_and_remove_previous_csv_files():
    import os
    import zipfile
    import datetime
    folder_path = 'ut_data'
    # Taking backup of previous csv files
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    zip_filename = f'archive/csv_files_{timestamp}.zip'
    
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.csv'):
                file_path = os.path.join(folder_path, file_name)
                zipf.write(file_path, file_name)
    print(f"\tCSV files zipped successfully. Zip file name: {zip_filename}")

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
    hovedtypegrupper = nin3_typer[['HTGKode','Typekategori','Typekategori2','Hovedtypegruppe', 'Hovedtypegruppenavn', 'Typekategori3', 'Hovedtype']]
    hovedtypegrupper = hovedtypegrupper.applymap(lambda x: x.strip() if isinstance(x, str) else x) #Setter alle kolonner til string
    hovedtypegrupper['Typekategori3'] = hovedtypegrupper['Typekategori3'].replace('', '0').fillna('0') # BYTTER UT tomme verdier med 0 på Typekategori3

    # Convert all columns to string type for consistency
    hovedtypegrupper = hovedtypegrupper.astype(str)

    # Replace NaN values with a consistent value
    hovedtypegrupper = hovedtypegrupper.fillna('0')
    hovedtypegrupper = hovedtypegrupper[hovedtypegrupper['Hovedtypegruppenavn'] != '0']
    hovedtypegrupper2 = hovedtypegrupper.loc[:, ['Typekategori2', 'Hovedtypegruppe', 'Hovedtypegruppenavn', 'Typekategori3', 'HTGKode']]

    hovedtypegrupper2 = hovedtypegrupper2.drop_duplicates(subset=['HTGKode'])
    hovedtypegrupper2 = hovedtypegrupper2.sort_values(by=['HTGKode'])
    hovedtypegrupper2.to_csv('ut_data/hovedtypegrupper.csv', index=False, sep=";")
    
    # Controlling uniqueness of Kode column
    #--------------------------------------
    sjekk_unikhet(hovedtypegrupper2, 'HTGKode')


def type_htg_mapping_csv(nin3_typer):
    # fetch all kodecolumns for Type and HTG
    typer_htg_0 = nin3_typer[['HTGKode','Ecosystnivå', 'Typekategori', 'Typekategori2', 'Hovedtypegruppe']] 
    typer_htg_0 = typer_htg_0.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    typer_htg_0.rename(columns={'HTGKode': 'htgkode'}, inplace=True)
    typer_htg_0["typekode"] = typer_htg_0['Ecosystnivå'].map(str)+'-'+typer_htg_0['Typekategori'].map(str)+'-'+typer_htg_0['Typekategori2'].map(str)
    #typer_htg = typer_htg_0.iloc[:, -2:] # select last two columns (typekode, thgkode)
    typer_htg = typer_htg_0[['typekode', 'htgkode']]
    typer_htg = typer_htg.groupby(['typekode', 'htgkode']).count().reset_index()
    #typer_htg
    typer_htg.to_csv('ut_data/type_htg_mapping.csv', index=False, sep=";")

def hovedtype_csv_first(nin3_typer):
    ht_df = nin3_typer[['HTKode', 'Hovedtypenavn', 'Hovedtypegruppe', 'Prosedyrekategori', 'Hovedtype', 'HTGKode', 'Grunntype']]
    ht1 = ht_df[ht_df['Hovedtype'] != '-'] # dropping rows where Hovedtype is '-'
    ht1 = ht1[ht1['Grunntype']=='0']
    #Filter away fake hovedtype
    ht1.drop_duplicates(subset=['HTKode', 'Hovedtypenavn'], inplace=True)
    
    ht1 = ht1[ht1['Hovedtypenavn'].str.len() > 0]  # Drop rows where string length of Hovedtypenavn-value is 0
    # Create hovedtype.csv
    ht1 = ht1.reindex(columns=['HTKode', 'Hovedtypenavn', 'Hovedtypegruppe', 'Prosedyrekategori', 'Hovedtype', 'HTGKode'])
    ht1 = ht1.sort_values(by=['HTKode'])
    return ht1

def hovedtype_csv(nin3_typer):
    ht1_sorted = hovedtype_csv_first(nin3_typer)
    ht1_sorted.to_csv('ut_data/hovedtype.csv', index=False, sep=";")
    sjekk_unikhet(ht1_sorted, 'HTKode')

def hovedtype_hovedtypegruppe_mapping_csv(nin3_typer):
    ht1_sorted = hovedtype_csv_first(nin3_typer)
    htg_ht = ht1_sorted[['HTKode', 'HTGKode']]
    htg_ht.to_csv('ut_data/hovedtypegruppe_hovedtype_mapping.csv', index=False, sep=";")

    sjekk_unikhet(ht1_sorted, 'HTKode')

def grunntyper_csv(nin3_typer):
    grunntyper = nin3_typer[['Langkode','Hovedtypegruppe', 'Prosedyrekategori', 'Hovedtype', 'Grunntype', 'Grunntypenavn', 'GTKode']]
    grunntyper.rename(columns = {'11 GT':'Grunntype'}, inplace = True)
    grunntyper_vasket2 = grunntyper[grunntyper['Grunntypenavn'].str.len() > 1]#Filter: GT with name only
    grunntyper_vasket2 = grunntyper_vasket2.sort_values(by=['GTKode'])
    grunntyper_vasket2.to_csv('ut_data/grunntyper.csv', index=False, sep=";") # NaN blir blank string i csv
    sjekk_unikhet(grunntyper_vasket2,'GTKode')

def hovedtype_grunntype_mapping_csv(nin3_typer):
    ht_gt_mapping= nin3_typer[['HTKode', 'GTKode', 'Grunntypenavn']]
    ht_gt_mapping = ht_gt_mapping[ht_gt_mapping['Grunntypenavn'].str.len() > 1]#Filter: GT with name only   
    ht_gt_mapping = ht_gt_mapping.drop(['Grunntypenavn'], axis=1).sort_values(by=['GTKode'])
    ht_gt_mapping.to_csv('ut_data/hovedtype_grunntype_mapping.csv', index=False, sep=";")

def validate_last_joint(number, langkode):
            last_joint = langkode.split('-')[-1]
            last_joint = last_joint.replace('0', '')            
            try:
                last_joint_int = int(last_joint)
                nr_int = int(number)
                if (nr_int == last_joint_int):
                    return langkode
                else:
                    new_last_joint = str(nr_int)
                    if nr_int < 10:
                        print(f"\tLast joint of langkode and number are not the same: {langkode}, {new_last_joint} attempting fix of langkode")
                        new_last_joint = '0'+str(nr_int)
                    langkode = langkode.rsplit('-', 1)[0] + '-' + new_last_joint
                    return langkode
            except ValueError:
                print(f"Error: Last joint '{last_joint}' is not a valid integer.")
                return langkode

def m005_csv(nin3_typer):
    nin3_m005 = nin3_typer[['M005-kode', 'M005-navn', 'M005']]
    nin3_m005.rename(columns={'M005-kode': 'M005-langkode'}, inplace=True)  # rename column to 'M005-langkode' 
    nin3_m005 = nin3_m005[nin3_m005['M005-langkode'].str.contains('-M005-')]  # filter: only valid M005-langkode
    nin3_m005 = nin3_m005.dropna(subset=['M005-langkode'])# remove nan values
    for index, row in nin3_m005.iterrows():#115
        langkode = row['M005-langkode']
        m005 = row['M005']
        if len(langkode) > 3 and len(row['M005-navn'])>3 and len(row['M005'])>0:
            langkode = validate_last_joint(m005, langkode)
            row['M005-langkode'] = langkode
    nin3_m005 = nin3_m005.drop('M005', axis=1)
    nin3_m005['M005-kortkode'] = nin3_m005['M005-langkode'].str.extract(r'([A-Z]{2}\d+-M005-\d+)') # extract M005-kortkode
    nin3_m005 = nin3_m005[nin3_m005['M005-navn'].str.len() >= 2]  # remove rows where M005-navn str length is shorter than 2
    nin3_m005.drop_duplicates(subset=['M005-langkode', 'M005-navn'], inplace=True)
    nin3_m005 = nin3_m005.sort_values('M005-langkode') #order by M005-langkode  
    nin3_m005.to_csv('ut_data/M005.csv', index=False, sep=";")
    sjekk_unikhet(nin3_m005, 'M005-langkode')

def m005_grunntype_mapping_csv(nin3_typer):
    m005_gt = nin3_typer[['M005-kode','GTKode']] # select columns
    m005_gt.rename(columns = {'M005-kode':'M005-langkode','GTKode':'grunntype_kode'}, inplace = True) #rename columns
    m005_gt = m005_gt[m005_gt['M005-langkode'].str.contains('-M005-')] # Removing rows where langkode does not contain '-M005-'
    m005_gt.drop_duplicates(subset=['M005-langkode', 'grunntype_kode']) # Removing duplicates
    m005_gt = m005_gt.sort_values(by='M005-langkode') # Sorting by M005-langkode
    m005_gt.to_csv('ut_data/m005_grunntype_mapping.csv', index=False, sep=";")
    

def m005_hovedtype_mapping_csv(nin3_typer):
    m005_HT = nin3_typer[['M005-kode', 'HTKode']] # select columns
    m005_HT.rename(columns = {'M005-kode':'M005-langkode'}, inplace = True) #rename columns
    m005_HT = m005_HT[m005_HT['M005-langkode'].str.contains('-M005-')] # Removing rows where langkode does not contain '-M005-'
    m005_HT = m005_HT.drop_duplicates(subset=['M005-langkode', 'HTKode']) # Removing duplicates
    m005_HT = m005_HT.sort_values(by='M005-langkode') # Sorting by M005-langkode
    m005_HT.to_csv('ut_data/m005_hovedtype_mapping.csv', index=False, sep=";")

def m020_csv(nin3_typer):
    nin3_m020 = nin3_typer[['M020-kode', 'M020-navn', 'M020']]
    nin3_m020.rename(columns={'M020-kode': 'M020-langkode'}, inplace=True)  # rename column to 'M020-langkode'
    nin3_m020 = nin3_m020[nin3_m020['M020-langkode'].str.contains('-M020-')]
    nin3_m020 = nin3_m020.dropna(subset=['M020-langkode'])# remove nan values
    for index, row in nin3_m020.iterrows():#115
        langkode = row['M020-langkode']
        m020 = row['M020']
        if len(langkode) > 3 and len(row['M020-navn'])>3 and len(row['M020'])>0:
            langkode = validate_last_joint(m020, langkode)
            row['M020-langkode'] = langkode
    nin3_m020 = nin3_m020.drop('M020', axis=1)# remove column M020
    nin3_m020['M020-kortkode'] = nin3_m020['M020-langkode'].str.extract(r'([A-Z]{2}\d+-M020-\d+)')
    nin3_m020_filtered = nin3_m020[nin3_m020['M020-navn'].str.len() > 2] # remove rows where M020-navn str length is shorter than 2
    nin3_m020_filtered.drop_duplicates(subset=['M020-langkode', 'M020-navn'], inplace=True)#remove duplicates
    m020_sorted = nin3_m020_filtered.sort_values('M020-langkode') #order by M020-langkode
    m020_sorted.to_csv('ut_data/m020.csv', index=False, sep=";")
    sjekk_unikhet(m020_sorted, 'M020-langkode')

def m020_grunntype_mapping_csv(nin3_typer):
    m020_gt = nin3_typer[['M020-kode','GTKode']]
    m020_gt.rename(columns = {'M020-kode':'M020-langkode','GTKode':'grunntype_kode'}, inplace = True)#rename columns
    m020_gt = m020_gt[m020_gt['M020-langkode'].str.contains('-M020-')] # Removing rows where langkode does not contain '-M020-'
    m020_gt.drop_duplicates(subset=['M020-langkode', 'grunntype_kode']) # Removing duplicates
    m020_gt = m020_gt.sort_values(by='M020-langkode') # Sorting by M020-langkode
    m020_gt.to_csv('ut_data/m020_grunntype_mapping.csv', index=False, sep=";")

def m020_hovedtype_mapping_csv(nin3_typer):
    m020_HT = nin3_typer[['M020-kode', 'HTKode']] # select columns
    m020_HT.rename(columns = {'M020-kode':'M020-langkode'}, inplace = True) #rename columns
    m020_HT = m020_HT[m020_HT['M020-langkode'].str.contains('-M020-')] # Removing rows where langkode does not contain '-M020-'
    m020_HT = m020_HT.drop_duplicates(subset=['M020-langkode', 'HTKode']) # Removing duplicates
    m020_HT = m020_HT.sort_values(by='M020-langkode') # Sorting by M020-langkode
    m020_HT.to_csv('ut_data/m020_hovedtype_mapping.csv', index=False, sep=";")# create csv

def m050_csv(nin3_typer):
    m050 = nin3_typer[['M050-kode', 'M050-navn']] # select columns
    m050.rename(columns = {'M050-kode':'M050-langkode'}, inplace = True) #rename columns
    m050 = m050[m050['M050-langkode'].str.contains('-M050-')] # Removing rows where langkode does not contain '-M050-'
    m050 = m050[m050['M050-navn'].str.len() > 2] # remove rows where M050-navn str length is shorter than 2
    m050['M050-kortkode'] = m050['M050-langkode'].str.extract(r'([A-Z]{2}\d+-M050-\d+)') # create kortkode
    m050 = m050.drop_duplicates(subset=['M050-langkode', 'M050-navn']) # Removing duplicates
    m050 = m050.sort_values(by='M050-langkode') # Sorting by M050-langkode
    m050.to_csv('ut_data/m050.csv', index=False, sep=";") # create csv

def m050_grunntype_mapping_csv(nin3_typer):
    m050_gt = nin3_typer[['M050-kode','GTKode']]
    m050_gt.rename(columns = {'M050-kode':'M050-langkode','GTKode':'grunntype_kode'}, inplace = True) #rename columns
    m050_gt = m050_gt[m050_gt['M050-langkode'].str.contains('-M050-')] # Removing rows where langkode does not contain '-M050-'
    m050_gt.drop_duplicates(subset=['M050-langkode', 'grunntype_kode']) # Removing duplicates
    m050_gt = m050_gt.sort_values(by='M050-langkode') # Sorting by M050-langkode
    m050_gt.to_csv('ut_data/m050_grunntype_mapping.csv', index=False, sep=";")# create csv

def m050_hovedtype_mapping_csv(nin3_typer):
    m050_HT = nin3_typer[['M050-kode', 'HTKode']] # select columns
    m050_HT.rename(columns = {'M050-kode':'M050-langkode'}, inplace = True) #rename columns
    m050_HT = m050_HT[m050_HT['M050-langkode'].str.contains('-M050-')] # Removing rows where langkode does not contain '-M050-'
    m050_HT = m050_HT.drop_duplicates(subset=['M050-langkode', 'HTKode']) # Removing duplicates
    m050_HT = m050_HT.sort_values(by='M050-langkode') # Sorting by M050-langkode
    m050_HT.to_csv('ut_data/m050_hovedtype_mapping.csv', index=False, sep=";")# create csv

def grunntype_variabeltrinn_mapping_csv(nin3_typer):
    gt_vt = nin3_typer[nin3_typer['10 GT/kE'] == 'G'][['GTKode', 'Definisjonsgrunnlag', 'Grunntypenavn']]
    gt_vt_1 = gt_vt[(gt_vt['Grunntypenavn'] != '-')
                        & (gt_vt['Grunntypenavn'] != '')]#Removing rows where grunntype has no name(assert their not a grunntype)
    gt_vt_1 = gt_vt_1[['GTKode', 'Definisjonsgrunnlag']]#Removing column not needed anymore
    gt_vt_1['Definisjonsgrunnlag'] = gt_vt_1['Definisjonsgrunnlag'].str.replace('[\[\]]', '')
    gt_vt_1['Definisjonsgrunnlag'] = gt_vt_1['Definisjonsgrunnlag'].str.replace('[', '')#removing brackets 1/2
    gt_vt_1['Definisjonsgrunnlag'] = gt_vt_1['Definisjonsgrunnlag'].str.replace(']', '')#removing brackets 2/2
    gt_vt_1['Definisjonsgrunnlag'] = gt_vt_1['Definisjonsgrunnlag'].str.replace('-','_')#correcting notation for trinn(ref taskboard #64)
    #generate,duplicate rows based on ','-separator in Definisjonsgrunnlag
    gt_vt_1['Definisjonsgrunnlag'] = gt_vt_1['Definisjonsgrunnlag'].str.split(',')
    gt_vt_1 = gt_vt_1.explode('Definisjonsgrunnlag')

    gt_vt_1 = gt_vt_1[gt_vt_1['Definisjonsgrunnlag'] != "_"]#removing rows with no varkode2 or trinn
    gt_vt_1 = gt_vt_1[gt_vt_1['Definisjonsgrunnlag'] != ""]#removing rows with no varkode2 or trinn
    gt_vt_1['Varkode2'] = gt_vt_1['Definisjonsgrunnlag'].str.split('_').str[0] #create columns for varkode2
    gt_vt_1['Varkode2'] = gt_vt_1['Varkode2'].str.upper() #varkode2 is always uppercase (issue #104)
    

    gt_vt_1['Trinn_end'] = gt_vt_1['Definisjonsgrunnlag'].str.split('_').str[1] #create row with collection of trinn characters
    gt_vt_1['Trinn_end'] = gt_vt_1['Trinn_end'].fillna('')
    gt_vt_w_trinn = gt_vt_1[gt_vt_1['Trinn_end'] != '']
    gt_vt_no_trinn = gt_vt_1[gt_vt_1['Trinn_end'] == '']#in case there are rows with varkode2 but no trinn

    # Explode() into seperate rows per characters in Trinn_end
    gt_vt_w_trinn['Trinn_end'] = gt_vt_w_trinn['Trinn_end'].str.split('')
    gt_vt_w_trinn = gt_vt_w_trinn.explode('Trinn_end')
    gt_vt_w_trinn = gt_vt_w_trinn[gt_vt_w_trinn['Trinn_end'] != ""]

    gt_vt_w_trinn = gt_vt_w_trinn.apply(lambda x: x.str.strip() if x.dtype == "object" else x)#removing whitespace from string columns
        #Merge rows_w_trinn with variabelnavnkode_varkode
    variabelnavnkode_varkode2 = pd.read_csv('tmp/vnkode_vk2_trinn.csv', sep=";", encoding='utf-8')
    gt_vt_w_trinn = gt_vt_w_trinn.merge(variabelnavnkode_varkode2, left_on=['Varkode2','Trinn_end'], right_on=['Varkode2_kopi', 'sub_trinn'], how='left')
    gt_vt_w_trinn_no_hits = gt_vt_w_trinn[gt_vt_w_trinn['TrinnKode'].isna()]#Dataframe that shows rows where merge did not find a match

    if gt_vt_w_trinn_no_hits.shape[0] > 0: #Creating errormessage and result-file for rows without matches
        filename = 'tmp/gt_vt_trinn_no_matches.csv'
        print(f"Warning: Rows without match ({gt_vt_w_trinn_no_hits.shape[0]}):\n written to {filename}")
        gt_vt_w_trinn_no_hits.to_csv(filename, index=False)

    # join gt_vt_w_trinn with gt_vt_no_trinn if gt_vt_no_trinn has rows
    if gt_vt_no_trinn.shape[0] > 0:# (at the time om implementing this gt_vt_no_trinn has no rows)
        gt_vt_no_trinn = gt_vt_no_trinn.merge(variabelnavnkode_varkode2, left_on=['Varkode2','Trinn_end'], right_on=['Varkode2_kopi', 'sub_trinn'], how='left')
        gt_vt_no_trinn = gt_vt_no_trinn.fillna('')
        gt_vt_w_trinn = pd.concat([gt_vt_w_trinn, gt_vt_no_trinn])
    gt_vt_done = gt_vt_w_trinn[['GTKode', 'Varkode2', 'TrinnKode', 'VNKode','Definisjonsgrunnlag']]
    gt_vt_done = gt_vt_done.drop_duplicates()
    gt_vt_done = gt_vt_done.sort_values(by=['GTKode'])
    gt_vt_done.to_csv('ut_data/grunntype_variabeltrinn_mapping.csv', index=False, sep=";")

def hovedtype_variabeltrinn_mapping_csv(nin3_typer_orig):
    ht_variabeltrinn = nin3_typer_orig[(nin3_typer_orig['9 HT'] != '0') & (nin3_typer_orig['11 GT']=='0')][['Definisjonsgrunnlag', 'HTKode']]
    ht_variabeltrinn = ht_variabeltrinn[~ht_variabeltrinn['Definisjonsgrunnlag'].str.contains(';')]# removing rows with ';' in Definisjonsgrunnlag, disse er uforståelige data
    ht_variabeltrinn['Definisjonsgrunnlag'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.split(',')
    ht_variabeltrinn = ht_variabeltrinn.explode('Definisjonsgrunnlag')
    ht_variabeltrinn['Definisjonsgrunnlag'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.replace("(‒)", "") #removing '(-)' from definisjonsgrunnlag
    ht_variabeltrinn['Definisjonsgrunnlag'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.replace("( ‒)", "") #removing '( ‒)' from definisjonsgrunnlag
    ht_variabeltrinn['Definisjonsgrunnlag'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.replace("[", "") #removing left brackets
    ht_variabeltrinn['Definisjonsgrunnlag'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.replace("]", "") #removing right brackets
    ht_variabeltrinn['Definisjonsgrunnlag'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.strip() #removing sourrounding whitespace
    ht_variabeltrinn['Definisjonsgrunnlag'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.split(',')
    ht_variabeltrinn = ht_variabeltrinn.explode('Definisjonsgrunnlag') # explode dataframe to sperate list elements into separate rows
    ht_variabeltrinn['Definisjonsgrunnlag'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.split(' ')
    ht_variabeltrinn = ht_variabeltrinn.explode('Definisjonsgrunnlag') # explode dataframe to separate list elements into separate rows

    ht_variabeltrinn['Varkode2'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.split('_').str[0]
    ht_variabeltrinn['Varkode2'] = ht_variabeltrinn['Varkode2'].str.upper() #varkode2 is always uppercase (issue #104)
    #removing invalid Varkode2 values UK, NS, N, K, '‒)'
    ht_variabeltrinn = ht_variabeltrinn[~ht_variabeltrinn['Varkode2'].isin(['UK','NS','N','K', '‒)'])]
    ht_variabeltrinn['Trinn_end'] = ht_variabeltrinn['Definisjonsgrunnlag'].str.split('_').str[1]
    ht_variabeltrinn['Trinn_end'] = ht_variabeltrinn['Trinn_end'].fillna('')
    ht_rows_w_trinn = ht_variabeltrinn[ht_variabeltrinn['Trinn_end'] != '']
    ht_rows_no_trinn = ht_variabeltrinn[ht_variabeltrinn['Trinn_end'] == '']

    # Split Trinn_end into separate characters
    ht_rows_w_trinn['Trinn_end'] = ht_rows_w_trinn['Trinn_end'].str.split('')
    ht_rows_w_trinn = ht_rows_w_trinn.explode('Trinn_end') #Split and explode Trinn_end to separate characters
    ht_rows_w_trinn = ht_rows_w_trinn[ht_rows_w_trinn['Trinn_end'] != ''] #removing empty trinn values

    #writing trinn as trinn.kortkode (adding variabelnavn as prefix):
        #Merge rows_w_trinn with variabelnavnkode_varkodee
    variabelnavnkode_varkode2 = pd.read_csv('tmp/vnkode_vk2_trinn.csv', sep=";")
    ht_rows_w_trinn = ht_rows_w_trinn.merge(variabelnavnkode_varkode2, left_on=['Varkode2','Trinn_end'], right_on=['Varkode2_kopi', 'sub_trinn'], how='left')

    #prepping ht_rows_no_trinn for join
    ht_rows_no_trinn = ht_rows_no_trinn.merge(variabelnavnkode_varkode2, left_on='Varkode2', right_on='Varkode2_kopi', how='left')
    ht_rows_no_trinn['TrinnKode'] = ''
    ht_rows_no_trinn = ht_rows_no_trinn.fillna('')

    if ht_rows_no_trinn.shape[0] > 0:
        filename = 'tmp/ht_vt_trinn_no_matches.csv'
        print(f"Warning: Rows without match ({ht_rows_no_trinn.shape[0]}) \n written to {filename}")
        ht_rows_no_trinn.to_csv(filename, index=False)

    ht_variabeltrinn_join = pd.concat([ht_rows_w_trinn, ht_rows_no_trinn]) #joining rows of ht_rows_w_trinn and ht_rows_no_trinn
    ht_variabeltrinn_join = ht_variabeltrinn_join[ht_variabeltrinn_join['Varkode2'] != ""]#removing noise from joning
    ht_variabeltrinn_done = ht_variabeltrinn_join[['HTKode','Varkode2','TrinnKode','VNKode', 'Definisjonsgrunnlag']]
    #ht_variabeltrinn_done = ht_variabeltrinn_done.rename(columns={"Kortkode": "Variabelnavn_kortkode"})
    ht_variabeltrinn_done = ht_variabeltrinn_done.drop_duplicates()
    ht_variabeltrinn_done = ht_variabeltrinn_done.sort_values(by=['HTKode'])

    ht_variabeltrinn_done.to_csv('ut_data/hovedtype_variabeltrinn_mapping.csv', index=False, sep=";")


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
    df.to_csv('tmp/2_3_koder_fra_sqlite_prod.csv', sep=';', index=False)

    # Load v2.3 koder to dict
    koder23 = {}
    with open('tmp/2_3_koder_fra_sqlite_prod.csv', newline='') as csvfile:
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
    htg0 = nin3_typer_orig[['HTGKode','3 AbC', '4 kat1', '5 kat2', '6 kat3','7 HTG', '8 Pk','9 HT', '11 GT', 'NiN 2 kode', 'FP', 'SP']]
    #display(htg0)
    # remove heading and tailing spaces from all columns
    htg0 = htg0.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    #print(htg0.dtypes)
    htg0_1 = htg0[
        (htg0['5 kat2'] != 'LA') & # holdes midlertidig utenfor
        (htg0['4 kat1'] != 'LI') & # holdes midlertidig utenfor
        (htg0['9 HT'] == '0') & 
        (htg0['11 GT'] == '0')
        ][['HTGKode','3 AbC', '4 kat1', '5 kat2', '6 kat3','7 HTG', '8 Pk','9 HT', '11 GT', 'NiN 2 kode', 'FP', 'SP']]
   
   
    # cleaning up the dataframe for non-HTG rows
    htg1 = htg0_1[htg0_1['NiN 2 kode'] != '-'] 
    htg1 = htg1[htg1['NiN 2 kode'] != '']

    htg2 = htg1[['HTGKode', 'NiN 2 kode', 'FP', 'SP']]
    htg2 = htg2.rename(columns={'NiN 2 kode': 'forrigekode'})
    htg2['Klasse'] = 'HTG'
    htg2.to_csv('tmp/htg_konv.csv', sep=';', index=False)

    # Open the file as dictreader
    htg_rows = []

    with open('tmp/htg_konv.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            new = {} # new dict to store the new row
            new['HTGKode']=row['HTGKode']
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
        fieldnames = ['HTGKode','forrigekode','FP','SP','Klasse', 'url']
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
        (ht0['11 GT']=='0')][['HTKode','3 AbC', '4 kat1', '5 kat2', '6 kat3','7 HTG', '8 Pk','9 HT', '11 GT', 'NiN 2 kode', 'FP', 'SP']]
    # remove heading and tailing spaces from all columns

    ht0_0 = ht0_0[ht0_0['NiN 2 kode'] != '-']
    ht0_0 = ht0_0[ht0_0['NiN 2 kode'] != '']
    ht1 = ht0_0
    #ht1['HTkode'] = ht1['5 kat2'].str[0]+ht1['7 HTG'].map(str)+'-'+ht1['8 Pk'].map(str)+'-'+ht1['9 HT'].map(str)

    ht2 = ht1[['HTKode', 'NiN 2 kode', 'FP', 'SP']]
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
            new['HTKode']=row['HTKode']
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
        fieldnames = ['HTKode','forrigekode','FP','SP','Klasse', 'url']
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
        (gt0['11 GT']!='0')][['GTKode','3 AbC', '4 kat1', '5 kat2', '6 kat3','7 HTG', '8 Pk','9 HT', '11 GT', 'NiN 2 kode', 'FP', 'SP']]

    #gt0_0['GTKode'] = gt0_0['7 HTG'].map(str)+'-'+gt0_0['8 Pk'].map(str)+'-'+gt0_0['9 HT'].map(str)+'-'+gt0_0['11 GT'].map(str)
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
    variabler = variabler[variabler['Variabel_kode'] != 'nan-nan']    
    variabler.to_csv('ut_data/variabel.csv', index=False, sep=";")

def variabelnavn_variabel_mapping_csv(nin3_variabler):
    vvm = nin3_variabler[nin3_variabler['11 Tr/Kl'] == 'W'][['Langkode', 'Kortkode','3 ABC', '4 NM', '5 kat', '6 FG/EK', '7 Varkode1', '8 VarKode2', 'VarNavn']].drop_duplicates()
    vvm = vvm.rename(columns={'5 kat': 'Variabelkategori2', 
                            '6 FG/EK': 'Variabeltype', '7 Varkode1': 'Variabelgruppe', '8 VarKode2': 'Variabelnavn_kode', 'VarNavn': 'Variabelnavn_navn'})
    vvm['Variabel_kode'] = vvm[['3 ABC', '4 NM']].apply(lambda x: '-'.join(x), axis=1)
    #dupl = vvm['Variabelnavn_kode'].duplicated().any()
    vvm.to_csv('ut_data/variabelnavn_variabel_mapping.csv', index=False, sep=";")

"""
Generates a CSV file containing information about measurement scales and units used in the NiN3 dataset.

The function `maaleskala_enhet_csv` takes a Pandas DataFrame `nin3_variabler` as input, which is assumed to contain the NiN3 dataset. It then processes the data to extract information about measurement scales and units, and writes this information to a CSV file named `maaleskala_enhet.csv` in the `ut_data` directory.

The function performs the following steps:

1. Separates the input DataFrame into two parts: one containing rows where the '11 Tr/Kl' column is not 'W', and the other containing rows where it is 'W'.
2. For the first part, it extracts the '8 VarKode2' and '10 Målesk' columns, renames the '10 Målesk' column to 'Måleskala', and fills in the 'Enhet' column with 'VSO' or 'VSI' based on the 'Måleskala' value.
3. For the second part, it reads a Markdown file named `maaleskala_trinn_enhet.md` from the `inn_data` directory, processes the contents of the file to create a Pandas DataFrame, and then extracts and processes the 'Måleskala' and 'Enhet' columns from this DataFrame.
4. Finally, it concatenates the two DataFrames and writes the resulting DataFrame to the `maaleskala_enhet.csv` file in the `ut_data` directory.
"""
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
    me_1['MåleskalaNavn'] = me_1['MåleskalaNavn'].str.replace(')', '') #112
    me_1 = me_1[me_1['MåleskalaNavn'] != 'nan-nan'] #113 :nan-nan, removing row where MåleskalaNavn = nan-nan
    me_1 = me_1.drop('8 VarKode2', axis=1)
    me_1 = me_1.drop_duplicates()
    #me_1.head(5) 

    # part 2/2
    me_2 = pd.read_csv('inn_data/maaleskala_trinn_enhet_del2.csv', sep=";", encoding= 'utf-8')
    # Remove any leading/trailing white spaces from the column names and drop any empty rows
    me_2 = me_2.drop(['Trinn', 'Trinnverdi'], axis=1)
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
    mt1 = nin3_variabler[['Langkode','Kortkode', '8 VarKode2', '10 Målesk', '11 Tr/Kl', 'Trinn/klassebetegnelse']].copy()
    mt1 = mt1[mt1['11 Tr/Kl'] != 'W']
    mt1['MåleskalaNavn'] = mt1['8 VarKode2'].astype(str) + '-' + mt1['10 Målesk'].astype(str).str.strip()
    mt1 = mt1[mt1['11 Tr/Kl'] != ''] #Dropping rows where Tr/Kl = ""
    mt1 = mt1[mt1['Kortkode'] != 'nan']    
    mt1 = mt1.rename(columns={'11 Tr/Kl': 'Trinn', 'Trinn/klassebetegnelse': 'Trinnverdi'})
    mt1['Trinn'] = mt1['Kortkode']
    mt1 = mt1.drop(['Kortkode', '8 VarKode2', '10 Målesk'], axis=1)

    # part 2/2
    # Fetching variabelnavnkode and måleskalakode
    # hent trinn fra md-fil
    # tilpass og concat med del1 dataframe
    mt2 = pd.read_csv('inn_data/maaleskala_trinn_enhet_del2.csv', sep=";", encoding= 'utf-8')
    mt2['Trinnverdi'] = mt2['Trinnverdi'].str.replace(' ', '')
    mt2['MåleskalaNavn'] = mt2['Måleskala'].str.strip()
    mt2 = mt2.drop(['Enhet', 'Måleskala'], axis=1)
    mt2['Trinnverdi'] = mt2['Trinnverdi'].fillna(0)
    mt = pd.concat([mt1, mt2], ignore_index=True)#union of the 2 dataframes
    # Strip Trinn for white spaces
    mt['Trinn'] = mt['Trinn'].str.strip()
    # Drop rows where Trinn = '--'
    mt = mt[mt['Trinn'] != '--']
    # Set first letter in trinnverdi to upper case
    mt.loc[mt['Trinnverdi'].str.len() > 0, 'Trinnverdi'] = mt.loc[mt['Trinnverdi'].str.len() > 0, 'Trinnverdi'].str.replace(';', ':')
    mt['Trinnverdi'] = mt['Trinnverdi'].str.capitalize() # capitalize letters
    mt = mt[mt['Trinn'] != 'nan_nan'] # remove null-rows
    mt['Langkode'] = mt['Langkode'].str.replace(")", "") #112
    mt['Trinn'] = mt['Trinn'].str.replace(")", "") #112
    mt['MåleskalaNavn'] = mt['MåleskalaNavn'].str.replace(")", "") #112
    mt = mt.drop_duplicates() #fix for task #66
    mt = mt.sort_values(by=['MåleskalaNavn']) # order by MåleskalaNavn
    mt.to_csv('ut_data/maaleskala_trinn.csv', index=False, sep=";")
    
def variabelnavn_maaleskala_mapping_csv(nin3_variabler):
    vn_ms = nin3_variabler[nin3_variabler['11 Tr/Kl'] == 'W'][['Kortkode', '8 VarKode2', '10 Målesk']].drop_duplicates()
    #display(vn_ms)
    #display(vn_ms[vn_ms['Kortkode'] == 'LM-KI_e'])
    vn_ms = vn_ms.dropna(subset=['10 Målesk'])  # drop rows where '10 Målesk' has no value
    vn_ms = vn_ms.dropna(subset=['8 VarKode2']) # drop rows where '8 VarKode2' has no value

    # Rewrite multiple maaleskala values [DT*, P*] (#117)
    vn_ms['10 Målesk'] = vn_ms['10 Målesk'].str.replace('DT*', 'D0,D1a,D1b,T0,T1a,T1b,T1c')
    vn_ms['10 Målesk'] = vn_ms['10 Målesk'].str.replace('D*', 'D0,D1a,D1b')
    vn_ms['10 Målesk'] = vn_ms['10 Målesk'].str.replace('T*', 'T0,T1a,T1b,T1c')
    vn_ms['10 Målesk'] = vn_ms['10 Målesk'].str.replace('P*', 'P4,P5,P6a,P6b,P6c,P7,P8,P9a,P9b')

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
    ##vn_ms_new['Kortkode'] = vn_ms_new['Kortkode'].str.replace('_', '-') ##skips this so reference is not broken (taskboard #52)
    # make all rows unique
    vn_ms_new = vn_ms_new.drop_duplicates()
    vn_ms_new = vn_ms_new.drop(['8 VarKode2'], axis=1)
    # display the new dataframe
    vn_ms_new.rename(columns={'Kortkode':'Variabelnavn_kortkode', '10 Målesk': 'Måleskala'}, inplace=True)
    vn_ms_new.to_csv('ut_data/variabelnavn_maaleskala_mapping.csv', index=False, sep=";")  

def variabelnavn_konvertering_csv(nin3_variabler, koder23):
    vn_conv = nin3_variabler[nin3_variabler['11 Tr/Kl'] == 'W'][['Kortkode','NiN 2 kode', 'FP', 'SP']]
    vn_conv = vn_conv[vn_conv['NiN 2 kode'] != '-']
    vn_conv = vn_conv[vn_conv['NiN 2 kode'] != '']
    vn_conv = vn_conv[vn_conv['NiN 2 kode'] != 'nan']
    vn_conv = vn_conv[vn_conv['FP'] != 'ny']

    # replace invalid values; 'nan',']','-' on FP,SP with ''
    vn_conv.loc[vn_conv['FP'] == 'nan', 'FP'] = ''
    vn_conv.loc[vn_conv['SP'] == 'nan', 'SP'] = ''
    vn_conv['SP'] = vn_conv['SP'].str.replace(']', "")
    vn_conv['FP'] = vn_conv['FP'].str.replace('-', "")
    vn_conv['SP'] = vn_conv['SP'].str.replace('-', "")
    # remove rows where NiN 2 kode = 'ny'
    vn_conv = vn_conv[vn_conv['NiN 2 kode'] != 'ny']
        


    # split rows with multiple NiN 2 kode values
    vn_conv['NiN 2 kode'] = vn_conv['NiN 2 kode'].str.split(',')
    vn_conv = vn_conv.explode('NiN 2 kode')
    vn_conv['NiN 2 kode'] = vn_conv['NiN 2 kode'].str.split(';')
    vn_conv = vn_conv.explode('NiN 2 kode')

    # remove whitespace
    vn_conv['NiN 2 kode'] = vn_conv['NiN 2 kode'].str.strip()

    # drop duplicates
    vn_conv = vn_conv.drop_duplicates()
    vn_conv = vn_conv.rename(columns={'NiN 2 kode': 'forrigekode'})
    # add columns 'Klasse' and 'url'
    vn_conv['Klasse'] = 'VN'
    vn_conv['url'] = ''

    #Iterates through each row in the `vn_conv` DataFrame and updates the 'url' column for that row based on the 'forrigekode' value and the `koder23` lookup data.

    #For each row:
    #1. Converts the 'forrigekode' value to lowercase and stores it in `forrigeKode_lookupkey`.
    #2. Calls the `create_v23_variabel_url()` function, passing `forrigeKode_lookupkey` and `koder23` as arguments, and stores the returned URL in the `url` variable.
    #3. Updates the 'url' column for the current row in the `vn_conv` DataFrame with the value from the `url` variable.

    for index, row in vn_conv.iterrows():
        # iterate through each row
        forrigeKode_lookupkey = row['forrigekode'].lower()
        result = create_v23_variabel_url(forrigeKode_lookupkey, koder23)
        url = result.get('url')
        vn_conv.at[index, 'url'] = url

    vn_conv.to_csv('ut_data/konvertering_vn_v30.csv', index=False, sep=";")  

def trinn_konvertering_csv(nin3_variabler):
    trinn_konv = nin3_variabler[['Kortkode','NiN 2 kode','FP','SP','11 Tr/Kl']]
    trinn_konv = trinn_konv.rename(columns={'NiN 2 kode': 'forrigekode'})

    #filter and 'wash' data
    trinn_konv = trinn_konv[trinn_konv['11 Tr/Kl']!='W']#removing variabler
    trinn_konv = trinn_konv.drop('11 Tr/Kl', axis=1)#dropping column '11 Tr/Kl'
    trinn_konv = trinn_konv[trinn_konv['Kortkode']!='nan']#removing trinn rows without kortkode
    trinn_konv = trinn_konv[~trinn_konv['forrigekode'].isin(['-', 'nan','ny'])]#removing rows with no previous kode
    trinn_konv = trinn_konv[trinn_konv['FP']!='ny']
    trinn_konv['FP'] = trinn_konv['FP'].replace('-', '')#remove '-' from int value, blank is null
    trinn_konv['FP'] = trinn_konv['FP'].replace('?', '')#remove '?' from int value
    trinn_konv['SP'] = trinn_konv['SP'].replace('-', '')#remove '-' from int value, blank is null
    trinn_konv['SP'] = trinn_konv['SP'].replace(r'\<', '', regex=True)#remove '<' from int value
    trinn_konv['SP'] = trinn_konv['SP'].str.replace(r'\?', '', regex=True)#remove '?' from int value

    trinn_konv['Klasse'] = 'VT'
    trinn_konv['url'] = ''
    trinn_konv.to_csv('ut_data/konvertering_trinn_v30.csv', sep=';', index=False)

def adjust_nin3_typer_col_names(nin3_typer):
        nin3_typer.rename(columns={
            '3 AbC': 'Ecosystnivå',
            '4 kat1': 'Typekategori',
            '5 kat2': 'Typekategori2',
            '6 kat3': 'Typekategori3',
            '7 HTG': 'Hovedtypegruppe',
            '8 Pk': 'Prosedyrekategori',
            '9 HT': 'Hovedtype',
            '11 GT': 'Grunntype',
            }, inplace=True)
        return nin3_typer

# STØTTEMETODER -Kortkoder
def kodeForHTG(langkode:str, kat1_4:str, kat2_5:str, htg_7:str):
    if not isinstance(langkode, str):
        print(f"langkode must be a string, langkode-value was:'{langkode}'")
        return ""
    ledd_arr = langkode.split('-')
    if ledd_arr[4]=="MV":
        kode = kat1_4+"-"+htg_7
        return kode    
    if ledd_arr[4]=="LI":
        kode = kat1_4+"-"+htg_7
        return kode    
    else:
        kode = kat2_5+"-"+htg_7
        return kode
    
def kodeForHT(langkode:str, kat1_4:str, kat2_5:str, htg_7:str, pk_8:str, ht_9:str):
    if not isinstance(langkode, str):
        print(f"langkode must be a string, langkode-value was:'{langkode}'")
        return ""
    ledd_arr = langkode.split('-')
    if ledd_arr[4]=="MV":
        kode =  kat1_4+"-"+htg_7+ht_9
        return kode
    if ledd_arr[4]=="LI":
        kode =  kat1_4+"-"+htg_7+ht_9
        return kode
    if ledd_arr[4]=="LV":
        kode =  kat2_5+"-"+htg_7+ht_9
        return kode
    if ledd_arr[5]=="NA":
        kode =  kat2_5+"-"+htg_7+pk_8+ht_9    
        return kode
    if ledd_arr[5]=="LA":
        kode =  kat2_5+"-"+htg_7+ht_9
        return kode
    if ledd_arr[5]=="NK":
        kode =  kat2_5+"-"+htg_7+ht_9
        return kode
    else: 
        return ""
    

def kodeForGT(langkode:str, htg_7:str, pk_8:str, ht_9:str, gt_11:str):
    if not isinstance(langkode, str):
        print(f"langkode must be a string, langkode-value was:'{langkode}'")
        return ""
    ledd_arr = langkode.split('-')
    if ledd_arr[4]=="LI":
        kode = htg_7+ht_9+"-"+gt_11
        return kode
    if ledd_arr[5]=="NA":
        kode = htg_7+pk_8+ht_9+"-"+gt_11
        return kode
    if ledd_arr[5]=="LA":
        kode = htg_7+ht_9+"-"+gt_11
        return kode
    else:
        return ""
    
def create_kortkoder_for_nin3_typer(nin3_typer):
    nin3_typer["HTGKode"] = nin3_typer.apply(lambda x: kodeForHTG(
        x['Langkode'], 
        x['Typekategori'], 
        x['Typekategori2'], 
        x['Hovedtypegruppe']), axis=1)
    nin3_typer["HTKode"] = nin3_typer.apply(lambda x: kodeForHT(
        x['Langkode'],
        x['Typekategori'],
        x['Typekategori2'],
        x['Hovedtypegruppe'],
        x['Prosedyrekategori'],
        x['Hovedtype']), axis=1)
    nin3_typer["GTKode"] = nin3_typer.apply(lambda x: kodeForGT(
        x['Langkode'],
        x['Hovedtypegruppe'],
        x['Prosedyrekategori'],
        x['Hovedtype'],
        x['Grunntype']), axis=1)
    
def create_kortkoder_for_nin3_typer_orig(nin3_typer_orig):
    nin3_typer_orig["HTGKode"] = nin3_typer_orig.apply(lambda x: kodeForHTG(
        x['Langkode'], 
        x['4 kat1'], 
        x['5 kat2'], 
        x['7 HTG']), axis=1)
    nin3_typer_orig["HTKode"] = nin3_typer_orig.apply(lambda x: kodeForHT(
        x['Langkode'],
        x['4 kat1'],
        x['5 kat2'],
        x['7 HTG'],
        x['8 Pk'],
        x['9 HT']), axis=1)
    nin3_typer_orig["GTKode"] = nin3_typer_orig.apply(lambda x: kodeForGT(
        x['Langkode'],
        x['7 HTG'],
        x['8 Pk'],
        x['9 HT'],
        x['11 GT']), axis=1)
    
def vnkode_vk2_trinn(nin3_variabler):
    vnkode_vk2_trinn = nin3_variabler[nin3_variabler['11 Tr/Kl'] != 'W'][['Kortkode', '8 VarKode2', '11 Tr/Kl']]
    vnkode_vk2_trinn = vnkode_vk2_trinn[vnkode_vk2_trinn['8 VarKode2']!='nan']
    vnkode_vk2_trinn.rename(columns={
        'Kortkode': 'TrinnKode',
        '8 VarKode2': 'Varkode2_kopi',
        '11 Tr/Kl': 'sub_trinn'
        }, inplace=True)
    vnkode_vk2_trinn['VNKode'] = vnkode_vk2_trinn['TrinnKode'].str.split('_').str[0]
    vnkode_vk2_trinn.to_csv("tmp/vnkode_vk2_trinn.csv", index=False, sep=";")


# MAIN
def create_csv_files():
    import conf
    regnearkfil = conf.regnearkfil
    print(f"*** Backing up and removing previous csv files")
    backup_and_remove_previous_csv_files()
    print("*** Fetching endringslog.csv")
    copy_changelog()
    print(f"*** Fetching data from Excel file: {regnearkfil}")
    nin3_typer = load_nin3_typer_sheet()
    nin3_typer_orig = nin3_typer.copy(deep=True)
    # Handling all columns as strings
    nin3_typer = nin3_typer.astype(str)
    nin3_typer_orig = nin3_typer_orig.astype(str)
    nin3_typer = adjust_nin3_typer_col_names(nin3_typer)
    # Setting kortkode columns for type
    create_kortkoder_for_nin3_typer(nin3_typer)  
    # issue #107  
    issue_107 = nin3_typer[['HTKode', 'HTGKode']]
    issue_107.to_csv("tmp/issue_107.csv", index=False, sep=";")#For validation   
    create_kortkoder_for_nin3_typer_orig(nin3_typer_orig)
    #nin3_typer.to_excel('tmp/nin3_typer.xlsx', index=False)
    print(f"*** Saving nin3_typer as tmp/nin3_typer.xlsx")
    nin3_variabler = load_nin3_variabler_sheet()
    print(f"*** Creating lookup-file: tmp/vnkode_vk2_trinn.csv")
    vnkode_vk2_trinn(nin3_variabler)
    print(f"*** Creating typer.csv")
    typer_csv(nin3_typer)
    print(f"*** Creating hovedtypegrupper.csv")
    hovedtypegruppe_csv(nin3_typer)
    print(f"*** Creating type_htg_mapping.csv")
    type_htg_mapping_csv(nin3_typer)
    print(f"*** Creating hovedtype.csv")
    hovedtype_csv(nin3_typer)
    print(f"*** Hovedtypegruppe_hovedtype_mapping.csv")
    hovedtype_hovedtypegruppe_mapping_csv(nin3_typer)
    print(f"*** Creating grunntyper.csv")
    grunntyper_csv(nin3_typer)
    print(f"*** Creating hovedtype_grunntype_mapping.csv")
    hovedtype_grunntype_mapping_csv(nin3_typer)
    print(f"*** Creating M005.csv")
    m005_csv(nin3_typer)
    print(f"*** Creating m005_grunntype_mapping.csv")
    m005_grunntype_mapping_csv(nin3_typer)
    print(f"*** Creating m005_hovedtype_mapping.csv")
    m005_hovedtype_mapping_csv(nin3_typer)
    print(f"*** Creating m020.csv")
    m020_csv(nin3_typer)
    print(f"*** Creating m020_grunntype_mapping.csv")
    m020_grunntype_mapping_csv(nin3_typer)
    print(f"*** Creating m020_hovedtype_mapping.csv")
    m020_hovedtype_mapping_csv(nin3_typer)
    print(f"*** Creating m050.csv")
    m050_csv(nin3_typer)
    print(f"*** Creating m050_grunntype_mapping.csv")
    m050_grunntype_mapping_csv(nin3_typer)
    print(f"*** Creating m050_hovedtype_mapping.csv")
    m050_hovedtype_mapping_csv(nin3_typer)
    print(f"*** Creating grunntype_variabeltrinn_mapping.csv")
    grunntype_variabeltrinn_mapping_csv(nin3_typer)
    print(f"*** Creating hovedtype_variabeltrinn_mapping.csv")
    hovedtype_variabeltrinn_mapping_csv(nin3_typer_orig)
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
    print(f"*** Creating trinn_konvertering.csv")
    trinn_konvertering_csv(nin3_variabler)
    print("Done!")







    