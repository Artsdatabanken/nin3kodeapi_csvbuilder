import os
import csv

def load_nin3_typer_sheet(regnearkfil, pd):
    import conf
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
    return nin3_typer

def load_m005_sheet(regnearkfil, pd):
    m005_df = pd.read_excel(regnearkfil, 
                            sheet_name='M005', 
                            na_filter=False)
    return m005_df[['M005-langkode','M005-navn']]

def load_m020_sheet(regnearkfil, pd):
    m020_df = pd.read_excel(regnearkfil, 
                            sheet_name='M020', 
                            na_filter=False)
    m020_df.rename(columns={'M020_kode': 'M020-langkode', 
                            'M020_navn':'M020-navn'}, inplace=True)                              
    return m020_df[['M020-langkode','M020-navn']]

def load_m050_sheet(regnearkfil, pd):
    m050_df = pd.read_excel(regnearkfil, 
                            sheet_name='M050', 
                            na_filter=False)
    m050_df.rename(columns={'M050_kode': 'M050-langkode'}, inplace=True)
    return m050_df[['M050-langkode','M050-navn']]

def load_typer_m005(regnearkfil, pd):
    nin3_typer = load_nin3_typer_sheet(regnearkfil, pd)
    m005_df = nin3_typer[['M005-kode','M005-navn']]
    m005_df = m005_df.drop_duplicates(subset=['M005-kode'])
    m005_df = m005_df.dropna(subset=['M005-kode'])
    m005_df.rename(columns={'M005-kode': 'M005-langkode'}, inplace=True)                       
    return m005_df

def load_typer_m020(regnearkfil, pd):  
    nin3_typer = load_nin3_typer_sheet(regnearkfil, pd)
    m020_df = nin3_typer[['M020-kode','M020-navn']]
    m020_df = m020_df.drop_duplicates(subset=['M020-kode'])
    m020_df = m020_df.dropna(subset=['M020-kode'])
    m020_df.rename(columns={'M020-kode': 'M020-langkode'}, inplace=True)
    return m020_df

def load_typer_m050(regnearkfil, pd):
    nin3_typer = load_nin3_typer_sheet(regnearkfil, pd)
    m050_df = nin3_typer[['M050-kode','M050-navn']]
    m050_df = m050_df.drop_duplicates(subset=['M050-kode'])
    m050_df = m050_df.dropna(subset=['M050-kode'])
    m050_df.rename(columns={'M050-kode': 'M050-langkode'}, inplace=True)
    return m050_df

def find_unique_values(df1, df2, column_name):
    common_values = set(df1[column_name]).intersection(df2[column_name])
    unique_values_df1 = set(df1[column_name]) - common_values
    unique_values_df2 = set(df2[column_name]) - common_values
    unique_values = list(unique_values_df1.union(unique_values_df2))
    return unique_values

def check_csv_folder(folder_path):
    arr = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            try:
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    num_lines = sum(1 for _ in reader)
                    file.seek(0)  # Reset file pointer to the beginning                    
                    arr.append({'file_name': file_name, 'num_lines': num_lines})
            except csv.Error as e:
                print(f"Error reading {file_name}: {e}")
    return arr

def main_kle():
    import pandas as pd
    regnearkfil = '../CSV_produksjon/inn_data/NiN3.0_Tot_e15_20231123_import_kodebase_NKfix.xlsx'
    #nin3typer = load_nin3_typer_sheet(regnearkfil, pd)
    m005_missing = find_unique_values(load_typer_m005(regnearkfil, pd), load_m005_sheet(regnearkfil, pd), 'M005-langkode')
    m020_missing = find_unique_values(load_typer_m020(regnearkfil, pd), load_m020_sheet(regnearkfil, pd), 'M020-langkode')
    m050_missing = find_unique_values(load_typer_m050(regnearkfil, pd), load_m050_sheet(regnearkfil, pd), 'M050-langkode')

    print("M005 missing:")
    print("\n".join(str(x) for x in m005_missing))
    print("M020 missing:")
    print("\n".join(str(x) for x in m020_missing))
    print("M050 missing:")
    print("\n".join(str(x) for x in m050_missing))