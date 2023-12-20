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
            rows_with_semicolon = pd.concat([rows_with_semicolon, rows])
    
    # Print the [Langkode] of the rows and the name of the column
    for index, row in rows_with_semicolon.iterrows():
        print(f"[{key_column}]: {row[key_column]}, Column: {row['columnname']}")

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
