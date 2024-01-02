import requests
import os
import time
import os
import time
import sqlite3
from datetime import datetime
from conf import api_root_url

conn = None

def call_endpoint(request_url):
    try:
        response = requests.get(request_url)
        http_status = response.status_code
        if http_status != 200:
            msg = f"Error: HTTP Status: {http_status}: request: {request_url}"
            log(msg)
        else:
            msg = f"HTTP Status: {http_status}: request: {request_url}"
            log(msg, writeToFile=True)
    except requests.exceptions.RequestException as e:
        msg = f"Error: {str(e)}: request: {request_url}"
        log(msg)
        

def prepare_log():
    log_folder = 'logs'
    log_file = 'log.txt'
    current_time_millis = str(int(time.time() * 1000))
    
    # Check if the logs folder exists
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    
    # Check if the log file exists
    if os.path.exists(os.path.join(log_folder, log_file)):
        # Rename the existing log file
        os.rename(os.path.join(log_folder, log_file), os.path.join(log_folder, f"log_{current_time_millis}.txt"))
    
    # Create a new empty log file
    open(os.path.join(log_folder, log_file), 'w').close()

def getConn():
    global conn
    if conn is None:
        conn = sqlite3.connect("nin3kodeapi.db")
    return conn

def closeConn():
    global conn
    if conn is not None:
        conn.close()
        conn = None

def prepare_log():
    log_folder = 'logs'
    log_file = 'log.txt'
    current_time_millis = str(int(time.time() * 1000))
    
    # Check if the logs folder exists
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    
    # Check if the log file exists
    if os.path.exists(os.path.join(log_folder, log_file)):
        # Rename the existing log file
        os.rename(os.path.join(log_folder, log_file), os.path.join(log_folder, f"log_{current_time_millis}.txt"))
    
    # Create a new empty log file
    open(os.path.join(log_folder, log_file), 'w').close()

def fetch_codes(tablename:str):
    conn = getConn()
    codesQ = f"SELECT Kode FROM {tablename}"
    cursor = conn.cursor()
    cursor.execute(codesQ)
    codes = [row[0] for row in cursor.fetchall()]
    closeConn()
    return codes

def fetch_ms_names():
    conn = getConn()
    codesQ = f"SELECT MaaleskalaNavn FROM Maaleskala"
    cursor = conn.cursor()
    cursor.execute(codesQ)
    codes = [row[0] for row in cursor.fetchall()]
    closeConn()
    return codes

def log(message:str, writeToFile=True, writeToConsole=True):
    log_folder = 'logs'
    log_file = 'log.txt'
    current_time = datetime.now()
    current_time_str = current_time.strftime("%Y%m%d_%H%M%S")
    if writeToConsole:
        print(message)
    # need a function to fetch log file
    if writeToFile:
        with open(os.path.join(log_folder, log_file), 'a') as f:
            f.write(f"{current_time_str} - {message}\n")

def main():
    prepare_log() #rename previous log file and create new log file
    #kortkode for type-classes
    tcodes = fetch_codes('Type')
    htgcodes = fetch_codes('Hovedtypegruppe')
    htocodes = fetch_codes('Hovedtype')
    gtcodes = fetch_codes('Grunntype')
    klecodes = fetch_codes('Kartleggingsenhet')
    #kortkode for variabler
    varcodes = fetch_codes('Variabel')
    vncodes = fetch_codes('Variabelnavn')
    maaleskalaNames = fetch_ms_names()

    # typer alle kode
    request_url = f"{api_root_url}/v3.0/typer/allekoder"
    call_endpoint(request_url)

    # variabler alle kode
    request_url = f"{api_root_url}/v3.0/variabler/allekoder"
    call_endpoint(request_url)

    for type_code in tcodes:
        request_url = f"{api_root_url}/v3.0/typer/kodeforType/{type_code}"
        call_endpoint(request_url)
    for htgcode in htgcodes:
        request_url = f"{api_root_url}/v3.0/typer/kodeforHovedtypegruppe/{htgcode}"
        call_endpoint(request_url)
    for htocode in htocodes:
        request_url = f"{api_root_url}/v3.0/typer/kodeforHovedtype/{htocode}"
        call_endpoint(request_url)
    for gtcode in gtcodes:
        request_url = f"{api_root_url}/v3.0/typer/kodeforGrunntype/{gtcode}"
        call_endpoint(request_url)
    for klecode in klecodes:
        request_url = f"{api_root_url}/v3.0/typer/kodeforKartleggingsenhet/{klecode}"
        call_endpoint(request_url)
    for varcode in varcodes:
        request_url = f"{api_root_url}/v3.0/variabler/kodeforVariabel/{varcode}"
        call_endpoint(request_url)
    for vn in vncodes:
        request_url = f"{api_root_url}/v3.0/variabler/kodeforVariabelnavn/{vn}"
        call_endpoint(request_url)
    for ms in maaleskalaNames:
        request_url = f"{api_root_url}/v3.0/variabler/maaleskala/{ms}"
    
    log("Ferdig !!!")