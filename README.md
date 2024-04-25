# nin3kodeapi_csvbuilder
Sub-projects by folders:
* CSV_produksjon
    * Creating csv files for import in nin3kodeapi based on master-document(xlsx)
* Database:
    * Querying data in sqlite database used by nin3kodeapi
* API
    * Running requests agains nin3-kodeapi
* 3_0_excel
    * Creating xlsx -file from sqlite database where each classtype and relation has its own spreadsheet.
* drupal_import
    * Creating drupal import format (update langkode in drupal based on langkode from nin3kodeapi)

Technology:
- Mainly jupyter notebook/lab, pandas and python3, requests, openpyxl, tabulate

Prerequisites:
- Python3 environment with jupyter package installed.
- Recommend using:
    - VS Code with jupyter + Python extension
        - 'Rainbow CSV' extension for working with the csv-files
        - 'SQLTools' extension for looking at the sqlite file(s)
        