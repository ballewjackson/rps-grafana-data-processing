# Howdy!
This repository contains python scripts to analyze internal data at the TAMU Rapid Prototyping Studio. The data is saved to CSV files in order to be loaded into Grafana.

# Additonal Notes:
This application relies on you having a google service account set-up with access to your spread sheet. In doing this you will collect a .json token file for your service account and will collect the sheet id of your spreadsheet. These values are saved in keys.py and are extracted by the sheetsInterface.py script for security purposes. keys.py looks like:

SERVICE_ACCOUNT_FILE = "put the path to your service account token here"

SPREADSHEET_ID = "put your spreadsheet ID here"

keys = {
    "SERVICE_ACCOUNT_FILE": SERVICE_ACCOUNT_FILE,
    "SPREADSHEET_ID": SPREADSHEET_ID
}