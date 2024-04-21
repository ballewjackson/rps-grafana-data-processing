from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from keys import keys

SERVICE_ACCOUNT_FILE = keys["SERVICE_ACCOUNT_FILE"]
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)
SPREADSHEET_ID = keys["SPREADSHEET_ID"]

def get_timestamp_column():
    """
    Returns the a column with the timestampe of every order from the google sheet saved in the SPREADSHEET_ID and shared with the service account. 
    In order to change the data range, you must change RANGE_NAME in this function.
    Output looks like:
                Timestamp
0  8/15/2023 11:11:28
1  8/21/2023 12:01:00
2  8/22/2023 11:41:07
3  8/22/2023 17:08:15
4  8/22/2023 18:26:09
    """
    global service, SPREADSHEET_ID
    RANGE_NAME = 'Form Responses 1!C:C'

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()

    values = result.get('values', [])

    if values:
        print("Successfully retrieved timestamp data.")
        df = pd.DataFrame(values[1:], columns=values[0])
        return df
    else:
        print("Failed to retrieve timestamp data.")
        return None

def get_time_course_data():
    """
    Returns a dataframe with columns "Timestamp", "Course" from the google sheet saved in the SPREADSHEET_ID and shared with the service account. 
    In order to change the data range, you must change RANGE_NAME in this function.
    Output looks like:
                    Timestamp    Course
3   8/22/2023 17:08:15  MEEN 402
8   8/28/2023 14:40:06  MEEN 402
12  8/29/2023 17:37:38  MEEN 402
15  8/30/2023 18:00:28  MEEN 402
17  8/31/2023 13:22:19  MEEN 402
    """
    global service, SPREADSHEET_ID
    RANGE_NAME = 'Form Responses 1!C:H'

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()

    values = result.get('values', [])

    if not values:
        print("Failed to retrieve data.")
        return None

    df = pd.DataFrame(values[1:], columns=values[0])

    # drop columns "Email Adddress", "First Name", "Last Name", "Order Type"
    df = df.drop(columns=["Email Address", "First Name ", "Last Name ", "Order Type"])
    # drop all rows with Course == NaN
    df = df.dropna(subset=["Course"])
    print("Successfully retrieved time course data.")
    return df

def get_print_batch_data_kg_by_course():
    """
    Matches data from the form responses and print batches to get the mass of each order in kg.
    Output looks like:
       Order ID           Timestamp    Course  Mass (kg)
3         4  8/22/2023 17:08:15  MEEN 402      0.000
12       13  8/29/2023 17:37:38  MEEN 402      0.425
21       22   9/2/2023 17:09:43  MEEN 368      0.000
22       23   9/5/2023 10:43:59  MEEN 404      0.056
23       24   9/5/2023 13:27:17  MEEN 402      1.346
    """
    global service, SPREADSHEET_ID
    RANGE_NAME = 'Print Batches!A:I'

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()

    values = result.get('values', [])

    if not values:
        print("Failed to retrieve data.")
        return None

    df = pd.DataFrame(values[1:], columns=values[0])

    # drop columns "Order ID", "Print Batch ID", "Machine ID", "Labor Hours", "Machine Runtime", "Material Type"
    df = df.drop(columns=["Print Batch ID", "Machine ID", "Labor Hours", "Machine Runtime", "Material Type", "Timestamp"])
    # drop all rows with "Print Batch Status" != "Complete"
    df = df[df["Print Batch Status"] == "Completed"]
    df = df.drop(columns=["Print Batch Status"])

    # print(df.head())

    # going to have to reference the other subsheet to get course data.
    RANGE_NAME = 'Form Responses 1!A:M'

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()

    values = result.get('values', [])

    if not values:
        print("Failed to retrieve data.")
        return None

    dfRef = pd.DataFrame(values[1:], columns=values[0])
    dfRef = dfRef[dfRef["Fabrication Type"] == "3D Printing - FDM (Filament)"]
    dfRef = dfRef.drop(columns=["Email Address", "First Name ", "Last Name ", "Order Type", "Section", "Team or Group", "Professor", "Accept Terms and Conditions", "Fabrication Type"])
    # drop all rows with Course == NaN
    dfRef = dfRef.dropna(subset=["Course"])
    dfRef = dfRef[dfRef["Course"] != ""]
    # drop all rows with Order Status != "Complete"
    dfRef = dfRef[dfRef["Order Status"] == "Completed"]
    dfRef = dfRef.drop(columns=["Order Status"])

    # add "Mass (g)" column to dfRef
    dfRef["Mass (kg)"] = 0

    # print(dfRef.head())

    #print(df.head(50))
    #print(df.tail(50))

    # For row in df, find the corresponding row in dfRef and add the mass to the "Mass (g)" column
    for row in df.iterrows():
        # row = (1, Order ID          3
        # Material Qty.    21
        # Name: 1, dtype: object)
        orderId = row[1]["Order ID"]
        materialQty = row[1]["Material Qty."]
        try:
            materialQty = float(materialQty)
        except:
            materialQty = 0
        if orderId not in dfRef["Order ID"].values:
            continue
        currMass = dfRef.loc[dfRef["Order ID"] == orderId, "Mass (kg)"].values[0]
        # print(currMass, type(currMass))
        # print(materialQty, type(materialQty))
        # break
        updatedMass = currMass + materialQty
        dfRef.loc[dfRef["Order ID"] == orderId, "Mass (kg)"] = updatedMass
    
    # print(dfRef.head())
    # print(dfRef.tail(50))
    
    # dfRef = dfRef.drop(columns=["Order ID"])

    # divide the values in "Mass (kg)" by 1000
    dfRef["Mass (kg)"] = dfRef["Mass (kg)"].apply(lambda x: x / 1000)

    print("Successfully retrieved print batch data.")
    return dfRef

