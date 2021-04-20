import requests
import json
import sys
import time
import linecache
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import json

webhook_url = "your_webhook_url_discord"

spreadsheet_media_id = "your_id_spreadsheet"

source_list_media = "source/media_list.csv"
old_data = "results/oldData_bitly.xlsx"
results_data = "results/newUpdate_data_bitly.xlsx"
ID_results_data = "1tgZxWVhNIl1LQdsHo-7Lcefm0XesH01UrQorppO324U"

ID_referring_countries = "1uc3cY6G6QnTUEpEppCUhid8XQrgXZ9nlWB1i7sCR5iU"
data_referring_countries = "results/data_referring_and_countries.xlsx"

cred_emcanalyticsteam = {
    'pathClientSecret': 'cred/client_secret_emcanalyticsteam.json',
    'pathTokenDrive': 'cred/token_drive_emcanalyticsteam.pickle'
}
headers = {
    'Authorization': 'Your_authorize_id',
}
# https://api-ssl.bitly.com/v4/bitlinks/bit.ly/12a4b6c/clicks?unit=month&units=5&size=10&unit_reference=2006-01-
def get_access():
    creds = dict()
    creds['domain'] = 'https://api-ssl.bitly.com/'
    creds['version'] = 'v4'
    creds['long_url'] = creds['domain'] + creds['version'] + "/bitlinks/"
    return creds

def makeApiCall(url, params, debug = 'no'):
    data = requests.get(url, params=params, headers=headers)
    response = dict()
    response['url'] = url
    response['params'] = params
    response['json_data'] = json.loads(data.content)

    if ('yes' == debug):
        displayApiCallData(response)

    return response

def displayApiCallData(response):
    print "\nURL: "
    print response['url']
    print "\nEndpoint Params: "
    print response['params']

def read_data_google_drive(File_ID_GoogleDrive, Title_File, dict):
    """
    function for read data from google drive,base on name ID file.
    :param File_ID_GoogleDrive: code/name id file from google drive
    :param Title_File: path + Title of file from google drive
    :return:
    """
    gauth = GoogleAuth()
    # load cline credentials with path dir, you must change the name of client_secrets.json
    gauth.LoadClientConfigFile(dict['pathClientSecret'])
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(dict['pathTokenDrive'])
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile(dict['pathTokenDrive'])

    drive = GoogleDrive(gauth)
    # read file from google drive with ID name
    File = drive.CreateFile({'id' : File_ID_GoogleDrive})
    # find out title of the file, and the mimetype
    print('title: %s, mimeType: %s' % (File['title'], File['mimeType']))
    # you can change the mimetype according the output of the mimetype file
    if '.xlsx' in Title_File:
        File.GetContentFile(Title_File, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    elif '.csv' in Title_File:
        File.GetContentFile(Title_File, mimetype='text/csv')
    else:
        File.GetContentFile(Title_File, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

def upload_and_replace_file(Directory_file, ID_file_for_replace_content_file, initDict):
    """
    :param Directory_file: Directory file local to replace content a file in google drive
    :param ID_file_for_replace_content_file: ID a file in google drive
    :return:
    """
    gauth = GoogleAuth()
    # load cline credentials with path dir, you must change the name of client_secrets.json
    gauth.LoadClientConfigFile(initDict['pathClientSecret'])
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(initDict['pathTokenDrive'])
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile(initDict['pathTokenDrive'])

    drive = GoogleDrive(gauth)
    # read file from google drive with ID name
    read_data = drive.CreateFile({'id': ID_file_for_replace_content_file})
    # function for replace content a file in google drive with content file local
    read_data.SetContentFile(Directory_file)
    read_data.Upload({'convert': True})

def ExceptionHandler():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    return 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)

def send_error_to_Discord(webhook_url, message):
    all_message = message + '\n' + str(ExceptionHandler())
    payload = {
        "content": all_message
    }
    headers = {
        'Content-Type': 'application/json',
    }
    response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)
    return response
