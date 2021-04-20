from datetime import datetime, timedelta
from datetime import datetime
import pandas as pd
import requests
import json
import time
import lib
import re
from lib import get_access, makeApiCall
from pandas.io.json import json_normalize
cred = lib.cred_emcanalyticsteam

try:
    try:
        lib.read_data_google_drive(lib.spreadsheet_media_id, lib.source_list_media, cred)
        print "success get list media"
        time.sleep(3)
    except Exception as e:
        print "ERROR get list media"
        time.sleep(3)

    data = pd.read_csv(lib.source_list_media, sep=',')
    data = data[~data['Media'].isnull()]
    data = data[~data['Bitly Link'].isnull()]

    def getClicks(params, media, parameter):
        url = "https://api-ssl.bitly.com/v4/bitlinks/" + media + parameter
        data = requests.get(url, params=params, headers=lib.headers)
        time.sleep(2)
        response = json.loads(data.content)
        return response

    params = (
        ('unit', 'day'),
        ('units', '3'),
        ('size', '15'),
    )

    referring_data = pd.DataFrame([])
    countries_data = pd.DataFrame([])
    for index, row in data.iterrows():
        remove = re.sub(r"https?://", "", row['Bitly Link'])
        req_referring = getClicks(params, remove, "/referring_domains")
        referring = json_normalize(req_referring["metrics"])
        if referring.empty:
            referring = pd.DataFrame(columns=['Media', 'Privy Name', 'date', 'clicks', 'value'])
            referring_data = referring_data.append(referring)
            time.sleep(2)
        else:
            referring['date'] = req_referring['unit_reference']
            referring['Media'] = row['Media']
            privy_name = getClicks(params, remove, "")
            referring['Privy Name'] = privy_name['title']
            referring_data = referring_data.append(referring)
            time.sleep(2)
        req_countries = getClicks(params, remove, "/countries")
        countries = json_normalize(req_countries["metrics"])
        if countries.empty:
            countries = pd.DataFrame(columns=['Media', 'Privy Name', 'date', 'clicks', 'value'])
            countries_data = countries_data.append(countries)
            time.sleep(2)
        else:
            countries['date'] = req_countries['unit_reference']
            countries['Media'] = row['Media']
            privy_name = getClicks(params, remove, "")
            countries['Privy Name'] = privy_name['title']
            countries_data = countries_data.append(countries)
            time.sleep(2)

    if referring_data.empty:
        newUpdate_data_ref = pd.read_excel(lib.data_referring_countries, sheet_name="data referring")
        print (newUpdate_data_ref)
        print ("referring data is empty")
    else:
        referring_data['date'] = referring_data['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%dT%H:%M:%S+0000"), "%Y-%m-%d %H:%M:%S"))
        referring_data = referring_data[['Media', 'Privy Name', 'date', 'clicks', 'value']]
        min_date_ref = referring_data['date'].min()
        max_date_ref = referring_data['date'].max()

        old_data_ref = pd.read_excel(lib.data_referring_countries, sheet_name="data referring")
        old_data_ref = old_data_ref.loc[~(old_data_ref['date'] >= min_date_ref) & (old_data_ref['date'] <= max_date_ref)].reset_index(drop=True)
        print ("Success Remove old data by range Date")

        newUpdate_data_ref = old_data_ref.append(referring_data, ignore_index=True)
        newUpdate_data_ref = newUpdate_data_ref.sort_values('Media')
        print ("Success Append Old data and New Data to results")

    if countries_data.empty:
        newUpdate_data_con = pd.read_excel(lib.data_referring_countries, sheet_name="data countries")
        print newUpdate_data_con
        print("countries data is empty")
    else:
        countries_data['date'] = countries_data['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%dT%H:%M:%S+0000"), "%Y-%m-%d %H:%M:%S"))
        countries_data = countries_data[['Media', 'Privy Name', 'date', 'clicks', 'value']]
        print ("success get all meta data of media engagements")

        min_date_con = countries_data['date'].min()
        max_date_con = countries_data['date'].max()

        old_data_con = pd.read_excel(lib.data_referring_countries, sheet_name="data countries")
        old_data_con = old_data_con.loc[~(old_data_con['date'] >= min_date_con) & (old_data_con['date'] <= max_date_con)].reset_index(drop=True)
        print ("Success Remove old data by range Date")

        newUpdate_data_con = old_data_con.append(countries_data, ignore_index=True)
        newUpdate_data_con = newUpdate_data_con.sort_values('Media')
        print ("Success Append Old data and New Data to results")

    writer = pd.ExcelWriter(lib.data_referring_countries)
    newUpdate_data_ref.to_excel(writer, sheet_name='data referring', index=False)
    newUpdate_data_con.to_excel(writer, sheet_name='data countries', index=False)
    writer.save()
    writer.close()
    time.sleep(3)

    try:
        lib.upload_and_replace_file(lib.data_referring_countries, lib.ID_referring_countries, cred)
        print ("Success Update Data Results to Spreadsheet")
        time.sleep(3)
    except:
        print ("Error Update Data Results to Spreadsheet")
        time.sleep(3)

except Exception as e:
    error = lib.ExceptionHandler()
    print(error)
    lib.send_error_to_Discord(lib.webhook_url, str(error))
