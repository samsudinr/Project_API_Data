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
        if parameter == "":
            data = data.json()['title']
        else:
            response = json.loads(data.content)
            data = json_normalize(response['link_clicks'])
        return data

    params = (
        ('unit', 'day'),
        ('units', '3'),
        ('size', '1'),
    )

    temp = pd.DataFrame([])

    for index, row in data.iterrows():
        remove = re.sub(r"(https://|http://)", "", str(row['Bitly Link']))
        response = getClicks(params, remove, "/clicks")
        time.sleep(2)
        privy_name = getClicks(params, remove, "")
        response['Privy Name'] = privy_name
        response['Media'] = row['Media']
        temp = temp.append(response, ignore_index=True)

    temp['date'] = temp['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%dT%H:%M:%S+0000"), "%Y-%m-%d %H:%M:%S"))
    temp = temp[['Media', 'Privy Name', 'date', 'clicks']]
    print ("success get all meta data of media engagements")
    min_date_temp = temp['date'].min()
    max_date_temp = temp['date'].max()


    old_data = pd.read_excel(lib.old_data)
    old_data = old_data.loc[~(old_data['date'] >= min_date_temp) & (old_data['date'] <= max_date_temp)].reset_index(drop=True)
    print ("Success Remove old data by range Date")

    newUpdate_data = old_data.append(temp, ignore_index=True)
    newUpdate_data = newUpdate_data.sort_values('Media')
    print ("Success Append Old data and New Data to results")

    writer = pd.ExcelWriter(lib.results_data)
    newUpdate_data.to_excel(writer, sheet_name='data', index=False)
    writer.save()
    writer.close()

    try:
        lib.upload_and_replace_file(lib.results_data, lib.ID_results_data, cred)
        print ("Success Update Data Results to Spreadsheet")
        time.sleep(3)
    except:
        print ("Error Update Data Results to Spreadsheet")
        time.sleep(3)

except Exception as e:
    error = lib.ExceptionHandler()
    print(error)
    lib.send_error_to_Discord(lib.webhook_url, str(error))
