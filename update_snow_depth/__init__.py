import datetime
import azure.functions as func
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import date
from azure.storage.blob import BlobServiceClient
import math
import os

def main(mytimer: func.TimerRequest, outputBlob: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    url_list = {
    "SN79791": "https://www.yr.no/nb/historikk/tabell/1-533225/Norge/Nordland/Saltdal/Saltfjellet-Svartisen%20nasjonalpark", 
    "SN84905": "https://www.yr.no/nb/historikk/tabell/5-84900/Norge/Nordland/Narvik/Bj%C3%B8rnfjell", 
    "SN94195": "https://www.yr.no/nb/historikk/tabell/5-94255/Norge/Troms%20og%20Finnmark/Hammerfest/Hammerfest"
    }

    blob_service_client = BlobServiceClient.from_connection_string(os.environ['Blockblob'])
    f = blob_service_client.get_blob_client("actuals", 'snow_depths.json')
    json_content = f.download_blob().readall()
    dfExisting = pd.read_json(json_content)

    df_out = pd.DataFrame(columns = \
        ['Station_id'
        ,'Snødybde' ] )

    for station_id, baseurl in url_list.items():

        dt = date.today() - datetime.timedelta(days=1) if station_id == "SN94195" else date.today() 
        URL = baseurl + "?q=" \
                    + str(dt.year) + "-" \
                    + str(dt.month).zfill(2) + "-" \
                    + str(dt.day).zfill(2)

        df = pd.DataFrame(columns = \
            ['Station_id'
            ,'År' \
            ,'Måned' \
            ,'Dag' \
            ,'Min_temp' \
            ,'Max_temp'
            ,'Gjennomsnitt' \
            ,'Nedbør' \
            ,'Snødybde' \
            ,'Vind' \
            ,'Vindkast' ] \
            )

        page = requests.get(URL).text 
        soup = BeautifulSoup(page, 'lxml')
        body = soup.find('body')
        tbl = body.find('table', class_='fluid-table__table')

        for tr in tbl.tbody.find_all('tr'):
            td = tr.find_all('td')
            df = df.append({
                "Station_id": station_id
                ,"Snødybde": td[6].find('span', class_="fluid-table__cell-content").text.replace(',','.').replace('–', ' ').strip()

                }, ignore_index=True)
            df = df.replace(r'^\s*$', np.nan, regex=True)

        if math.isnan(float(df["Snødybde"].tail(1).item())):
            val = dfExisting[dfExisting.Station_id == station_id].Snødybde.item()
        else:
            val = df["Snødybde"].tail(1).item()

        df_out = df_out.append({ 
             "Station_id": station_id 
             , "Snødybde": val
             }, ignore_index=True)

    outputBlob.set(df_out.to_json(orient='records',force_ascii=False, indent=2))

 